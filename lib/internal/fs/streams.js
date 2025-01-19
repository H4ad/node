'use strict';

const {
  Array,
  FunctionPrototypeBind,
  MathMax,
  MathMin,
  ObjectDefineProperty,
  ObjectSetPrototypeOf,
  PromisePrototypeThen,
  ReflectApply,
  Symbol,
} = primordials;

const {
  ERR_INVALID_ARG_TYPE,
  ERR_METHOD_NOT_IMPLEMENTED,
  ERR_OUT_OF_RANGE,
  ERR_STREAM_DESTROYED,
  ERR_SYSTEM_ERROR,
} = require('internal/errors').codes;
const {
  deprecate,
  kEmptyObject,
} = require('internal/util');
const {
  validateBoolean,
  validateFunction,
  validateInteger, validateOneOf,
} = require('internal/validators');
const {errorOrDestroy} = require('internal/streams/destroy');
const fs = require('fs');
const {kRef, kUnref, FileHandle} = require('internal/fs/promises');
const {Buffer} = require('buffer');
const {
  copyObject,
  getOptions,
  getValidatedFd,
  validatePath,
} = require('internal/fs/utils');
const {Readable, Writable, finished} = require('stream');
const {toPathIfFileURL} = require('internal/url');
const kIoDone = Symbol('kIoDone');
const kIsPerformingIO = Symbol('kIsPerformingIO');

const kFs = Symbol('kFs');
const kHandle = Symbol('kHandle');

function _construct(callback) {
  const stream = this;
  if (typeof stream.fd === 'number') {
    callback();
    return;
  }

  if (stream.open !== openWriteFs && stream.open !== openReadFs) {
    // Backwards compat for monkey patching open().
    const orgEmit = stream.emit;
    stream.emit = function (...args) {
      if (args[0] === 'open') {
        this.emit = orgEmit;
        callback();
        ReflectApply(orgEmit, this, args);
      } else if (args[0] === 'error') {
        this.emit = orgEmit;
        callback(args[1]);
      } else {
        ReflectApply(orgEmit, this, args);
      }
    };
    stream.open();
  } else {
    stream[kFs].open(stream.path, stream.flags, stream.mode, (er, fd) => {
      if (er) {
        callback(er);
      } else {
        stream.fd = fd;
        callback();
        stream.emit('open', stream.fd);
        stream.emit('ready');
      }
    });
  }
}

// This generates an fs operations structure for a FileHandle
const FileHandleOperations = (handle) => {
  return {
    open: (path, flags, mode, cb) => {
      throw new ERR_METHOD_NOT_IMPLEMENTED('open()');
    },
    close: (fd, cb) => {
      handle[kUnref]();
      PromisePrototypeThen(handle.close(),
        () => cb(), cb);
    },
    fsync: (fd, cb) => {
      PromisePrototypeThen(handle.sync(), () => cb(), cb);
    },
    read: (fd, buf, offset, length, pos, cb) => {
      PromisePrototypeThen(handle.read(buf, offset, length, pos),
        (r) => cb(null, r.bytesRead, r.buffer),
        (err) => cb(err, 0, buf));
    },
    write: (fd, buf, offset, length, pos, cb) => {
      PromisePrototypeThen(handle.write(buf, offset, length, pos),
        (r) => cb(null, r.bytesWritten, r.buffer),
        (err) => cb(err, 0, buf));
    },
    writev: (fd, buffers, pos, cb) => {
      PromisePrototypeThen(handle.writev(buffers, pos),
        (r) => cb(null, r.bytesWritten, r.buffers),
        (err) => cb(err, 0, buffers));
    },
  };
};

function close(stream, err, cb) {
  if (!stream.fd) {
    cb(err);
  } else if (stream.flush) {
    stream[kFs].fsync(stream.fd, (flushErr) => {
      _close(stream, err || flushErr, cb);
    });
  } else {
    _close(stream, err, cb);
  }
}

function _close(stream, err, cb) {
  stream[kFs].close(stream.fd, (er) => {
    cb(er || err);
  });
  stream.fd = null;
}

function importFd(stream, options) {
  if (typeof options.fd === 'number') {
    // When fd is a raw descriptor, we must keep our fingers crossed
    // that the descriptor won't get closed, or worse, replaced with
    // another one
    // https://github.com/nodejs/node/issues/35862
    stream[kFs] = options.fs || fs;
    return options.fd;
  } else if (typeof options.fd === 'object' &&
    options.fd instanceof FileHandle) {
    // When fd is a FileHandle we can listen for 'close' events
    if (options.fs) {
      // FileHandle is not supported with custom fs operations
      throw new ERR_METHOD_NOT_IMPLEMENTED('FileHandle with fs');
    }
    stream[kHandle] = options.fd;
    stream[kFs] = FileHandleOperations(stream[kHandle]);
    stream[kHandle][kRef]();
    options.fd.on('close', FunctionPrototypeBind(stream.close, stream));
    return options.fd.fd;
  }

  throw new ERR_INVALID_ARG_TYPE('options.fd',
    ['number', 'FileHandle'], options.fd);
}

function ReadStream(path, options) {
  if (!(this instanceof ReadStream))
    return new ReadStream(path, options);

  // A little bit bigger buffer and water marks by default
  options = copyObject(getOptions(options, kEmptyObject));
  if (options.highWaterMark === undefined)
    options.highWaterMark = 64 * 1024;

  if (options.autoDestroy === undefined) {
    options.autoDestroy = false;
  }

  if (options.fd == null) {
    this.fd = null;
    this[kFs] = options.fs || fs;
    validateFunction(this[kFs].open, 'options.fs.open');

    // Path will be ignored when fd is specified, so it can be falsy
    this.path = toPathIfFileURL(path);
    this.flags = options.flags === undefined ? 'r' : options.flags;
    this.mode = options.mode === undefined ? 0o666 : options.mode;

    validatePath(this.path);
  } else {
    this.fd = getValidatedFd(importFd(this, options));
  }

  options.autoDestroy = options.autoClose === undefined ?
    true : options.autoClose;

  validateFunction(this[kFs].read, 'options.fs.read');

  if (options.autoDestroy) {
    validateFunction(this[kFs].close, 'options.fs.close');
  }

  this.start = options.start;
  this.end = options.end;
  this.pos = undefined;
  this.bytesRead = 0;
  this[kIsPerformingIO] = false;

  if (this.start !== undefined) {
    validateInteger(this.start, 'start', 0);

    this.pos = this.start;
  }


  if (this.end === undefined) {
    this.end = Infinity;
  } else if (this.end !== Infinity) {
    validateInteger(this.end, 'end', 0);

    if (this.start !== undefined && this.start > this.end) {
      throw new ERR_OUT_OF_RANGE(
        'start',
        `<= "end" (here: ${this.end})`,
        this.start,
      );
    }
  }

  ReflectApply(Readable, this, [options]);
}

ObjectSetPrototypeOf(ReadStream.prototype, Readable.prototype);
ObjectSetPrototypeOf(ReadStream, Readable);

ObjectDefineProperty(ReadStream.prototype, 'autoClose', {
  __proto__: null,
  get() {
    return this._readableState.autoDestroy;
  },
  set(val) {
    this._readableState.autoDestroy = val;
  },
});

const openReadFs = deprecate(function () {
  // Noop.
}, 'ReadStream.prototype.open() is deprecated', 'DEP0135');
ReadStream.prototype.open = openReadFs;

ReadStream.prototype._construct = _construct;

ReadStream.prototype._read = function (n) {
  n = this.pos !== undefined ?
    MathMin(this.end - this.pos + 1, n) :
    MathMin(this.end - this.bytesRead + 1, n);

  if (n <= 0) {
    this.push(null);
    return;
  }

  const buf = Buffer.allocUnsafeSlow(n);

  this[kIsPerformingIO] = true;
  this[kFs]
    .read(this.fd, buf, 0, n, this.pos, (er, bytesRead, buf) => {
      this[kIsPerformingIO] = false;

      // Tell ._destroy() that it's safe to close the fd now.
      if (this.destroyed) {
        this.emit(kIoDone, er);
        return;
      }

      if (er) {
        errorOrDestroy(this, er);
      } else if (bytesRead > 0) {
        if (this.pos !== undefined) {
          this.pos += bytesRead;
        }

        this.bytesRead += bytesRead;

        if (bytesRead !== buf.length) {
          // Slow path. Shrink to fit.
          // Copy instead of slice so that we don't retain
          // large backing buffer for small reads.
          const dst = Buffer.allocUnsafeSlow(bytesRead);
          buf.copy(dst, 0, 0, bytesRead);
          buf = dst;
        }

        this.push(buf);
      } else {
        this.push(null);
      }
    });
};

ReadStream.prototype._destroy = function (err, cb) {
  // Usually for async IO it is safe to close a file descriptor
  // even when there are pending operations. However, due to platform
  // differences file IO is implemented using synchronous operations
  // running in a thread pool. Therefore, file descriptors are not safe
  // to close while used in a pending read or write operation. Wait for
  // any pending IO (kIsPerformingIO) to complete (kIoDone).
  if (this[kIsPerformingIO]) {
    this.once(kIoDone, (er) => close(this, err || er, cb));
  } else {
    close(this, err, cb);
  }
};

ReadStream.prototype.close = function (cb) {
  if (typeof cb === 'function') finished(this, cb);
  this.destroy();
};

ObjectDefineProperty(ReadStream.prototype, 'pending', {
  __proto__: null,
  get() {
    return this.fd === null;
  },
  configurable: true,
});

function WriteStream(path, options) {
  if (!(this instanceof WriteStream))
    return new WriteStream(path, options);

  options = copyObject(getOptions(options, kEmptyObject));

  // Only buffers are supported.
  options.decodeStrings = true;

  if (options.fd == null) {
    this.fd = null;
    this[kFs] = options.fs || fs;
    validateFunction(this[kFs].open, 'options.fs.open');

    // Path will be ignored when fd is specified, so it can be falsy
    this.path = toPathIfFileURL(path);
    this.flags = options.flags === undefined ? 'w' : options.flags;
    this.mode = options.mode === undefined ? 0o666 : options.mode;

    validatePath(this.path);
  } else {
    this.fd = getValidatedFd(importFd(this, options));
  }

  options.autoDestroy = options.autoClose === undefined ?
    true : options.autoClose;

  if (!this[kFs].write && !this[kFs].writev) {
    throw new ERR_INVALID_ARG_TYPE('options.fs.write', 'function',
      this[kFs].write);
  }

  if (this[kFs].write) {
    validateFunction(this[kFs].write, 'options.fs.write');
  }

  if (this[kFs].writev) {
    validateFunction(this[kFs].writev, 'options.fs.writev');
  }

  if (options.autoDestroy) {
    validateFunction(this[kFs].close, 'options.fs.close');
  }

  this.flush = options.flush;
  if (this.flush == null) {
    this.flush = false;
  } else {
    validateBoolean(this.flush, 'options.flush');
    validateFunction(this[kFs].fsync, 'options.fs.fsync');
  }

  // It's enough to override either, in which case only one will be used.
  if (!this[kFs].write) {
    this._write = null;
  }
  if (!this[kFs].writev) {
    this._writev = null;
  }

  this.start = options.start;
  this.pos = undefined;
  this.bytesWritten = 0;
  this[kIsPerformingIO] = false;

  if (this.start !== undefined) {
    validateInteger(this.start, 'start', 0);

    this.pos = this.start;
  }

  ReflectApply(Writable, this, [options]);

  if (options.encoding)
    this.setDefaultEncoding(options.encoding);
}

ObjectSetPrototypeOf(WriteStream.prototype, Writable.prototype);
ObjectSetPrototypeOf(WriteStream, Writable);

ObjectDefineProperty(WriteStream.prototype, 'autoClose', {
  __proto__: null,
  get() {
    return this._writableState.autoDestroy;
  },
  set(val) {
    this._writableState.autoDestroy = val;
  },
});

const openWriteFs = deprecate(function () {
  // Noop.
}, 'WriteStream.prototype.open() is deprecated', 'DEP0135');
WriteStream.prototype.open = openWriteFs;

WriteStream.prototype._construct = _construct;

function writeAll(data, size, pos, cb, retries = 0) {
  this[kFs].write(this.fd, data, 0, size, pos, (er, bytesWritten, buffer) => {
    // No data currently available and operation should be retried later.
    if (er?.code === 'EAGAIN') {
      er = null;
      bytesWritten = 0;
    }

    if (this.destroyed || er) {
      return cb(er || new ERR_STREAM_DESTROYED('write'));
    }

    this.bytesWritten += bytesWritten;

    retries = bytesWritten ? 0 : retries + 1;
    size -= bytesWritten;
    pos += bytesWritten;

    // Try writing non-zero number of bytes up to 5 times.
    if (retries > 5) {
      cb(new ERR_SYSTEM_ERROR('write failed'));
    } else if (size) {
      writeAll.call(this, buffer.slice(bytesWritten), size, pos, cb, retries);
    } else {
      cb();
    }
  });
}

function writeStringAll(data, cb, retries = 0) {
  // process._rawDebug(`Writing data with length: ${data.length} with callback ${cb}`);

  this[kFs].write(this.fd, data, (er, bytesWritten, buffer) => {
    // process._rawDebug(`Got write callback with er: ${er}, bytesWritten: ${bytesWritten}, buffer: ${buffer}`);

    // No data currently available and operation should be retried later.
    if (er?.code === 'EAGAIN') {
      er = null;
      bytesWritten = 0;
    }

    // process._rawDebug(`Destroyed: ${this.destroyed}`);

    if (this.destroyed || er) {
      return cb(er || new ERR_STREAM_DESTROYED('write'));
    }

    this.len -= bytesWritten;
    this.bytesWritten += bytesWritten;

    retries = bytesWritten ? 0 : retries + 1;

    // process._rawDebug(`Retries: ${retries}, Len: ${this.len}, BytesWritten: ${bytesWritten}, Pos: ${this.pos}`);
    //
    if (this.len > this.minLength) {
      this._write();
    } else {
      cb();
    }
  });
}

function writevAll(chunks, size, pos, cb, retries = 0) {
  this[kFs].writev(this.fd, chunks, this.pos, (er, bytesWritten, buffers) => {
    // No data currently available and operation should be retried later.
    if (er?.code === 'EAGAIN') {
      er = null;
      bytesWritten = 0;
    }

    if (this.destroyed || er) {
      return cb(er || new ERR_STREAM_DESTROYED('writev'));
    }

    this.bytesWritten += bytesWritten;

    retries = bytesWritten ? 0 : retries + 1;
    size -= bytesWritten;
    pos += bytesWritten;

    // Try writing non-zero number of bytes up to 5 times.
    if (retries > 5) {
      cb(new ERR_SYSTEM_ERROR('writev failed'));
    } else if (size) {
      writevAll.call(this, [Buffer.concat(buffers).slice(bytesWritten)], size, pos, cb, retries);
    } else {
      cb();
    }
  });
}

WriteStream.prototype._write = function (data, encoding, cb) {
  this[kIsPerformingIO] = true;
  writeAll.call(this, data, data.length, this.pos, (er) => {
    this[kIsPerformingIO] = false;
    if (this.destroyed) {
      // Tell ._destroy() that it's safe to close the fd now.
      cb(er);
      return this.emit(kIoDone, er);
    }

    cb(er);
  });

  if (this.pos !== undefined)
    this.pos += data.length;
};

WriteStream.prototype._writev = function (data, cb) {
  const len = data.length;
  const chunks = new Array(len);
  let size = 0;

  for (let i = 0; i < len; i++) {
    const chunk = data[i].chunk;

    chunks[i] = chunk;
    size += chunk.length;
  }

  this[kIsPerformingIO] = true;
  writevAll.call(this, chunks, size, this.pos, (er) => {
    this[kIsPerformingIO] = false;
    if (this.destroyed) {
      // Tell ._destroy() that it's safe to close the fd now.
      cb(er);
      return this.emit(kIoDone, er);
    }

    cb(er);
  });

  if (this.pos !== undefined)
    this.pos += size;
};

WriteStream.prototype._destroy = function (err, cb) {
  // Usually for async IO it is safe to close a file descriptor
  // even when there are pending operations. However, due to platform
  // differences file IO is implemented using synchronous operations
  // running in a thread pool. Therefore, file descriptors are not safe
  // to close while used in a pending read or write operation. Wait for
  // any pending IO (kIsPerformingIO) to complete (kIoDone).
  if (this[kIsPerformingIO]) {
    this.once(kIoDone, (er) => close(this, err || er, cb));
  } else {
    close(this, err, cb);
  }
};

WriteStream.prototype.close = function (cb) {
  if (cb) {
    if (this.closed) {
      process.nextTick(cb);
      return;
    }
    this.on('close', cb);
  }

  // If we are not autoClosing, we should call
  // destroy on 'finish'.
  if (!this.autoClose) {
    this.on('finish', this.destroy);
  }

  // We use end() instead of destroy() because of
  // https://github.com/nodejs/node/issues/2006
  this.end();
};

// There is no shutdown() for files.
WriteStream.prototype.destroySoon = WriteStream.prototype.end;

ObjectDefineProperty(WriteStream.prototype, 'pending', {
  __proto__: null,
  get() {
    return this.fd === null;
  },
  configurable: true,
});

function WriteTruncatingBufferedStream(path, options) {
  if (!(this instanceof WriteTruncatingBufferedStream))
    return new WriteTruncatingBufferedStream(path, options);

  options = copyObject(getOptions(options, kEmptyObject));

  // or is only string, or is only buffer
  options.decodeStrings = false;
  // following specification of sonic-boom, we don't have autoDestroy
  options.autoDestroy = false;

  if (options.fd == null) {
    this.fd = null;
    this[kFs] = options.fs || fs;
    validateFunction(this[kFs].open, 'options.fs.open');

    // Path will be ignored when fd is specified, so it can be falsy
    this.path = toPathIfFileURL(path);
    this.flags = options.flags === undefined ? 'w' : options.flags;
    this.mode = options.mode === undefined ? 0o666 : options.mode;

    validatePath(this.path);
  } else {
    this.fd = getValidatedFd(importFd(this, options));
  }

  if (!this[kFs].write && !this[kFs].writev) {
    throw new ERR_INVALID_ARG_TYPE('options.fs.write', 'function',
      this[kFs].write);
  }

  if (this[kFs].write) {
    validateFunction(this[kFs].write, 'options.fs.write');
  }

  validateFunction(this[kFs].close, 'options.fs.close');

  // TODO(H4ad): I don't know if we should use or ignore this method
  // https://nodejs.org/api/stream.html#writable_writevchunks-callback
  // based on the doc, this will never be called under the design of this class
  this._writev = null;

  this.len = 0;
  this.bufs = [];
  this.lens = [];

  this.start = options.start;
  this.pos = undefined;
  this.bytesWritten = 0;
  this[kIsPerformingIO] = false;

  if (this.start !== undefined) {
    validateInteger(this.start, 'start', 0);

    this.pos = this.start;
  }

  this.minLength = options.minLength || 0;
  if (options.minLength !== undefined)
    validateInteger(this.minLength, 'options.minLength', 0);

  this.maxLength = options.maxLength || 0;
  if (options.maxLength !== undefined)
    validateInteger(this.maxLength, 'options.maxLength', this.minLength + 1);

  // 16 KB. Don't write more than docker buffer size.
  // https://github.com/moby/moby/blob/513ec73831269947d38a644c278ce3cac36783b2/daemon/logger/copier.go#L13
  this.maxWrite = options.maxWrite || 16 * 1024;
  validateInteger(this.maxWrite, 'options.maxWrite', this.minLength + 1);

  this.hwm = MathMax(this.minLength, 16 * 1024);

  if (options.contentMode === undefined)
    options.contentMode = 'utf8';
  else
    validateOneOf(options.contentMode, 'options.contentMode', ['utf8', 'buffer']);

  this.contentMode = options.contentMode;

  ReflectApply(Writable, this, [options]);

  // if (options.encoding)
  //   this.setDefaultEncoding(options.encoding);
}

ObjectSetPrototypeOf(WriteTruncatingBufferedStream.prototype, WriteStream.prototype);
ObjectSetPrototypeOf(WriteTruncatingBufferedStream, WriteStream);

WriteTruncatingBufferedStream.prototype._write = function (cb) {
  // console.trace(cb);
  // process._rawDebug(`Calling _write with callback ${cb}`);

  const data = this.bufs.shift() || '';

  // process._rawDebug(`Actual write with data ${Buffer.byteLength(data)} and is writing ${this[kIsPerformingIO]}`)

  this[kIsPerformingIO] = true;
  writeStringAll.call(this, data, (er) => {
    // process._rawDebug(`WriteStringAll callback with er: ${er}`);

    this[kIsPerformingIO] = false;
    if (this.destroyed) {
      if (cb)
        cb(er);

      // Tell ._destroy() that it's safe to close the fd now.
      return this.emit(kIoDone, er);
    }

    if (cb)
      cb(er);

    if (!er)
      this.emit('drain')
  });
}

WriteTruncatingBufferedStream.prototype.write = function (data) {
  if (this.contentMode === 'utf8')
    return handleWriteTruncatingBufferedString(this, data);
  else
    return handleWriteTruncatingBufferedBuffer(this, data);

  // this[kIsPerformingIO] = true;
  // writeAll.call(this, data, data.length, this.pos, (er) => {
  //   this[kIsPerformingIO] = false;
  //   if (this.destroyed) {
  //     // Tell ._destroy() that it's safe to close the fd now.
  //     cb(er);
  //     return this.emit(kIoDone, er);
  //   }
  //
  //   cb(er);
  // });
  //
  // if (this.pos !== undefined)
  //   this.pos += data.length;
};

function handleWriteTruncatingBufferedString(instance, data) {
  const len = instance.len + data.length;
  const bufs = instance.bufs;

  // process._rawDebug(`DataLength: ${dataLength}, Len: ${len}, InstanceLen: ${instance.len}, HWM: ${instance.hwm}, BufsLength: ${bufs.length}`);

  if (instance.maxLength && len > instance.maxLength) {
    instance.emit('drop', data)
    return instance.len < instance.hwm
  }

  if (
    bufs.length === 0 ||
    bufs[bufs.length - 1].length + data.length > instance.maxWrite
  ) {
    bufs.push('' + data)
  } else {
    bufs[bufs.length - 1] += data
  }

  instance.len = len

  if (!instance[kIsPerformingIO] && instance.len >= instance.minLength) {
    instance._write()
  }

  return instance.len < instance.hwm
}

function handleWriteTruncatingBufferedBuffer(data, cb) {

}

function actualWriteTruncatingBufferedBuffer(instance) {
}

WriteStream.prototype.flush = function (cb) {
  if (cb !== undefined)
    validateFunction(cb, 'callback');

  this._write(cb);
}

WriteStream.prototype.close = function (cb) {
  if (cb) {
    if (this.closed) {
      process.nextTick(cb);
      return;
    }
    this.on('close', cb);
  }

  // how to handle when we want to close and there are still data in the buffer?
  if (this.bufs.length) {
    this.flush(err => {
      // We use end() instead of destroy() because of
      // https://github.com/nodejs/node/issues/2006
      this.end(err);
    });
  } else {
    // We use end() instead of destroy() because of
    // https://github.com/nodejs/node/issues/2006
    this.end(err);
  }
};

WriteStream.prototype._destroy = function (err, cb) {
  if (this.bufs.length) {
    this.flush(er => {
      ReflectApply(WriteStream.prototype._destroy, instance, [err || er, cb]);
    });
  } else {
    ReflectApply(WriteStream.prototype._destroy, instance, [err, cb]);
  }
};

module.exports = {
  ReadStream,
  WriteStream,
  WriteTruncatingBufferedStream,
};
