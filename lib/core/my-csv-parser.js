var stream = require('stream');
var inherits = require('inherits');
var genobj = require('generate-object-property');
var genfun = require('generate-function');

var quote = Buffer.from('"')[0];
var comma = Buffer.from(',')[0];
var cr = Buffer.from('\r')[0];
var nl = Buffer.from('\n')[0];

var Parser = function (opts) {
    if (!opts) opts = {};
    if (Array.isArray(opts)) opts = { headers: opts };

    stream.Transform.call(this, { objectMode: true });

    this.separator = opts.separator ? Buffer.from(opts.separator)[0] : comma;
    this.quote = opts.quote ? Buffer.from(opts.quote)[0] : quote;
    this.escape = opts.escape ? Buffer.from(opts.escape)[0] : this.quote;
    this.newline = Buffer.from(opts.newline)[0];
    this.strict = opts.strict || null;
    this.mapHeaders = opts.mapHeaders || identity;
    this.mapValues = opts.mapValues || identity;

    this._raw = !!opts.raw;
    this._prev = null;
    this._prevEnd = 0;
    this._empty = this._raw ? new Buffer(0) : '';
    this._Row = null;
    this._first = false;
    this._generateHeaders = false;
    this._headers = null;
    if (opts.headers === true) {
        this._first = true;
    }
    else if (!opts.headers) {
        this._generateHeaders = true;
    }
    else {
        this.setHeaders(opts.headers);
    }
}

inherits(Parser, stream.Transform);

Parser.prototype._transform = function (data, enc, cb) {
    var buf = null;
    if (this._prev) {
        var length = this._prev.length + data.length;
        buf = Buffer.allocUnsafe(length);
        const binding = process.binding('buffer');
        binding.copy(this._prev, buf, 0);
        if (typeof data === 'string') {
            data = binding.createFromString(data, 'utf-8');
        }
        binding.copy(data, buf, this._prev.length);
        
        this._prev = null;
    }
    else if (typeof data === 'string') {
        buf = Buffer.from(data);
    }
    else {
        buf = data;
    }
    var start = this._prevEnd;
    this._processBuffer(buf, start);
    return cb();
}

Parser.prototype._flush = function (cb) {
    if (this._prev) {
        this._processBuffer(this._prev, this._prevEnd, true);
    }
    cb();
}

Parser.prototype._processBuffer = function (buf, start, forceEnd) {
    var rows = [];
    this._prevEnd = Parser.processBuffer(buf, 
        start, 
        this.separator, 
        this.newline, 
        this.quote, 
        this.escape, 
        this.mapValues,
        (cells) => {
            if (this._first) {
                this.setHeaders(cells);
                this.emit('headers', this._headers);
            }
            else {
                if (this._generateHeaders) {
                    // If we don't have explicit headers, generate them from the payload
                    this.setHeaders(new Array(cells.length).fill(0).map((value, index) => `_col${index}`));
                    this._generateHeaders = false;
                }
                var row = cells;
                if (this._Row) {
                    row = new this._Row(cells);
                }
                rows.push(row);
            }
            return true;
        },
        forceEnd);
    if (this._prevEnd) {
        // Preserve the partial line for processing in the next callback
        this._prev = buf.slice(this._prevEnd);
        this._prevEnd = 0;
    }
    // Finally, send the buffer on
    this.push(rows);
}

Parser.processBuffer = function (buf, start, separator, rowDelimiter, quoteChr, escapeChr, mapValue, cbRow, forceEnd) {
    var retval = 0;
    var quoted = false;
    var escaped = false;
    var bufLen = buf.length;
    var cells = [];
    var offset = start;

    quoteChr |= quote;
    escapeChr |= quote;

    var processLine = (pos) => {
        // trim newline
        var end = pos - 1; 
        cells.push(Parser._oncell(buf, offset, end + 1, quoteChr, mapValue));
        var keepProcessing = cbRow(cells);
        cells = [];
        retval = pos + 1;
        offset = pos + 1;
        return keepProcessing;
    };

    for (var i = start; i < bufLen; i++) {
        var chr = buf[i];
        var nextChr = i + 1 < bufLen ? buf[i + 1] : null;

        if (!escaped && chr === escapeChr && nextChr === quoteChr) {
            escaped = true;
            continue;
        } else if (chr === quoteChr) {
            if (escaped) {
                escaped = false;
                // non-escaped quote (quoting the cell)
            } else if (nextChr === separator || nextChr === rowDelimiter || nextChr === cr) {
                quoted = false;
            }
            else {
                quoted = !quoted;
            }
            continue;
        }
        if (!quoted) {
            if (chr === separator) {
                cells.push(Parser._oncell(buf, offset, i, quoteChr, mapValue));
                offset = i + 1;
            }
            else if (chr === rowDelimiter) {
                if (!processLine(i)) {
                    break;
                }
            }
        }
    }
    if (forceEnd && cells.length) {
        processLine(bufLen);
        retval = 0;
    }
    else if (retval === bufLen) {
        retval = 0;
    }
    return retval;
}

Parser.prototype.setHeaders = function (headers) {
    if (headers === true) {
        this._first = true;
        this._Row = null;
        this._headers = null;
    }
    else {
        this._Row = null;
        this._headers = headers;
        if (this._headers) {
            this._first = false;
            this._compile(this._headers);
        }
    }
}

Parser.prototype._compile = function (headers) {
    var Row = genfun()('function Row (cells) {');

    var self = this;
    headers.forEach(function (cell, i) {
        var newHeader = self.mapHeaders(cell, i);
        if (newHeader) {
            Row('%s = cells[%d]', genobj('this', newHeader), i);
        }
    });

    Row('}');

    this._Row = Row.toFunction();

    if (Object.defineProperty) {
        Object.defineProperty(this._Row.prototype, 'headers', {
            enumerable: false,
            value: headers
        });
    } else {
        this._Row.prototype.headers = headers;
    }
}

Parser._oncell = function (buf, start, end, quoteChr, mapValue) {
    // remove quotes from quoted cells
    if (buf[start] === quoteChr && buf[end - 1] === quoteChr) {
        start++;
        end--;
    }

    var value = Parser._onvalue(buf, start, end);
    return mapValue ? mapValue(value) : value;
}

Parser._onvalue = function (buf, start, end) {
    return buf.toString('utf-8', start, end);
}

function identity(id) {
    return id;
}

module.exports = Parser;
