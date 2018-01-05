var stream = require('stream');
var inherits = require('inherits');
var genobj = require('generate-object-property');
var genfun = require('generate-function');

var quote = new Buffer('"')[0];
var comma = new Buffer(',')[0];
var cr = new Buffer('\r')[0];
var nl = new Buffer('\n')[0];

var Parser = function (opts) {
    if (!opts) opts = {};
    if (Array.isArray(opts)) opts = { headers: opts };

    stream.Transform.call(this, { objectMode: true });

    this.separator = opts.separator ? new Buffer(opts.separator)[0] : comma;
    this.quote = opts.quote ? new Buffer(opts.quote)[0] : quote;
    this.escape = opts.escape ? new Buffer(opts.escape)[0] : this.quote;
    if (opts.newline) {
        this.newline = new Buffer(opts.newline)[0];
        this.customNewline = true;
    } else {
        this.newline = nl;
        this.customNewline = false;
    }

    this.headers = opts.headers || null;
    this.strict = opts.strict || null;
    this.mapHeaders = opts.mapHeaders || identity;
    this.mapValues = opts.mapValues || identity;

    this._raw = !!opts.raw;
    this._prev = null;
    this._prevEnd = 0;
    this._first = true;
    this._empty = this._raw ? new Buffer(0) : '';
    this._Row = null;

    if (this.headers) {
        this._first = false;
        this._compile(this.headers);
    }
}

inherits(Parser, stream.Transform);

Parser.prototype._transform = function (data, enc, cb) {
    if (typeof data === 'string') {
        data = new Buffer(data);
    }

    var buf = data;
    var start = this._prevEnd;
    if (this._prev) {
        buf = Buffer.concat([this._prev, data]);
        this._prev = null;
    }
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
    var quoted = false;
    var escaped = false;
    var bufLen = buf.length;
    var comma = this.separator;
    var cells = [];
    var isQuoted = false;
    var offset = start;

     var processLine = (pos) => {
        // trim newline
        var end = pos - 1; 
        if (!this.customNewline && buf.length && buf[end - 1] === cr) {
            end--;
        }
        cells.push(this._oncell(buf, offset, end + 1));
        // Send the column values down the stream
        if (this._first) {
            this._first = false;
            this.headers = cells;
            this._compile(cells);
            this.emit('headers', this.headers);
        }
        else {
            this.push(new this._Row(cells));
        }
        cells = [];
        this._prevEnd = pos + 1;
        offset = pos + 1;
    };

    for (var i = start; i < bufLen; i++) {
        var chr = buf[i];
        var nextChr = i + 1 < bufLen ? buf[i + 1] : null;

        if (!escaped && chr === this.escape && nextChr === this.quote && i !== start) {
            escaped = true;
            continue;
        } else if (chr === this.quote) {
            if (escaped) {
                escaped = false;
                // non-escaped quote (quoting the cell)
            } else {
                quoted = !quoted;
            }
            continue;
        }

        if (!quoted) {
            if (this._first && !this.customNewline) {
                if (chr === nl) {
                    this.newline = nl;
                } else if (chr === cr) {
                    if (nextChr !== nl) {
                        this.newline = cr;
                    }
                }
            }
            if (chr === comma) {
                cells.push(this._oncell(buf, offset, i));
                offset = i + 1;
            }
            else if (chr === this.newline) {
                processLine(i);
            }
        }
    }

    if (forceEnd && cells.length) {
        processLine(bufLen);
        this._prevEnd = 0;
    }
    else if (this._prevEnd === bufLen) {
        this._prevEnd = 0;
    }
    else {
        // Preserve the partial line for processing in the next callback
        this._prev = buf.slice(this._prevEnd);
        this._prevEnd = 0;
    }
}

Parser.prototype._compile = function () {
    if (this._Row) return;

    var Row = genfun()('function Row (cells) {');

    var self = this;
    this.headers.forEach(function (cell, i) {
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
            value: this.headers
        });
    } else {
        this._Row.prototype.headers = this.headers;
    }
}

Parser.prototype._oncell = function (buf, start, end) {
    // remove quotes from quoted cells
    if (buf[start] === this.quote && buf[end - 1] === this.quote) {
        start++;
        end--;
    }

    var value = this._onvalue(buf, start, end);
    return this._first ? value : this.mapValues(value);
}

Parser.prototype._onvalue = function (buf, start, end) {
    return buf.slice(start, end);
}

function identity(id) {
    return id;
}

module.exports = function (opts) {
    return new Parser(opts);
}
