'use strict';

const storageManager = require('./blob/StorageManager'),
    N = require('./HttpHeaderNames'),
    EntityType = require('./Constants').StorageEntityType,
    env = require('./env'),
    fs = require("fs-extra"),
    crypto = require('crypto'),
    AError = require('./AzuriteError'),
    ErrorCodes = require('./ErrorCodes'),
    odata = require('odata-parser'),
    transform = require('stream-transform'),
    csv_parser = require('./my-csv-parser'),
    csv_stringify = require('csv-stringify'),
    parquet = require('parquetjs'),
    { Transform } = require('stream');

const FILTER_QS = "$filter";
const SELECT_QS = "$select";

// This class must also handle the standard GetBlob (ie. NO Select operation) responses
// as well
class SelectProcessor {
    constructor(request, res, azResponse) {
        this._res = res;
        this._azResponse = azResponse;
        // Check if Select filtering or projection has been requested
        this.options = {
            source: {
                file_type: request.httpProps[N.SELECT_SOURCE_TYPE] || 'CSV',
                column_delim: decodeURI(request.httpProps[N.SELECT_SOURCE_COLUMN_DELIM] || ','),
                row_delim: decodeURI(request.httpProps[N.SELECT_SOURCE_ROW_DELIM] || '\n'),
            },
            dest: {
                file_type: request.httpProps[N.SELECT_DEST_TYPE] || 'CSV',
                column_delim: decodeURI(request.httpProps[N.SELECT_DEST_COLUMN_DELIM] || ','),
                row_delim: decodeURI(request.httpProps[N.SELECT_DEST_ROW_DELIM] || '\n'),
            },
            odata_ast: null,
            object_filename: env.diskStorageUri(request.id),
            startByte: undefined,
            endByte: undefined
        };
        // See if there's an explicit range asked for
        const range = request.httpProps[N.RANGE];
        if (range) {
            const pair = range.split('=')[1].split('-');
            this.options.startByte = parseInt(pair[0]);
            this.options.endByte = parseInt(pair[1]);
        }
        // Extract the OData params from the query string
        var parts = [];
        var addPart = (partName) => {
            if (request.query[partName]) {
                parts.push(`${partName}=${request.query[partName]}`);
            }
        };
        addPart(FILTER_QS);
        addPart(SELECT_QS);
        if (parts.length) {
            try {
                this.options.odata_ast = odata.parse(parts.join('&'));
            }
            catch (e) {
                // Translate error & re-throw
                var baseError = ErrorCodes.InvalidInput;
                baseError.userMessage += ' ' + e.message;
                throw new AError(baseError);
            }
            // The OData parser returns errors in 2 ways; throwing exceptions & assigning the 'error' member in the result
            if (this.options.odata_ast.error) {
                var baseError = ErrorCodes.InvalidInput;
                baseError.userMessage += ' ' + this.options.odata_ast.error;
                throw new AError(baseError);
            }
        }
    }

    sendResponse(startRange, endRange) {
        // We have 2 modes here, depending on whether or not the client has asked for MD5 hash of the response;
        // If MD5 hash AND there's a subset range or Select operation, then we have to buffer the response, compute the 
        // hash, attach to the response header (because we can't modify response headers once we start streaming the body) and
        // then stream the body.
        // In all cases, we simply pipe the handlers together.
        var bufferResponse = this._azResponse.httpProps[N.RANGE_GET_CONTENT_MD5] &&
            (this.options.startByte > 0 || this.isSelectOperation());
        // TODO: For CSV Select operations that include header rows, we need to process the 
        // first row

        // If CSV Select with an offset, back up before the specified start to see if we're at the beginning of a row,
        // or we are part way through a row (which we will discard)
        var rangeSelect = this.isSelectOperation() && (this.options.startByte > 0 || this.options.endByte);
        var startByte = this.options.startByte;
        var endByte = this.options.endByte;
        if (rangeSelect) {
            startByte = Math.max(startByte - this.options.source.row_delim.length, 0);
            endByte += 10 * 1024;
        }
        var readStream = fs.createReadStream(this.options.object_filename, {
            flags: 'r',
            start: startByte,
            end: endByte,
            encoding: 'utf8'
        });
        var outputColumnsResolver = {};
        // Chain this stream to our Select transformer
        if (this.isSelectOperation()) {
            if (rangeSelect) {
                var lineFilter = new _LineRangeOversampler(readStream, 
                    this.options.source.column_delim, 
                    this.options.source.row_delim, 
                    this.options.startByte,
                    this.options.endByte);
                readStream = readStream.pipe(transform((buf, cb) => lineFilter.process(buf, cb)));
            }
            var selectFilter = new _SelectFilter(this.options, outputColumnsResolver);
            readStream = readStream
                .pipe(this._createFileParser())
                .pipe(transform((rows, cb) => selectFilter.process(rows, cb)));
        }
        if (bufferResponse) {
            // Buffer the response so that we can update headers prior to sending response
            var output = [];
            readStream.on('data', (chunk) => {
                output.push(chunk);
            });
            readStream.on('end', () => {
                const body = new Buffer(output, 'utf8');
                const hash = crypto.createHash('md5')
                    .update(body)
                    .digest('base64');
                this._azResponse.addHttpProperty(N.CONTENT_MD5, hash);
                this._res.set(this._azResponse.httpProps);
                this._res.status(206)
                    .send(body);
            });
        }
        else {
            // Stream directly to the response (after possibly encoding)
            if (this.isSelectOperation()) {
                readStream = readStream.pipe(this._createOutputFormatter(outputColumnsResolver));
            }
            readStream.pipe(this._res);
        }
    }

    isSelectOperation() {
        return this.options.odata_ast;
    }

    _createFileParser() {
        switch (this.options.source.file_type.toUpperCase()) {
            case 'CSV':
                var parser = new csv_parser({
                    separator: this.options.source.column_delim,
                    newline: this.options.source.row_delim,
                    trim: true
                });
                return parser;

            default:
                var baseError = ErrorCodes.InvalidInput;
                baseError.userMessage += ' Unsupported source file type. Supported types are; CSV';
                throw new AError(baseError);
        }
    }

    _createOutputFormatter(columnsResolver) {
        switch (this.options.dest.file_type.toUpperCase()) {
            case 'CSV':
                return transform((rows, cb) => {
                    csv_stringify(rows, {
                        delimiter: this.options.dest.column_delim,
                        rowDelimiter: this.options.dest.row_delim
                        // TODO: Add option for column headers in output from columnsResolver
                    }, cb);
                });

            case 'PARQUET':
                var parquetWriter = new Transform({
                    readableObjectMode: true,
                    writableObjectMode: true,
                    transform: (rows, encoding, cb) => {
                        var writer = (buf) => {
                            parquetWriter.push(buf);
                        };
                        if (!parquetWriter.writer) {
                            var schema = null;
                            if (columnsResolver.getColumns) {
                                schema = new parquet.ParquetSchema(columnsResolver.getColumns().reduce((obj, column) => { 
                                    return { ...obj, [column.name]: {
                                        // TODO: Sort out typing properly
                                        type: 'UTF8'
                                    } }; 
                                }, {}));
                            }
                            var parquetOpts = {useDataPageV2: false};
                            var envWriter = new parquet.ParquetEnvelopeWriter(schema, writer, () => {}, 0, parquetOpts);
                            envWriter.writeHeader();
                            parquetWriter.writer = new parquet.ParquetWriter(schema, envWriter, parquetOpts);
                        }
                        asyncForEach(rows, async row => await parquetWriter.writer.appendRow(row))
                            .then(v => {
                                cb();
                            });
                    },
                    flush: (cb) => {
                        if (parquetWriter.writer) {
                            parquetWriter.writer.close()
                                .then(v => cb());
                        }
                        else {
                            cb();
                        }
                    }
                });
                return parquetWriter;

            default:
                var baseError = ErrorCodes.InvalidInput;
                baseError.userMessage += ' Unsupported destination file type. Supported types are; CSV';
                throw new AError(baseError);
        }
    }
}

async function asyncForEach(array, callback) {
    for (let index = 0; index < array.length; index++) {
        await callback(array[index], index, array)
    }
}

class _LineRangeOversampler {
    constructor(sourceStream, separator, rowDelimiter, rangeStart, rangeEnd) {
        this._sourceStream = sourceStream;
        this._positionFirstLine = rangeStart > 0;
        this._columnSeparator = Buffer.from(separator)[0];
        this._rowDelimiter = Buffer.from(rowDelimiter)[0];
        this._rangeEnd = rangeEnd;
        this._bufFileOffset = -1;
        this._finished = false;
    }    

    // This is the transform() callback to filter out the partial first row & handle the oversampling last row logic
    process(buf, cb) {
        // This transformer is to determine if we start at the beginning of a row or in the middle of it
        // This transformer also handles the logic to oversample the range (if specified)
        if (this._bufFileOffset < 0) {
            this._bufFileOffset = this._sourceStream.start;
        }
        var thisBufOffset = this._bufFileOffset;
        this._bufFileOffset += buf.length;
        if (this._positionFirstLine) {
            if (typeof buf === 'string') {
                buf = Buffer.from(buf);
            }
            if (buf[0] === this._rowDelimiter) {
                // Our Range specified the beginning of a line - process from there
                buf = buf.slice(1);
            }
            else {
                // Find EOL and process from there
                var processEnd = csv_parser.processBuffer(buf, 
                    0, 
                    this._columnSeparator, 
                    this._rowDelimiter, 
                    null, null, null,
                    (row) => false,
                    false);
                buf = buf.slice(processEnd);
            }
            this._positionFirstLine = false;
        }
        if (this._finished) {
            buf = null;
        }
        else if (this._rangeEnd && this._rangeEnd < this._sourceStream.pos) {
            var bufCheckOffset = this._rangeEnd - thisBufOffset;
            if (typeof buf === 'string') {
                buf = Buffer.from(buf);
            }
            var endOfLinePos = csv_parser.processBuffer(buf, 
                bufCheckOffset, 
                this._columnSeparator, 
                this._rowDelimiter, 
                null, null, null,
                (row) => false,
                false);
            if (endOfLinePos) {
                buf = buf.slice(0, endOfLinePos);
                this._finished = true;
            }
        }
        cb(null, buf);
    }
}

class _SelectFilter {
    constructor(options, outputColumnsResolver) {
        this._options = options;
        this.rowNum = 0;
        this._outputColumnsResolver = outputColumnsResolver;
    }

    process(rows, cb) {
        // Setup output columns, if we haven't done already
        if (!this._outputColumnsResolver.getColumns && rows.length) {
            var self = this;
            var firstRow = rows[0];
            this._outputColumnsResolver.getColumns = () => {
                if (self._options.odata_ast['$select']) {
                    return self._options.odata_ast['$select'].map(selectColumn => {
                        return {
                            name: selectColumn,
                            // TODO: Infer data typing from first row
                            dataType: 'string'
                        };
                    });
                }
                return Object.entries(firstRow).map(prop => {
                    return {
                        name: prop[0],
                        // TODO: Infer data typing from data
                        dataType: 'string'
                    };
                });
            };
        }
        var encodedRows = [];
        for (var row of rows) {
            var processRow = true;
            this.rowNum++;
            // If we're evaluating a predicate, do this now
            if (this._options.odata_ast['$filter']) {
                processRow = this.evaluateExpression(this._options.odata_ast['$filter'], row);
            }
            if (processRow && this._options.odata_ast['$select']) {
                var newRow = {};
                for (var projectedCol in this._options.odata_ast['$select']) {
                    var colName = this._options.odata_ast['$select'][projectedCol];
                    newRow[colName] = row[colName];
                }
                row = newRow;
            }
            if (processRow) {
                encodedRows.push(row);
            }
        }
        cb(null, encodedRows);
    }

    evaluateExpression(expression, data) {
        switch (expression.type.toUpperCase()) {
            case 'OR':
                return this.evaluateExpression(expression.left, data) || this.evaluateExpression(expression.right, data);

            case 'AND':
                return this.evaluateExpression(expression.left, data) && this.evaluateExpression(expression.right, data);
                
            default:
                return this.compareValues(this.evaluateValue(expression.left, data), this.evaluateValue(expression.right, data), expression.type);
        }
    }

    evaluateValue(clause, data) {
        var retval = undefined;
        switch (clause.type.toUpperCase()) {
            case 'LITERAL':
                retval = clause.value;
                break;

            case 'PROPERTY':
                retval = data[clause.name];
                break;
        }
        return {
            value: retval,
            type: clause.type
        };
    }

    compareValues(lhs, rhs, operator) {
        // Perform implicit type conversion based on literal types
        var lhsValue, rhsValue;
        operator = operator.toUpperCase();
        if (lhs.type.toUpperCase() === 'LITERAL') {
            lhsValue = this.coerceValue(lhs.value, lhs.value, operator);
            rhsValue = this.coerceValue(lhs.value, rhs.value, operator);
        }
        else if (rhs.type.toUpperCase() === 'LITERAL') {
            rhsValue = this.coerceValue(rhs.value, rhs.value, operator);
            lhsValue = this.coerceValue(rhs.value, lhs.value, operator);
        }
        else {
            lhsValue = lhs.value;
            rhsValue = rhs.value;
        }
        switch (operator) {
            case 'EQ':
                return lhsValue == rhsValue;

            case 'NE':
                return lhsValue != rhsValue;

            case 'GT':
                return lhsValue > rhsValue;

            case 'GE':
                return lhsValue >= rhsValue;

            case 'LT':
                return lhsValue < rhsValue;

            case 'LE':
                return lhsValue <= rhsValue;
        }
    }

    coerceValue(sourceOperand, coercedOperand, operator) {
        var retval = coercedOperand;
        if (sourceOperand instanceof Date) {
            retval = new Date(coercedOperand);
            switch (operator) {
                case 'EQ':
                case 'NE':
                    retval = retval.getTime();
                    break;
            }
        }
        return retval;
    }
}

module.exports = SelectProcessor;
    