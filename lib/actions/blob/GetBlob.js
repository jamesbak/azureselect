'use strict';

const storageManager = require('./../../core/blob/StorageManager'),
    BbPromise = require('bluebird'),
    N = require('./../../core/HttpHeaderNames'),
    EntityType = require('./../../core/Constants').StorageEntityType,
    env = require('./../../core/env'),
    req = require('request'),
    fs = require("fs-extra"),
    crypto = require('crypto'),
    select = require('./../../core/SelectProcessor');

class GetBlob {
    constructor() {
    }

    process(request, res) {
        const range = request.httpProps[N.RANGE];
        BbPromise.try(() => {
            return storageManager.getBlob(request)
                .then((response) => {
                    response.addHttpProperty(N.ACCEPT_RANGES, 'bytes');
                    response.addHttpProperty(N.BLOB_TYPE, response.proxy.original.entityType);
                    response.addHttpProperty(N.REQUEST_SERVER_ENCRYPTED, false);
                    response.addHttpProperty(N.CONTENT_TYPE, response.proxy.original.contentType);
                    // response.addHttpProperty(N.CONTENT_MD5, response.proxy.original.md5);
                    response.addHttpProperty(N.CONTENT_LANGUAGE, response.proxy.original.contentLanguage);
                    response.addHttpProperty(N.CONTENT_ENCODING, response.proxy.original.contentEncoding);
                    response.addHttpProperty(N.CONTENT_DISPOSITION, response.proxy.original.contentDisposition);
                    response.addHttpProperty(N.CACHE_CONTROL, response.proxy.original.cacheControl);
                    if (request.auth) response.sasOverrideHeaders(request.query);

                    var processor = new select(request, res, response);
                    processor.sendResponse(request.httpProps[N.RANGE_GET_CONTENT_MD5]);
                })
            })
        .catch(e => {
            res.status(e.statusCode || 500)
                .send(e.message);
            if (!e.statusCode) throw e;
        });
    }

    _createRequestHeader(url, range) {
        const request = {};
        request.headers = {};
        request.url = url;
        if (range) {
            request.headers.Range = range
        }
        return request;
    }
}

module.exports = new GetBlob();