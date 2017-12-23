'use strict';

const env = require('./../../core/env'),
    ContainerRequest = require('./../../model/blob/AzuriteContainerRequest'),
    Operations = require('./../../core/Constants').Operations;

/*
 * Route definitions for all operation on the 'Account' resource type.
 * See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/blob-service-rest-api
 * for details on specification.
 */
module.exports = (app) => {
    app.route(`/`)
        .get((req, res, next) => {
            if (req.query.comp === 'list') {
                req.azuriteOperation = Operations.Account.LIST_CONTAINERS;
            }
            req.azuriteRequest = new ContainerRequest({ req: req });
            next();
        });
}