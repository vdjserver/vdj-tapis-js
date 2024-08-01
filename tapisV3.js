'use strict';

//
// tapisV3.js
// Wrapper functions for accessing the Tapis V3 APIs
//
// VDJServer
// http://vdjserver.org
//
// Copyright (C) 2022 The University of Texas Southwestern Medical Center
//
// Author: Scott Christley <scott.christley@utsouthwestern.edu>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
//

var tapisV3  = {};
module.exports = tapisV3;

// Node Libraries
var _ = require('underscore');
var jsonApprover = require('json-approver');
var FormData = require('form-data');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');

// Settings
var tapisSettings = require('./tapisSettings');
tapisV3.tapisSettings = tapisSettings;
var config = tapisSettings.config;

// Models
var ServiceAccount = require('./serviceAccountV3');
tapisV3.serviceAccount = ServiceAccount;
var GuestAccount = require('./guestAccountV3');
tapisV3.guestAccount = GuestAccount;

// Controllers
var AuthController = require('./authControllerV3');
tapisV3.authController = AuthController;

// Error logging to Slack
var webhookIO = require('./webhookIO');
tapisV3.webhookIO = webhookIO;

// attach schema to be used for validation tapis meta objects
tapisV3.init_with_schema = function(schema) {
    if (schema) {
        console.log('vdj-tapis (tapisV3) schema init:', schema.get_info()['title'], 'version', schema.get_info()['version']);
    }
    tapisV3.schema = schema;
    return tapisV3;
};

//
// Generic send request
//
tapisV3.sendRequest = async function(requestSettings, allow404, trap408) {
    var msg = null;

    const response = await axios(requestSettings)
        .catch(function(error) {
            if (allow404 && (error.response.status == 404)) {
                return Promise.resolve(null);
            }
            if (error.response && error.response.data) msg = 'Tapis request failed with error: ' + JSON.stringify(error.response.data);
            else msg = 'Tapis request failed with error: ' + JSON.stringify(error);
            return Promise.reject(new Error(msg));
        });

    //console.log(response);
    //console.log(response.data);
    //console.log(response.status);
    //console.log(response.statusText);
    //console.log(response.headers);
    //console.log(response.config);

    return Promise.resolve(response.data);
};

// Fetches a user token based on the supplied auth object
// and returns the auth object with token data on success

tapisV3.getToken = async function(auth) {
    var postData = {
        username: auth.username,
        password: auth.password,
        grant_type: 'password'
    };

    var url = 'https://' + tapisSettings.hostnameV3 + '/v3/oauth2/tokens';
    var requestSettings = {
        url: url,
        method: 'POST',
        data: postData,
        headers: {
            'Content-Type':   'application/json'
        }
    };
    //console.log(requestSettings);

    var msg = null;
    var data = await tapisV3.sendRequest(requestSettings)
        .catch(function(error) {
            return Promise.reject(error);
        });

    if (data.status != 'success') {
        msg = 'Tapis token response returned an error: ' + data.message;
        return Promise.reject(new Error(msg));
    }

    return Promise.resolve(data.result);
};

//
/////////////////////////////////////////////////////////////////////
//
// generic Tapis V3 meta operations
//

tapisV3.recordQuery = function(query) {

    return ServiceAccount.getToken()
        .then(function(token) {

            var postData = JSON.stringify([ query ]);

            var requestSettings = {
                url: 'https://' + tapisSettings.hostnameV3 + '/v3/meta/' + tapisSettings.mongo_dbname + '/query',
                method: 'POST',
                data: postData,
                headers: {
                    'Accept':   'application/json',
                    'X-Tapis-Token': ServiceAccount.accessToken(),
                    'Content-Type': 'application/json'
                }
            };

            //console.log(requestSettings);

            return tapisV3.sendRequest(requestSettings);
        });
};

// raw record creation
// can be used to create one or multiple records
tapisV3.createRecord = function(collection, data) {
    //if (tapisSettings.shouldInjectError("tapisIO.createMetadataForType")) return tapisSettings.performInjectError();

    // TODO: error if no collection
    // TODO: error if no data
    if (Array.isArray(data))
        var postData = JSON.stringify(data);
    else
        var postData = JSON.stringify([ data ]);

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                url: 'https://' + tapisSettings.hostnameV3 + '/v3/meta/' + tapisSettings.mongo_dbname + '/' + collection,
                method: 'POST',
                data: postData,
                headers: {
                    'Accept':   'application/json',
                    'X-Tapis-Token': ServiceAccount.accessToken(),
                    'Content-Type': 'application/json'
                }
            };

            //console.log(requestSettings);

            return tapisV3.sendRequest(requestSettings);
        });
};

// document creation that follows tapis_meta schema
// use the project specific functions for project metadata
tapisV3.createDocument = function(name, value, associationIds, owner, extras, skip_validate) {
    //if (tapisSettings.shouldInjectError("tapisV3.createDocument")) return tapisSettings.performInjectError();

    if (!name) return Promise.reject(new Error('name cannot be null for document.'));

    var date = new Date().toISOString();
    var uuid = uuidv4();
    var postData = {
        uuid: uuid,
        associationIds: [],
        owner: ServiceAccount.username,
        created: date,
        lastUpdated: date,
        name: name,
        value: {}
    };
    if (value) postData['value'] = value;
    if (associationIds) postData['associationIds'] = associationIds;
    if (owner) postData['owner'] = owner;
    if (extras) {
        for (let i in extras) {
            postData[i] = extras[i];
        }
    }

    // validate
    if (!skip_validate) {
        if (tapisV3.schema) {
            let s = tapisV3.schema.spec_for_tapis_name(postData['name']);
            if (!s) return Promise.reject('Cannot find spec with tapis name: ' + postData['name']);
            let error = s.validate_object(postData, ['x-vdjserver']);
            if (error) return Promise.reject('Invalid object with tapis name: ' + postData['name'] + ', error: ' + JSON.stringify(error));
        } else return Promise.reject('Schema is not set for Tapis V3.');
    }

    var collection = 'tapis_meta';

    return tapisV3.createRecord(collection, postData)
        .then(function(data) {
            console.log(JSON.stringify(data));
            var filter = { "uuid": postData['uuid'] };
            var query = JSON.stringify(filter);
            return tapisV3.performMultiServiceQuery(collection, query);
        })
        .then(function(data) {
            if (data.length != 1) return Promise.reject(new Error('Internal error: new document query with uuid: ' + postData['uuid'] + ' returned incorrect number of documents (' + data.length + ' != 1)'));
            console.log(JSON.stringify(data));
            return Promise.resolve(data[0]);
        });
};

// raw record replacement
tapisV3.updateRecord = function(collection, doc_id, data) {
    //if (tapisSettings.shouldInjectError("tapisIO.updateRecord")) return tapisSettings.performInjectError();

    // TODO: error if no collection
    // TODO: error if no doc id
    // TODO: error if no data
    var postData = JSON.stringify(data);

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                url: 'https://' + tapisSettings.hostnameV3 + '/v3/meta/' + tapisSettings.mongo_dbname + '/' + collection + '/' + doc_id,
                method: 'PUT',
                data: postData,
                headers: {
                    'Accept':   'application/json',
                    'X-Tapis-Token': ServiceAccount.accessToken(),
                    'Content-Type': 'application/json'
                }
            };

            //console.log(requestSettings);

            return tapisV3.sendRequest(requestSettings);
        });
};

// document updated that follows tapis_meta schema
// use the project specific functions for project metadata
// security: it is assumed user has access
tapisV3.updateDocument = async function(meta_uuid, name, value, associationIds, owner, extras, skip_validate) {
    //if (tapisSettings.shouldInjectError("tapisV3.updateDocument")) return tapisSettings.performInjectError();

    // retrieve by uuid
    var filter = { "uuid": meta_uuid };
    var query = JSON.stringify(filter);
    var metadata = await tapisV3.performMultiServiceQuery('tapis_meta', query)
        .catch(function(error) { Promise.reject(error); });

    // do some checks

    // this shouldn't happen
    if (!metadata) return Promise.reject(new Error('empty query response.'));
    // 404 not found
    if (metadata.length == 0) return Promise.resolve(null);
    // yikes!
    if (metadata.length != 1) return Promise.reject(new Error('internal error, multiple records have the same uuid.'));
    // eliminate array
    metadata = metadata[0];
    // it better have a doc id
    if (!metadata['_id']) return Promise.reject(new Error('internal error, metadata return is missing _id'));
    if (!metadata['_id']['$oid']) return Promise.reject(new Error('internal error, metadata return is missing $oid'));

    // update
    if (name) metadata['name'] = name;
    if (value) metadata['value'] = value;
    if (associationIds) metadata['associationIds'] = associationIds;
    if (owner) metadata['owner'] = owner;
    if (extras) {
        for (let i in extras) {
            metadata[i] = extras[i];
        }
    }
    metadata['lastUpdated'] = new Date().toISOString();

    // validate
    if (!skip_validate) {
        if (tapisV3.schema) {
            let s = tapisV3.schema.spec_for_tapis_name(metadata['name']);
            if (!s) return Promise.reject('Cannot find spec with tapis name: ' + metadata['name']);
            let error = s.validate_object(metadata, ['x-vdjserver']);
            if (error) return Promise.reject('Invalid object with tapis name: ' + metadata['name'] + ', error: ' + JSON.stringify(error));
        } else return Promise.reject('Schema is not set for Tapis V3.');
    }

    // finally do the update
    await tapisV3.updateRecord('tapis_meta', metadata['_id']['$oid'], metadata)
        .catch(function(error) { Promise.reject(error); });

    // retrieve again and return
    filter = { "uuid": meta_uuid };
    query = JSON.stringify(filter);
    metadata = await tapisV3.performMultiServiceQuery('tapis_meta', query)
        .catch(function(error) { Promise.reject(error); });

    // yikes!
    if (metadata.length != 1) return Promise.reject(new Error('internal error, after update, multiple records have the same uuid.'));

    return Promise.resolve(metadata[0]);
};

// raw record delete
tapisV3.deleteRecord = function(collection, doc_id) {
    //if (tapisSettings.shouldInjectError("tapisIO.deleteRecord")) return tapisSettings.performInjectError();

    if (!collection) Promise.reject(new Error('collection not specified'));
    if (!doc_id) Promise.reject(new Error('document id not specified'));

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                url: 'https://' + tapisSettings.hostnameV3 + '/v3/meta/' + tapisSettings.mongo_dbname + '/' + collection + '/' + doc_id,
                method: 'DELETE',
                headers: {
                    'Accept':   'application/json',
                    'X-Tapis-Token': ServiceAccount.accessToken(),
                    'Content-Type': 'application/json'
                }
            };

            console.log(requestSettings);

            return tapisV3.sendRequest(requestSettings);
        });
};

// document creation that follows tapis_meta schema
// use the project specific functions for project metadata
tapisV3.createMultipleDocuments = function(docs, skip_validate) {
    //if (tapisSettings.shouldInjectError("tapisV3.createMultipleDocuments")) return tapisSettings.performInjectError();

    if (!docs) return Promise.reject(new Error('no documents provided.'));
    if (docs.length == 0) return Promise.reject(new Error('no documents provided.'));

    var objs = [];
    var uuids = [];
    var date = new Date().toISOString();
    for (let i in docs) {
        let obj = docs[i];
        if (! obj['name']) return Promise.reject(new Error('object at index: ' + i + ' is missing name.'));
        let uuid = uuidv4();
        let new_obj = {
            uuid: uuid,
            associationIds: [],
            owner: ServiceAccount.username,
            created: date,
            lastUpdated: date,
            name: obj['name'],
            value: {}
        };
        if (obj['value']) new_obj['value'] = obj['value'];
        if (obj['associationIds']) new_obj['associationIds'] = obj['associationIds'];
        if (obj['owner']) new_obj['owner'] = obj['owner'];
        if (obj['extras']) {
            for (let j in obj['extras']) {
                new_obj[j] = obj['extras'][j];
            }
        }
        objs.push(new_obj);
        uuids.push(new_obj['uuid']);
    }

    // validate
    if (!skip_validate) {
        if (tapisV3.schema) {
            for (let i in objs) {
                let s = tapisV3.schema.spec_for_tapis_name(objs[i]['name']);
                if (!s) return Promise.reject('Cannot find spec with tapis name: ' + objs[i]['name']);
                let error = s.validate_object(objs[i], ['x-vdjserver']);
                if (error) return Promise.reject('Invalid object at index: ' + i + ' with tapis name: ' + objs[i]['name'] + ', error: ' + JSON.stringify(error));
            }
        } else return Promise.reject('Schema is not set for Tapis V3.');
    }

    var collection = 'tapis_meta';

    return tapisV3.createRecord(collection, objs)
        .then(function(data) {
            // TODO: do we check that right number of records were inserted?
            // we return the list of uuids
            return Promise.resolve(uuids);
        });
};

// query using guest account
tapisV3.performQuery = function(collection, query, projection, page, pagesize, count) {

    return GuestAccount.getToken()
        .then(function(token) {
            var mark = false;
            var url = 'https://' + tapisSettings.hostnameV3 + '/v3/meta/' + tapisSettings.mongo_dbname + '/' + collection;

            if (count) {
                url += '/_size';
            }
            if (query != null) {
                if (mark) url += '&';
                else url += '?';
                mark = true;
                url += 'filter=' + encodeURIComponent(query);
            }
            if (projection != null) {
                if (mark) url += '&';
                else url += '?';
                mark = true;
                url += 'keys=' + encodeURIComponent(JSON.stringify(projection));
            }
            if (page != null) {
                if (mark) url += '&';
                else url += '?';
                mark = true;
                url += 'page=' + encodeURIComponent(page);
            }
            if (pagesize != null) {
                if (mark) url += '&';
                else url += '?';
                mark = true;
                url += 'pagesize=' + encodeURIComponent(pagesize);
            }
            var sort = {};
            if (sort) {
                if (mark) url += '&';
                else url += '?';
                mark = true;
                url += 'sort=' + encodeURIComponent(JSON.stringify(sort));
            }

            var requestSettings = {
                url: url,
                method: 'GET',
                headers: {
                    'Accept': 'application/json',
                    'X-Tapis-Token': GuestAccount.accessToken()
                }
            };

            //console.log(requestSettings);

            return tapisV3.sendRequest(requestSettings);
        });
};

// query using service account
tapisV3.performServiceQuery = function(collection, query, projection, page, pagesize, count) {

    return ServiceAccount.getToken()
        .then(function(token) {
            var mark = false;
            var url = 'https://' + tapisSettings.hostnameV3 + '/v3/meta/' + tapisSettings.mongo_dbname + '/' + collection;

            if (count) {
                url += '/_size';
            }
            if (query != null) {
                if (mark) url += '&';
                else url += '?';
                mark = true;
                url += 'filter=' + encodeURIComponent(query);
            }
            if (projection != null) {
                if (mark) url += '&';
                else url += '?';
                mark = true;
                url += 'keys=' + encodeURIComponent(JSON.stringify(projection));
            }
            if (page != null) {
                if (mark) url += '&';
                else url += '?';
                mark = true;
                url += 'page=' + encodeURIComponent(page);
            }
            if (pagesize != null) {
                if (mark) url += '&';
                else url += '?';
                mark = true;
                url += 'pagesize=' + encodeURIComponent(pagesize);
            }
            var sort = {};
            if (sort) {
                if (mark) url += '&';
                else url += '?';
                mark = true;
                url += 'sort=' + encodeURIComponent(JSON.stringify(sort));
            }

            var requestSettings = {
                url: url,
                method: 'GET',
                headers: {
                    'Accept': 'application/json',
                    'X-Tapis-Token': ServiceAccount.accessToken()
                }
            };

            //console.log(requestSettings);

            return tapisV3.sendRequest(requestSettings);
        });
};

// general large queries
tapisV3.performLargeQuery = function(collection, query, projection, page, pagesize) {

    var postData = query;
    if (! postData) return Promise.reject(new Error('TAPIS-API ERROR: Empty query passed to tapisV3.performLargeQuery'));

    return GuestAccount.getToken()
        .then(function(token) {
            var mark = false;
            var url = 'https://' + tapisSettings.hostnameV3 + '/v3/meta/' + tapisSettings.mongo_dbname + '/' + collection + '/_filter';

            if (projection != null) {
                if (mark) url += '&';
                else url += '?';
                mark = true;
                url += 'keys=' + encodeURIComponent(JSON.stringify(projection));
            }
            if (page != null) {
                if (mark) url += '&';
                else url += '?';
                mark = true;
                url += 'page=' + encodeURIComponent(page);
            }
            if (pagesize != null) {
                if (mark) url += '&';
                else url += '?';
                mark = true;
                url += 'pagesize=' + encodeURIComponent(pagesize);
            }
            var sort = {};
            if (sort) {
                if (mark) url += '&';
                else url += '?';
                mark = true;
                url += 'sort=' + encodeURIComponent(JSON.stringify(sort));
            }

            var requestSettings = {
                url: url,
                method: 'POST',
                data: JSON.stringify(postData),
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'X-Tapis-Token': GuestAccount.accessToken()
                }
            };

            //console.log(requestSettings);

            return tapisV3.sendRequest(requestSettings);
        });
};

// General query that performs multiple requests to retrieve all of the results.
// Will utilize the appropriate function based upon the size of the query.
// This should not be utilized for queries that may return a large amount of data
// because the data is pulled into memory.
tapisV3.performMultiQuery = function(collection, query, projection, start_page, pagesize) {
    var models = [];
    if (!start_page) start_page = 1;

    //console.log(query);
    var doQuery = function(page) {
        var queryFunction = tapisV3.performQuery;
        if (query && query.length > tapisSettings.large_query_size) queryFunction = tapisV3.performLargeQuery;
        return queryFunction(collection, query, projection, page, pagesize)
            .then(function(records) {
                if (tapisSettings.debugConsole) console.log('TAPIS-API INFO: query returned ' + records.length + ' records.');
                if (records.length == 0) {
                    return Promise.resolve(models);
                } else {
                    models = models.concat(records);
                    if (records.length < pagesize) return Promise.resolve(models);
                    else return doQuery(page+1);
                }
            })
            .catch(function(errorObject) {
                return Promise.reject(errorObject);
            });
    };

    return doQuery(start_page);
}

tapisV3.performMultiServiceQuery = function(collection, query, projection, start_page, pagesize) {
    var models = [];
    if (!start_page) start_page = 1;
    if (!pagesize) pagesize = 1000;

    //console.log(query);
    var doQuery = function(page) {
        var queryFunction = tapisV3.performServiceQuery;
        // TODO: do we large query?
        //if (query && query.length > tapisSettings.large_query_size) queryFunction = tapisV3.performLargeQuery;
        return queryFunction(collection, query, projection, page, pagesize)
            .then(function(records) {
                if (tapisSettings.debugConsole) console.log('TAPIS-API INFO: query returned ' + records.length + ' records.');
                if (records.length == 0) {
                    return Promise.resolve(models);
                } else {
                    models = models.concat(records);
                    if (records.length < pagesize) return Promise.resolve(models);
                    else return doQuery(page+1);
                }
            })
            .catch(function(errorObject) {
                return Promise.reject(errorObject);
            });
    };

    return doQuery(start_page);
}

// restrict results by username
// objects must have a permission
tapisV3.performMultiUserQuery = function(username, collection, query, projection, start_page, pagesize) {
    var models = [];
    if (!start_page) start_page = 1;
    if (!pagesize) pagesize = 1000;

    //console.log(query);
    var doQuery = function(page) {
        var queryFunction = tapisV3.performServiceQuery;
        // TODO: do we large query?
        //if (query && query.length > tapisSettings.large_query_size) queryFunction = tapisV3.performLargeQuery;
        return queryFunction(collection, query, projection, page, pagesize)
            .then(function(records) {
                if (tapisSettings.debugConsole) console.log('TAPIS-API INFO: query returned ' + records.length + ' records.');
                if (records.length == 0) {
                    return Promise.resolve(models);
                } else {
                    // service account sees all
                    if (username == tapisSettings.serviceAccountKey)
                        models = models.concat(records);
                    else {
                        // otherwise check that user has write permission
                        for (let i = 0; i < records.length; ++i) {
                            let obj = records[i];
                            if (obj.permission) {
                                for (let j = 0; j < obj.permission.length; ++j) {
                                    if (obj.permission[j]['username'] == username && obj.permission[j]['permission'] && obj.permission[j]['permission']['write']) {
                                        models.push(obj);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if (records.length < pagesize) return Promise.resolve(models);
                    else return doQuery(page+1);
                }
            })
            .catch(function(errorObject) {
                return Promise.reject(errorObject);
            });
    };

    return doQuery(start_page);
}

// general aggregation
tapisV3.performAggregation = function(collection, aggregation, query, field, page, pagesize) {

    if (! query) return Promise.reject(new Error('TAPIS-API ERROR: Empty query passed to tapisV3.performAggregation'));
    if (! field) return Promise.reject(new Error('TAPIS-API ERROR: Empty field passed to tapisV3.performAggregation'));

    return GuestAccount.getToken()
        .then(function(token) {
            var url = 'https://' + tapisSettings.hostnameV3 + '/v3/meta/' + tapisSettings.mongo_dbname + '/' + collection + '/_aggrs/' + aggregation;
            url += '?avars=';
            url += encodeURIComponent('{"match":' + query + ',"field":"' + field + '"}');
            var mark = true;

            if (page != null) {
                if (mark) requestSettings['path'] += '&';
                else requestSettings['path'] += '?';
                mark = true;
                requestSettings['path'] += 'page=' + encodeURIComponent(page);
            }
            if (pagesize != null) {
                if (mark) requestSettings['path'] += '&';
                else requestSettings['path'] += '?';
                mark = true;
                requestSettings['path'] += 'pagesize=' + encodeURIComponent(pagesize);
            }

            var requestSettings = {
                url: url,
                method: 'GET',
                headers: {
                    'Accept': 'application/json',
                    'X-Tapis-Token': GuestAccount.accessToken()
                }
            };

            console.log(requestSettings);

            return tapisV3.sendRequest(requestSettings, false, true);
        });
};

// general large aggregation
tapisV3.performLargeAggregation = function(collection, aggregation, query, field, page, pagesize) {

    if (! query) return Promise.reject(new Error('TAPIS-API ERROR: Empty query passed to tapisV3.performLargeAggregation'));
    if (! field) return Promise.reject(new Error('TAPIS-API ERROR: Empty field passed to tapisV3.performLargeAggregation'));

    var postData = '{"match":' + query + ',"field":"' + field + '"}';
    //console.log(postData);

    return GuestAccount.getToken()
        .then(function(token) {
            var mark = false;
            var url = 'https://' + tapisSettings.hostnameV3 + '/v3/meta/' + tapisSettings.mongo_dbname + '/' + collection + '/_aggrs/' + aggregation;

            if (page != null) {
                if (mark) url += '&';
                else url += '?';
                mark = true;
                url += 'page=' + encodeURIComponent(page);
            }
            if (pagesize != null) {
                if (mark) url += '&';
                else url += '?';
                mark = true;
                url += 'pagesize=' + encodeURIComponent(pagesize);
            }

            var requestSettings = {
                url: url,
                method: 'POST',
                data: postData,
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'X-Tapis-Token': GuestAccount.accessToken()
                }
            };

            console.log(requestSettings);

            return tapisV3.sendRequest(requestSettings, false, true);
        });
};

//
/////////////////////////////////////////////////////////////////////
//
// Project operations
//

// create metadata record for a private project
tapisV3.createProjectMetadata = async function(username, project) {
    //if (tapisSettings.shouldInjectError("tapisIO.createProjectMetadata")) return tapisSettings.performInjectError();

    var date = new Date().toISOString();
    var uuid = uuidv4();
    var postData = {
        uuid: uuid,
        associationIds: [],
        owner: username,
        created: date,
        lastUpdated: date,
        name: 'private_project',
        value: project,
        permission: [{ "username": username, permission: { read: true, write: true } }]
    };

    // validate
    if (tapisV3.schema) {
        let s = tapisV3.schema.spec_for_tapis_name('private_project');
        if (!s) return Promise.reject('Cannot find spec with tapis name: private_project');
        let error = s.validate_object(postData, ['x-vdjserver']);
        if (error) return Promise.reject('Invalid object with tapis name: private_project, error: ' + JSON.stringify(error));
    }

    return tapisV3.createRecord('tapis_meta', postData)
        .then(function(data) {
            //console.log(JSON.stringify(data));
            return tapisV3.getProjectMetadata(username, uuid);
        })
        .then(function(data) {
            //console.log(JSON.stringify(data));
            return Promise.resolve(data[0]);
        });
};

// get private projects for a user
// or single project given uuid
tapisV3.getProjectMetadata = function(username, project_uuid) {
    //if (tapisSettings.shouldInjectError("tapisV3.getProjectMetadata")) return tapisSettings.performInjectError();

    var filter = { "name": "private_project" };
    if (project_uuid) filter['uuid'] = project_uuid;
    var query = JSON.stringify(filter);
    return tapisV3.performMultiUserQuery(username, 'tapis_meta', query);
};

// get public projects for a user
// or single public project given uuid
tapisV3.getPublicProjectMetadata = function(username, project_uuid) {
    //if (tapisSettings.shouldInjectError("tapisV3.getProjectMetadata")) return tapisSettings.performInjectError();

    var filter = { "name": "public_project" };
    if (project_uuid) filter['uuid'] = project_uuid;
    var query = JSON.stringify(filter);
    return tapisV3.performMultiUserQuery(username, 'tapis_meta', query);
};

// get any/all public projects
// or single public project given uuid
tapisV3.getAnyPublicProjectMetadata = function(project_uuid) {
    //if (tapisSettings.shouldInjectError("tapisV3.getProjectMetadata")) return tapisSettings.performInjectError();

    var filter = { "name": { "$in": ["private_project", "public_project"] } };
    if (project_uuid) filter['uuid'] = project_uuid;
    var query = JSON.stringify(filter);
    return tapisV3.performMultiServiceQuery('tapis_meta', query);
};

// query metadata associated with project
// security: it is assumed user has project access
tapisV3.queryMetadataForProject = function(project_uuid, meta_name, additional_filters) {
    //if (tapisSettings.shouldInjectError("tapisV3.queryMetadataForProject")) return tapisSettings.performInjectError();

    var filter = { "name": meta_name, "associationIds": project_uuid };
    if (additional_filters) {
        for (let k in additional_filters) {
            if (k == 'name') continue;
            if (k == 'associationIds') continue;
            filter[k] = additional_filters[k];
        }
    }
    var query = JSON.stringify(filter);
    return tapisV3.performMultiServiceQuery('tapis_meta', query);
};

// create metadata associated with project
// security: it is assumed user has project access
tapisV3.createMetadataForProject = async function(project_uuid, meta_name, obj) {
    //if (tapisSettings.shouldInjectError("tapisV3.createMetadataForProject")) return tapisSettings.performInjectError();

    var date = new Date().toISOString();
    var uuid = uuidv4();
    var metadata = {
        uuid: uuid,
        associationIds: [ ],
        owner: tapisSettings.serviceAccountKey,
        created: date,
        lastUpdated: date,
        name: meta_name,
        value: obj['value']
    };

    if (obj['associationIds']) metadata['associationIds'] = obj['associationIds'];
    if (!metadata['associationIds'].includes(project_uuid)) metadata['associationIds'].push(project_uuid);

    // validate
    if (tapisV3.schema) {
        let s = tapisV3.schema.spec_for_tapis_name(meta_name);
        if (!s) return Promise.reject('Cannot find spec with tapis name: ' + meta_name);
        if (obj['value']['vdjserver']) {
            obj['value']['vdjserver']['vdjserver_uuid'] = uuid;
            obj['value']['vdjserver']['version'] = tapisV3.schema.get_info()['version'];
        }
        let error = s.validate_object(metadata, ['x-vdjserver']);
        if (error) return Promise.reject('Invalid object with tapis name: ' + meta_name + ', error: ' + JSON.stringify(error));
    }

    // create the record
    await tapisV3.createRecord('tapis_meta', metadata)
        .catch(function(error) { Promise.reject(error); });

    // retrieve again and return
    metadata = await tapisV3.getMetadataForProject(project_uuid, uuid)
        .catch(function(error) { Promise.reject(error); });

    // yikes!
    if (metadata.length != 1) return Promise.reject(new Error('internal error, after create, wrong # of records have the same uuid.'));

    return Promise.resolve(metadata[0]);
};

// update metadata associated with project
// security: it is assumed user has project access
tapisV3.updateMetadataForProject = async function(project_uuid, meta_uuid, obj) {
    //if (tapisSettings.shouldInjectError("tapisV3.updateMetadataForProject")) return tapisSettings.performInjectError();

    // retrieve by uuid
    var filter = { "uuid": meta_uuid };
    var query = JSON.stringify(filter);
    var metadata = await tapisV3.performMultiServiceQuery('tapis_meta', query)
        .catch(function(error) { Promise.reject(error); });

    // do some checks

    // this shouldn't happen
    if (!metadata) return Promise.reject(new Error('empty query response.'));
    // 404 not found
    if (metadata.length == 0) return Promise.resolve(null);
    // yikes!
    if (metadata.length != 1) return Promise.reject(new Error('internal error, multiple records have the same uuid.'));
    // eliminate array
    metadata = metadata[0];
    // is it really project metadata?
    // either it is the project record itself or project uuid must be in associationIds
    if (project_uuid != meta_uuid) {
        if (!metadata['associationIds']) return Promise.reject(new Error('metadata record is not associated with project.'));
        if (!metadata['associationIds'].includes(project_uuid)) return Promise.reject(new Error('metadata record is not associated with project.'));
    }
    // it better have a doc id
    if (!metadata['_id']) return Promise.reject(new Error('internal error, metadata return is missing _id'));
    if (!metadata['_id']['$oid']) return Promise.reject(new Error('internal error, metadata return is missing $oid'));

    // only the name, value and associationIds can be updated
    // associationIds has to contain the project uuid
    // update lastUpdated
    metadata['value'] = obj['value'];
    if (obj['name']) metadata['name'] = obj['name'];
    if (project_uuid == meta_uuid) {
        if (obj['associationIds']) metadata['associationIds'] = obj['associationIds'];
    } else {
        if (obj['associationIds']) metadata['associationIds'] = obj['associationIds'];
        if (!metadata['associationIds'].includes(project_uuid)) metadata['associationIds'].push(project_uuid);
    }
    metadata['lastUpdated'] = new Date().toISOString();

    // validate
    if (tapisV3.schema) {
        let s = tapisV3.schema.spec_for_tapis_name(metadata['name']);
        if (!s) return Promise.reject('Cannot find spec with tapis name: ' + metadata['name']);
        let error = s.validate_object(metadata, ['x-vdjserver']);
        if (error) return Promise.reject('Invalid object with tapis name: ' + metadata['name'] + ', error: ' + JSON.stringify(error));
    }

    // finally do the update
    await tapisV3.updateRecord('tapis_meta', metadata['_id']['$oid'], metadata)
        .catch(function(error) { Promise.reject(error); });

    // retrieve again and return
    filter = { "uuid": meta_uuid };
    query = JSON.stringify(filter);
    metadata = await tapisV3.performMultiServiceQuery('tapis_meta', query)
        .catch(function(error) { Promise.reject(error); });

    // yikes!
    if (metadata.length != 1) return Promise.reject(new Error('internal error, after update, multiple records have the same uuid.'));

    return Promise.resolve(metadata[0]);
};

// get metadata with uuid associated with project
// security: it is assumed user has project access
tapisV3.getMetadataForProject = function(project_uuid, meta_uuid) {
    //if (tapisSettings.shouldInjectError("tapisV3.getMetadataForProject")) return tapisSettings.performInjectError();

    var filter = { "uuid": meta_uuid, "associationIds": project_uuid };
    var query = JSON.stringify(filter);
    return tapisV3.performMultiServiceQuery('tapis_meta', query);
};

// delete metadata with uuid associated with project
// security: it is assumed user has project access
tapisV3.deleteMetadataForProject = async function(project_uuid, meta_uuid) {
    //if (tapisSettings.shouldInjectError("tapisV3.deleteMetadataForProject")) return tapisSettings.performInjectError();

    // get the record
    var metadata = await tapisV3.getMetadataForProject(project_uuid, meta_uuid)
        .catch(function(error) { Promise.reject(error); });

    // this shouldn't happen
    if (!metadata) return Promise.reject(new Error('empty query response.'));
    // 404 not found
    if (metadata.length == 0) return Promise.resolve(null);
    // yikes!
    if (metadata.length != 1) return Promise.reject(new Error('internal error, multiple records have the same uuid.'));
    // eliminate array
    metadata = metadata[0];
    // is it really project metadata?
    // either it is the project record itself or project uuid must be in associationIds
    if (project_uuid != meta_uuid) {
        if (!metadata['associationIds']) return Promise.reject(new Error('metadata record is not associated with project.'));
        if (!metadata['associationIds'].includes(project_uuid)) return Promise.reject(new Error('metadata record is not associated with project.'));
    }
    // it better have a doc id
    if (!metadata['_id']) return Promise.reject(new Error('internal error, metadata return is missing _id'));
    if (!metadata['_id']['$oid']) return Promise.reject(new Error('internal error, metadata return is missing $oid'));

    // delete it
    await tapisV3.deleteRecord('tapis_meta', metadata['_id']['$oid'])
        .catch(function(error) { Promise.reject(error); });

    return Promise.resolve(true);
};

// add user permission to project
// security: it is assumed user has project access
tapisV3.addProjectPermissionForUser = async function(project_uuid, username) {
    //if (tapisSettings.shouldInjectError("tapisV3.addProjectPermission")) return tapisSettings.performInjectError();

    // retrieve by uuid
    var filter = { "uuid": project_uuid };
    var query = JSON.stringify(filter);
    var metadata = await tapisV3.performMultiServiceQuery('tapis_meta', query)
        .catch(function(error) { Promise.reject(error); });

    // do some checks

    // this shouldn't happen
    if (!metadata) return Promise.reject(new Error('empty query response.'));
    // 404 not found
    if (metadata.length == 0) return Promise.resolve(null);
    // yikes!
    if (metadata.length != 1) return Promise.reject(new Error('internal error, multiple records have the same uuid.'));
    // eliminate array
    metadata = metadata[0];
    // it better have a doc id
    if (!metadata['_id']) return Promise.reject(new Error('internal error, metadata return is missing _id'));
    if (!metadata['_id']['$oid']) return Promise.reject(new Error('internal error, metadata return is missing $oid'));

    // check that username has not already been added
    var found = false;
    for (let i in metadata['permission']) {
        if (metadata['permission'][i]['username'] == username) found = true;
    }
    if (found) return Promise.resolve(metadata);

    // add and save
    metadata['permission'].push({ "username": username, permission: { read: true, write: true } });
    metadata['lastUpdated'] = new Date().toISOString();

    // validate
    if (tapisV3.schema) {
        let s = tapisV3.schema.spec_for_tapis_name(metadata['name']);
        if (!s) return Promise.reject('Cannot find spec with tapis name: ' + metadata['name']);
        let error = s.validate_object(metadata, ['x-vdjserver']);
        if (error) return Promise.reject('Invalid object with tapis name: ' + metadata['name'] + ', error: ' + JSON.stringify(error));
    }

    // finally do the update
    await tapisV3.updateRecord('tapis_meta', metadata['_id']['$oid'], metadata)
        .catch(function(error) { Promise.reject(error); });

    // retrieve again and return
    filter = { "uuid": project_uuid };
    query = JSON.stringify(filter);
    metadata = await tapisV3.performMultiServiceQuery('tapis_meta', query)
        .catch(function(error) { Promise.reject(error); });

    // yikes!
    if (metadata.length != 1) return Promise.reject(new Error('internal error, after update, multiple records have the same uuid.'));

    return Promise.resolve(metadata[0]);
};

// revoke user permission to project
// security: it is assumed user has project access
tapisV3.removeProjectPermissionForUser = async function(project_uuid, username) {
    //if (tapisSettings.shouldInjectError("tapisV3.removeProjectPermissionForUser")) return tapisSettings.performInjectError();

    // retrieve by uuid
    var filter = { "uuid": project_uuid };
    var query = JSON.stringify(filter);
    var metadata = await tapisV3.performMultiServiceQuery('tapis_meta', query)
        .catch(function(error) { Promise.reject(error); });

    // do some checks

    // this shouldn't happen
    if (!metadata) return Promise.reject(new Error('empty query response.'));
    // 404 not found
    if (metadata.length == 0) return Promise.resolve(null);
    // yikes!
    if (metadata.length != 1) return Promise.reject(new Error('internal error, multiple records have the same uuid.'));
    // eliminate array
    metadata = metadata[0];
    // it better have a doc id
    if (!metadata['_id']) return Promise.reject(new Error('internal error, metadata return is missing _id'));
    if (!metadata['_id']['$oid']) return Promise.reject(new Error('internal error, metadata return is missing $oid'));

    // new permission list without user
    var found = false;
    var new_permissions = [];
    for (let i in metadata['permission']) {
        if (metadata['permission'][i]['username'] == username) found = true;
        else new_permissions.push(metadata['permission'][i]);
    }
    // no need to update if username was not in the list
    if (!found) return Promise.resolve(metadata);

    // save
    metadata['permission'] = new_permissions;
    metadata['lastUpdated'] = new Date().toISOString();

    // validate
    if (tapisV3.schema) {
        let s = tapisV3.schema.spec_for_tapis_name(metadata['name']);
        if (!s) return Promise.reject('Cannot find spec with tapis name: ' + metadata['name']);
        let error = s.validate_object(metadata, ['x-vdjserver']);
        if (error) return Promise.reject('Invalid object with tapis name: ' + metadata['name'] + ', error: ' + JSON.stringify(error));
    }

    // finally do the update
    await tapisV3.updateRecord('tapis_meta', metadata['_id']['$oid'], metadata)
        .catch(function(error) { Promise.reject(error); });

    // retrieve again and return
    filter = { "uuid": project_uuid };
    query = JSON.stringify(filter);
    metadata = await tapisV3.performMultiServiceQuery('tapis_meta', query)
        .catch(function(error) { Promise.reject(error); });

    // yikes!
    if (metadata.length != 1) return Promise.reject(new Error('internal error, after update, multiple records have the same uuid.'));

    return Promise.resolve(metadata[0]);
};

// delete all metadata of given name associated with project
// CAREFUL! Never externally exposed, used internally in limited scenarios
tapisV3.deleteAllProjectMetadataForName = async function(project_uuid, meta_name) {
    //if (tapisSettings.shouldInjectError("tapisV3.deleteAllMetadataForProject")) return tapisSettings.performInjectError();

    if (!project_uuid) Promise.reject(new Error('project_uuid not specified'));
    if (!meta_name) Promise.reject(new Error('meta_name not specified'));

    var metadataList = await tapisV3.queryMetadataForProject(project_uuid, meta_name)
    for (let i = 0; i < metadataList.length; ++i) {
        if (!metadataList[i]['_id']) return Promise.reject(new Error('internal error, metadata return is missing _id'));
        if (!metadataList[i]['_id']['$oid']) return Promise.reject(new Error('internal error, metadata return is missing $oid'));
    }

    for (let i = 0; i < metadataList.length; ++i) {
        await tapisV3.deleteRecord('tapis_meta', metadataList[i]['_id']['$oid'])
            .catch(function(error) { Promise.reject(error); });
    }

    return Promise.resolve(metadataList.length);
};

tapisV3.gatherRepertoireMetadataForProject = async function(projectMetadata, keep_uuids) {
    var context = 'tapisV3.gatherRepertoireMetadataForProject';

    var msg = null;
    var repertoireMetadata = [];
    var subjectMetadata = {};
    var sampleMetadata = {};
    var dpMetadata = {};
    var projectUuid = projectMetadata['uuid'];

    if (!tapisV3.schema) {
        return Promise.reject(new Error('schema is not defined for tapis.'));
    }

    // get repertoire objects
    var models = await tapisV3.queryMetadataForProject(projectUuid, 'repertoire')
        .catch(function(error) { Promise.reject(error); });

    config.log.info(context, 'gathered ' + models.length + ' repertoires.');

    // put into AIRR format
    var study = projectMetadata.value;
    var schema = tapisV3.schema.get_schema('AIRRRepertoire');
    var blank = schema.template();
    //console.log(JSON.stringify(study, null, 2));
    //console.log(JSON.stringify(blank, null, 2));

    if (!keep_uuids) delete study['vdjserver'];

    for (var i in models) {
        var model = models[i].value;
        model['repertoire_id'] = models[i].uuid;
        model['study'] = study;
        repertoireMetadata.push(model);
    }

    // get subject objects
    models = await tapisV3.queryMetadataForProject(projectUuid, 'subject')
        .catch(function(error) { Promise.reject(error); });

    config.log.info(context, 'gathered ' + models.length + ' subjects.');
    for (var i in models) {
        subjectMetadata[models[i].uuid] = models[i].value;
    }

    // get sample processing objects
    models = await tapisV3.queryMetadataForProject(projectUuid, 'sample_processing')
        .catch(function(error) { Promise.reject(error); });

    config.log.info(context, 'gathered ' + models.length + ' sample processings.');
    for (var i in models) {
        sampleMetadata[models[i].uuid] = models[i].value;
    }

    // get data processing objects
    models = await tapisV3.queryMetadataForProject(projectUuid, 'data_processing')
        .catch(function(error) { Promise.reject(error); });

    config.log.info(context, 'gathered ' + models.length + ' data processings.');
     for (var i in models) {
        dpMetadata[models[i].uuid] = models[i].value;
    }

    var dpschema = tapisV3.schema.get_schema('DataProcessing');

    // put into AIRR format
    for (var i in repertoireMetadata) {
        var rep = repertoireMetadata[i];
        var subject = subjectMetadata[rep['subject']['vdjserver_uuid']];
        if (! subject) {
            config.log.info(context, 'cannot collect subject: '
                          + rep['subject']['vdjserver_uuid'] + ' for repertoire: ' + rep['repertoire_id']);
        }
        if (!keep_uuids) delete subject['value']['vdjserver'];
        rep['subject'] = subject;

        var samples = [];
        for (var j in rep['sample']) {
            var sample = sampleMetadata[rep['sample'][j]['vdjserver_uuid']];
            if (! sample) {
                config.log.info(context, 'cannot collect sample: '
                              + rep['sample'][j]['vdjserver_uuid'] + ' for repertoire: ' + rep['repertoire_id']);
            }
            if (!keep_uuids) delete sample['value']['vdjserver'];
            samples.push(sample);
        }
        rep['sample'] = samples;

        var dps = [];
        for (var j in rep['data_processing']) {
            // can be null if no analysis has been done
            if (rep['data_processing'][j]['vdjserver_uuid']) {
                var dp = dpMetadata[rep['data_processing'][j]['vdjserver_uuid']];
                if (! dp) {
                    config.log.info(context, 'cannot collect data_processing: '
                                  + rep['data_processing'][j]['vdjserver_uuid'] + ' for repertoire: ' + rep['repertoire_id']);
                }
                if (!keep_uuids) delete dp['value']['vdjserver'];
                dps.push(dp);
            }
        }
        if (dps.length == 0) {
            rep['data_processing'] = [ dpschema.template() ];
        } else rep['data_processing'] = dps;
    }

    return Promise.resolve(repertoireMetadata);
};

//
/////////////////////////////////////////////////////////////////////
//
// Project File operations
//

tapisV3.createProjectDirectory = function(directory) {

    var postData = { path: '/projects/' + directory };

    return ServiceAccount.getToken()
        .then(function(token) {

            var requestSettings = {
                url: 'https://' + tapisSettings.hostnameV3 + '/v3/files/ops/' + tapisSettings.storageSystem,
                method: 'POST',
                data: JSON.stringify(postData),
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'X-Tapis-Token': ServiceAccount.accessToken()
                }
            };

            //console.log(requestSettings);

            return tapisV3.sendRequest(requestSettings);
        });
};

tapisV3.grantProjectFilePermissions = function(username, project_uuid, filePath) {

    var postData = {
        'username': username,
        'permission': 'MODIFY'
    };

    return ServiceAccount.getToken()
        .then(function(token) {

            var requestSettings = {
                url: 'https://' + tapisSettings.hostnameV3 + '/v3/files/permissions/' + tapisSettings.storageSystem + '//projects/' + project_uuid + '/' + filePath,
                method: 'POST',
                data: JSON.stringify(postData),
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'X-Tapis-Token': ServiceAccount.accessToken()
                }
            };

            return tapisV3.sendRequest(requestSettings);
        });
};

tapisV3.removeProjectFilePermissions = function(username, project_uuid, filePath) {

    return ServiceAccount.getToken()
        .then(function(token) {

            var requestSettings = {
                url: 'https://' + tapisSettings.hostnameV3 + '/v3/files/permissions/' + tapisSettings.storageSystem + '//projects/' + project_uuid + '/' + filePath + '?username=' + username,
                method: 'DELETE',
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'X-Tapis-Token': ServiceAccount.accessToken()
                }
            };

            return tapisV3.sendRequest(requestSettings);
        });
};

tapisV3.getProjectFileDetail = function(relativePath) {

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                url: 'https://' + tapisSettings.hostnameV3 + '/v3/files/ops/' + tapisSettings.storageSystem + '//projects/' + relativePath,
                method: 'GET',
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'X-Tapis-Token': ServiceAccount.accessToken()
                }
            };

            return tapisV3.sendRequest(requestSettings, null)
                .then(function(responseObject) {
                    return Promise.resolve(responseObject.result);
                })
                .catch(function(errorObject) {
                    return Promise.reject(errorObject);
                });
        });
};

tapisV3.getProjectFileMetadataByURL = function(project_uuid, file_url) {

    var filter = { 'value.url': file_url };
    return tapisV3.queryMetadataForProject(project_uuid, 'project_file', filter);
};

tapisV3.createProjectFilePostit = function(project_uuid, obj) {

    var postData = {
    };
    if (obj['allowedUses']) postData['allowedUses'] = obj['allowedUses'];
    if (obj['validSeconds']) postData['validSeconds'] = obj['validSeconds'];

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                url: 'https://' + tapisSettings.hostnameV3 + '/v3/files/postits/' + tapisSettings.storageSystem + '//projects/' + project_uuid + '/' + obj['path'],
                method: 'POST',
                data: JSON.stringify(postData),
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'X-Tapis-Token': ServiceAccount.accessToken()
                }
            };

            return tapisV3.sendRequest(requestSettings);
        });
};

tapisV3.moveProjectFile = function(fromPath, toPath) {

    var postData = {
        operation: "MOVE",
        newPath: toPath
    };

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                url: 'https://' + tapisSettings.hostnameV3 + '/v3/files/ops/' + tapisSettings.storageSystem + fromPath,
                method: 'PUT',
                data: JSON.stringify(postData),
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'X-Tapis-Token': ServiceAccount.accessToken()
                }
            };

            return tapisV3.sendRequest(requestSettings, null)
        });
};

tapisV3.uploadFileToProjectTempDirectory = function(projectUuid, filename, filedata) {

    // filedata should be data stored in a Blob()
    var form = new FormData();
    form.append('file', filedata);

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                url: 'https://' + tapisSettings.hostnameV3 + '/v3/files/ops/' + tapisSettings.storageSystem + '/projects/' + projectUuid + '/deleted/' + filename,
                method: 'POST',
                data: form,
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'multipart/form-data',
                    'X-Tapis-Token': ServiceAccount.accessToken()
                }
            };

            return tapisV3.sendRequest(requestSettings)
        });
};

tapisV3.getProjectFileContents = function(projectUuid, fileName) {

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                url: 'https://' + tapisSettings.hostnameV3 + '/v3/files/content/' + tapisSettings.storageSystem + '/projects/' + projectUuid + '/files/' + fileName,
                method: 'GET',
                headers: {
                    'X-Tapis-Token': ServiceAccount.accessToken()
                }
            };

            return tapisV3.sendRequest(requestSettings);
        });
};

//
/////////////////////////////////////////////////////////////////////
//
// User operations
//

tapisV3.getUserVerificationMetadata = function(username) {
    //if (tapisSettings.shouldInjectError("tapisV3.getUserVerificationMetadata")) return tapisSettings.performInjectError();

    var filter = { "name": "userVerification", "value.username": username, "owner": ServiceAccount.username };
    var query = JSON.stringify(filter);
    return tapisV3.performServiceQuery('tapis_meta', query);
};

tapisV3.getUserProfile = function(username) {
    //if (tapisSettings.shouldInjectError("tapisV3.getUserProfile")) return tapisSettings.performInjectError();

    var filter = { "name": "profile", "owner": username };
    var query = JSON.stringify(filter);
    return tapisV3.performServiceQuery('tapis_meta', query);
};


tapisV3.getTapisUserProfile = function(accessToken, username) {
    //if (tapisSettings.shouldInjectError("tapisIO.getTapisUserProfile")) return tapisSettings.performInjectError();

    if (username == 'me') {
        let requestSettings = {
            url: 'https://' + tapisSettings.hostnameV3 + '/v3/oauth2/userinfo',
            method: 'GET',
            rejectUnauthorized: false,
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'X-Tapis-Token': accessToken
            }
        };

        //console.log(requestSettings);
        return tapisV3.sendRequest(requestSettings);

    } else {
        let requestSettings = {
            url: 'https://' + tapisSettings.hostnameV3 + '/v3/oauth2/profiles/' + username,
            method: 'GET',
            rejectUnauthorized: false,
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'X-Tapis-Token': accessToken
            }
        };

        //console.log(requestSettings);
        return tapisV3.sendRequest(requestSettings);
    }
};

tapisV3.createFeedbackMetadata = async function(feedback, username, email) {

    var valueData = {
        feedbackMessage: feedback,
    };

    if (username.length > 0) {
        valueData.username = username;
    }

    if (email.length > 0) {
        valueData.email = email;
    }

    var date = new Date().toISOString();
    var uuid = uuidv4();
    var postData = {
        uuid: uuid,
        associationIds: [],
        owner: ServiceAccount.username,
        created: date,
        lastUpdated: date,
        name: 'feedback',
        value: valueData
    };

    // validate
    if (tapisV3.schema) {
        let s = tapisV3.schema.spec_for_tapis_name('feedback');
        if (!s) return Promise.reject('Cannot find spec with tapis name: feedback');
        let error = s.validate_object(postData, ['x-vdjserver']);
        if (error) return Promise.reject('Invalid object with tapis name: feedback, error: ' + JSON.stringify(error));
    }

    return tapisV3.createRecord('tapis_meta', postData)
        .then(function(data) {
            //console.log(JSON.stringify(data));
            var filter = { "uuid": uuid };
            var query = JSON.stringify(filter);
            return tapisV3.performServiceQuery('tapis_meta', query);
        })
        .then(function(data) {
            //console.log(JSON.stringify(data));
            return Promise.resolve(data[0]);
        });
};

//
/////////////////////////////////////////////////////////////////////
//
// Project load/unload from VDJServer ADC data repository
//

//
// Right now, all the project load/unload metadata is owned by the
// vdj account, no permissions for project users are given.
//

tapisV3.createProjectLoadMetadata = function(projectUuid, collection) {

    var meta_name = 'adc_project_load';
    if (tapisV3.schema) {
        let s = tapisV3.schema.spec_for_tapis_name(meta_name);
        if (!s) return Promise.reject('Cannot find spec with tapis name: ' + meta_name);

        let obj = s.template();
        obj['value']['projectUuid'] = projectUuid;
        obj['value']['collection'] = collection;
        return tapisV3.createDocument(meta_name, obj['value']);
    } else {
        return Promise.reject('Schema is not defined for Tapis V3.')
    }
};

// there should be only a single metadata record
tapisV3.getProjectLoadMetadata = function(projectUuid, collection) {

    var filter = {
        "name": "adc_project_load",
        "owner": ServiceAccount.username,
        "value.projectUuid": projectUuid,
        "value.collection": collection
    };
    var query = JSON.stringify(filter);
    return tapisV3.performServiceQuery('tapis_meta', query);
};

// query project load records
tapisV3.queryProjectLoadMetadata = function(projectUuid, collection, shouldLoad, isLoaded, repertoireMetadataLoaded, rearrangementDataLoaded) {

    var filter = {
        "name": "adc_project_load",
        "owner": ServiceAccount.username
    };
    if (projectUuid) filter['value.projectUuid'] = projectUuid;
    if (collection) filter['value.collection'] = collection;
    if (shouldLoad === false) filter["value.shouldLoad"] = false;
    else if (shouldLoad === true) filter["value.shouldLoad"] = true;
    if (isLoaded === false) filter["value.isLoaded"] = false;
    else if (isLoaded === true) filter["value.isLoaded"] = true;
    if (repertoireMetadataLoaded === false) filter["value.repertoireMetadataLoaded"] = false;
    else if (repertoireMetadataLoaded === true) filter["value.repertoireMetadataLoaded"] = true;
    if (rearrangementDataLoaded === false) filter["value.rearrangementDataLoaded"] = false;
    else if (rearrangementDataLoaded === true) filter["value.rearrangementDataLoaded"] = true;
    
    var query = JSON.stringify(filter);
    return tapisV3.performMultiServiceQuery('tapis_meta', query);
};

// get list of projects to be loaded
tapisV3.getProjectsToBeLoaded = function(collection) {

    var filter = {
        "name": "adc_project_load",
        "owner": ServiceAccount.username,
        "value.collection": collection,
        "value.shouldLoad": true,
        "value.isLoaded": false
    };
    var query = JSON.stringify(filter);
    return tapisV3.performMultiServiceQuery('tapis_meta', query);
};

// status record for a rearrangement load
tapisV3.createRearrangementLoadMetadata = function(projectUuid, repertoire_id, collection) {

    var meta_name = 'adc_rearrangement_load';
    if (tapisV3.schema) {
        let s = tapisV3.schema.spec_for_tapis_name(meta_name);
        if (!s) return Promise.reject('Cannot find spec with tapis name: ' + meta_name);

        let obj = s.template();
        obj['value']['projectUuid'] = projectUuid;
        obj['value']['repertoire_id'] = repertoire_id;
        obj['value']['collection'] = collection;
        obj['value']['load_set'] = 0;
        return tapisV3.createDocument(meta_name, obj['value']);
    } else {
        return Promise.reject('Schema is not defined for Tapis V3.')
    }
};

// get list of repertoires that need their rearrangement data to be loaded
tapisV3.getRearrangementsToBeLoaded = function(projectUuid, collection) {

    var filter = {
        "name": "adc_rearrangement_load",
        "owner": ServiceAccount.username,
        "value.projectUuid": projectUuid,
        "value.collection": collection
    };
    var query = JSON.stringify(filter);
    return tapisV3.performMultiServiceQuery('tapis_meta', query);
};

//
/////////////////////////////////////////////////////////////////////
//
// AIRR Data Commons functions
//

// facets aggregation query
tapisV3.performFacets = function(collection, query, field, start_page, pagesize) {
    var context = 'tapisV3.performFacets';
    var models = [];

    //console.log(query);
    var doAggr = function(page) {
        var aggrFunction = tapisV3.performAggregation;
        if (query && query.length > tapisSettings.large_query_size) {
            tapisSettings.config.log.info(context, 'Large facets query detected.');
            aggrFunction = tapisV3.performLargeAggregation;
        }
        // TAPIS BUG: with pagesize and normal aggregation so use the large one for now
        //aggrFunction = tapisV3.performLargeAggregation;
        return aggrFunction(collection, 'facets', query, field, null, null)
            .then(function(records) {
                tapisSettings.config.log.info(context, 'query returned ' + records.length + ' records.');
                if (records.length == 0) {
                    return Promise.resolve(models);
                } else {
                    // the new facets aggregation returns a single record with all the data
                    return Promise.resolve(records[0]['facets']);
                }
            })
            .catch(function(errorObject) {
                return Promise.reject(errorObject);
            });
    };
    
    return doAggr(start_page);
}

// the global/system list of ADC repositories
// this should be a singleton metadata entry owned by service account
tapisV3.getSystemADCRepositories = function() {

    var filter = { "name": "adc_system_repositories", "owner": ServiceAccount.username };
    var query = JSON.stringify(filter);
    return tapisV3.performServiceQuery('tapis_meta', query);
}

// ADC download cache status
// this should be a singleton metadata entry owned by service account
tapisV3.createADCDownloadCache = function() {
    //if (tapisSettings.shouldInjectError("tapisIO.createADCDownloadCache")) return tapisSettings.performInjectError();

    return Promise.reject('tapisV3.createADCDownloadCache: Not implemented.')

/*    var postData = {
        name: 'adc_cache',
        value: {
            enable_cache: false
        }
    };

    postData = JSON.stringify(postData);

    return tapisV3.createRecord('tapis_meta', postData); */
};

tapisV3.getADCDownloadCache = function() {
    //if (tapisSettings.shouldInjectError("tapisIO.getADCDownloadCache")) return tapisSettings.performInjectError();

    var filter = { "name": "adc_cache", "owner": ServiceAccount.username };
    var query = JSON.stringify(filter);
    return tapisV3.performServiceQuery('tapis_meta', query);
}

// create metadata entry for cached ADC study
tapisV3.createCachedStudyMetadata = function(repository_id, study_id, should_cache) {

    return Promise.reject('tapisV3.createCachedStudyMetadata: Not implemented.')
/*
    var postData = {
        name: 'adc_cache_study',
        value: {
            repository_id: repository_id,
            study_id: study_id,
            should_cache: should_cache,
            is_cached: false,
            archive_file: null,
            download_url: null
        }
    };

    postData = JSON.stringify(postData);

    return tapisV3.createRecord('tapis_meta', postData); */

};

// get list of studies cache entries
tapisV3.getStudyCacheEntries = function(repository_id, study_id, should_cache, is_cached) {

    var query = '{"name":"adc_cache_study"';
    if (repository_id) query += ',"value.repository_id":"' + repository_id + '"';
    if (study_id) query += ',"value.study_id":"' + study_id + '"';
    if (should_cache === false) query += ',"value.should_cache":false';
    else if (should_cache === true) query += ',"value.should_cache":true';
    if (is_cached === false) query += ',"value.is_cached":false';
    else if (is_cached === true) query += ',"value.is_cached":true';
    query += '}';

    return tapisV3.performMultiServiceQuery('tapis_meta', query);
};

// create metadata entry for cached ADC rearrangements for a single repertoire
tapisV3.createCachedRepertoireMetadata = function(repository_id, study_id, repertoire_id, should_cache) {

    return Promise.reject('tapisV3.createCachedRepertoireMetadata: Not implemented.')
/*
    var postData = {
        name: 'adc_cache_repertoire',
        value: {
            repository_id: repository_id,
            study_id: study_id,
            repertoire_id: repertoire_id,
            should_cache: should_cache,
            is_cached: false,
            archive_file: null,
            download_url: null
        }
    };

    postData = JSON.stringify(postData);

    return tapisV3.createRecord('tapis_meta', postData); */
};

// get list of repertoire cache entries
tapisV3.getRepertoireCacheEntries = function(repository_id, study_id, repertoire_id, should_cache, not_cached, max_limit) {

    var query = '{"name":"adc_cache_repertoire"';
    if (repository_id) query += ',"value.repository_id":"' + repository_id + '"';
    if (study_id) query += ',"value.study_id":"' + study_id + '"';
    if (repertoire_id) query += ',"value.repertoire_id":"' + repertoire_id + '"';
    if (should_cache) query += ',"value.should_cache":true';
    if (not_cached) query += ',"value.is_cached":false';
    query += '}';

    var limit = 50;
    if (max_limit) {
        if (max_limit < limit) limit = max_limit;
        if (max_limit < 1) return Promise.resolve([]);
    }

    // TODO: implement limit

    return tapisV3.performMultiServiceQuery('tapis_meta', query);
};

tapisV3.createADCDownloadCachePostit = function(cache_uuid, obj) {

    var postData = {
    };
    if (obj['allowedUses']) postData['allowedUses'] = obj['allowedUses'];
    if (obj['validSeconds']) postData['validSeconds'] = obj['validSeconds'];

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                url: 'https://' + tapisSettings.hostnameV3 + '/v3/files/postits/' + tapisSettings.storageSystem + '//community/cache/' + cache_uuid + '/' + obj['path'],
                method: 'POST',
                data: JSON.stringify(postData),
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'X-Tapis-Token': ServiceAccount.accessToken()
                }
            };

            return tapisV3.sendRequest(requestSettings);
        });
};

//
/////////////////////////////////////////////////////////////////////
//
// ADC Async functions
//

// get async query status
tapisV3.getAsyncQueryStatus = function(status_uuid) {
    //if (tapisSettings.shouldInjectError("tapisV3.getMetadataForProject")) return tapisSettings.performInjectError();

    var filter = { "uuid": status_uuid, "name": "async_query" };
    var query = JSON.stringify(filter);
    return tapisV3.performMultiServiceQuery('tapis_meta', query);
};

//
/////////////////////////////////////////////////////////////////////
//
// Statistics cache functions
//

// Statistics cache status
// this should be a singleton metadata entry owned by service account
tapisV3.createStatisticsCache = function() {
    //if (tapisSettings.shouldInjectError("tapisV3.createStatisticsCache")) return tapisSettings.performInjectError();

    var meta_name = 'statistics_cache';
    if (tapisV3.schema) {
        let s = tapisV3.schema.spec_for_tapis_name(meta_name);
        if (!s) return Promise.reject('Cannot find spec with tapis name: ' + meta_name);

        let obj = s.template();
        return tapisV3.createDocument(meta_name, obj['value']);
    } else {
        return Promise.reject('Schema is not defined for Tapis V3.')
    }
};

tapisV3.getStatisticsCache = function() {
    //if (tapisSettings.shouldInjectError("tapisV3.getStatisticsCache")) return tapisSettings.performInjectError();

    var filter = { "name": "statistics_cache", "owner": ServiceAccount.username };
    var query = JSON.stringify(filter);
    return tapisV3.performServiceQuery('tapis_meta', query);
}

/*
// create metadata entry for statistics cache for study
tapisIO.createStatisticsCacheStudyMetadata = function(repository_id, study_id, download_cache_id, should_cache) {

    var postData = {
        name: 'statistics_cache_study',
        value: {
            repository_id: repository_id,
            study_id: study_id,
            download_cache_id: download_cache_id,
            should_cache: should_cache,
            is_cached: false
        }
    };

    postData = JSON.stringify(postData);

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'POST',
                path:     '/meta/v2/data',
                rejectUnauthorized: false,
                headers: {
                    'Content-Type':   'application/json',
                    'Content-Length': Buffer.byteLength(postData),
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                }
            };

            return tapisIO.sendRequest(requestSettings, postData);
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

// get list of statistics cache entries for study
tapisIO.getStatisticsCacheStudyMetadata = function(repository_id, study_id, should_cache, is_cached) {

    var models = [];

    var query = '{"name":"statistics_cache_study"';
    if (repository_id) query += ',"value.repository_id":"' + repository_id + '"';
    if (study_id) query += ',"value.study_id":"' + study_id + '"';
    if (should_cache === false) query += ',"value.should_cache":false';
    else if (should_cache === true) query += ',"value.should_cache":true';
    if (is_cached === false) query += ',"value.is_cached":false';
    else if (is_cached === true) query += ',"value.is_cached":true';
    query += '}';

    var doFetch = function(offset) {
        return ServiceAccount.getToken()
            .then(function(token) {
                var requestSettings = {
                    host:     tapisSettings.hostname,
                    method:   'GET',
                    path:     '/meta/v2/data?q='
                        + encodeURIComponent(query)
                        + '&limit=50&offset=' + offset,
                    rejectUnauthorized: false,
                    headers: {
                        'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                    }
                };

                //console.log(requestSettings);

                return tapisIO.sendRequest(requestSettings, null)
            })
            .then(function(responseObject) {
                var result = responseObject.result;
                if (result.length > 0) {
                    // maybe more data
                    models = models.concat(result);
                    var newOffset = offset + result.length;
                    return doFetch(newOffset);
                } else {
                    // no more data
                    return Promise.resolve(models);
                }
            })
            .catch(function(errorObject) {
                return Promise.reject(errorObject);
            });
    }

    return doFetch(0);
};

// create metadata entry for statistics cache for a single repertoire
tapisIO.createStatisticsCacheRepertoireMetadata = function(repository_id, study_id, repertoire_id, download_cache_id, should_cache) {

    var postData = {
        name: 'statistics_cache_repertoire',
        value: {
            repository_id: repository_id,
            study_id: study_id,
            repertoire_id: repertoire_id,
            download_cache_id: download_cache_id,
            should_cache: should_cache,
            is_cached: false,
            statistics_job_id: null
        }
    };

    postData = JSON.stringify(postData);

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'POST',
                path:     '/meta/v2/data',
                rejectUnauthorized: false,
                headers: {
                    'Content-Type':   'application/json',
                    'Content-Length': Buffer.byteLength(postData),
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                }
            };

            return tapisIO.sendRequest(requestSettings, postData);
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

// get list of repertoire cache entries
tapisIO.getStatisticsCacheRepertoireMetadata = function(repository_id, study_id, repertoire_id, should_cache, is_cached, max_limit) {

    var models = [];

    var query = '{"name":"statistics_cache_repertoire"';
    if (repository_id) query += ',"value.repository_id":"' + repository_id + '"';
    if (study_id) query += ',"value.study_id":"' + study_id + '"';
    if (repertoire_id) query += ',"value.repertoire_id":"' + repertoire_id + '"';
    if (should_cache === false) query += ',"value.should_cache":false';
    else if (should_cache === true) query += ',"value.should_cache":true';
    if (is_cached === false) query += ',"value.is_cached":false';
    else if (is_cached === true) query += ',"value.is_cached":true';
    query += '}';

    var limit = 50;
    if (max_limit) {
        if (max_limit < limit) limit = max_limit;
        if (max_limit < 1) return Promise.resolve([]);
    }

    var doFetch = function(offset) {
        return ServiceAccount.getToken()
            .then(function(token) {
                var requestSettings = {
                    host:     tapisSettings.hostname,
                    method:   'GET',
                    path:     '/meta/v2/data?q='
                        + encodeURIComponent(query)
                        + '&limit=' + limit + '&offset=' + offset,
                    rejectUnauthorized: false,
                    headers: {
                        'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                    }
                };

                return tapisIO.sendRequest(requestSettings, null)
            })
            .then(function(responseObject) {
                var result = responseObject.result;
                if (result.length > 0) {
                    // maybe more data
                    models = models.concat(result);
                    if ((max_limit) && (models.length >= max_limit))
                        return Promise.resolve(models);
                    var newOffset = offset + result.length;
                    return doFetch(newOffset);
                } else {
                    // no more data
                    return Promise.resolve(models);
                }
            })
            .catch(function(errorObject) {
                return Promise.reject(errorObject);
            });
    }

    return doFetch(0);
};

// get statistics from the database
tapisIO.getStatistics = function(collection, repertoire_id, data_processing_id) {

    if (! repertoire_id) return Promise.reject(new Error("repertoire_id cannot be null"));
    var filter = { "repertoire.repertoire_id":repertoire_id };
    if (data_processing_id) filter['repertoire.data_processing_id'] = data_processing_id;

    return tapisIO.performQuery(collection, JSON.stringify(filter));
};

// delete statistics in the database
tapisIO.deleteStatistics = async function(collection, repertoire_id, data_processing_id) {

    var stats_records = await tapisIO.getStatistics(collection, repertoire_id, data_processing_id)
        .catch(function(error) {
            return Promise.reject(error);
        });

    for (let i in stats_records) {
        await tapisIO.deleteDocument(collection, stats_records[i]['_id']['$oid'])
            .catch(function(error) {
                return Promise.reject(error);
            });
    }

    return Promise.resolve();
};

// record statistics in the database
tapisIO.recordStatistics = function(collection, data) {

    // first delete any entries then insert
    return tapisIO.deleteStatistics(collection, data['repertoire']['repertoire_id'], data['repertoire']['data_processing_id'])
        .then(function() {
            return ServiceAccount.getToken();
        })
        .then(function(token) {

            var postData = JSON.stringify(data);

            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'POST',
                path:     '/meta/v3/' + tapisSettings.mongo_dbname + '/' + collection,
                rejectUnauthorized: false,
                headers: {
                    'Accept':   'application/json',
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken(),
                    'Content-Type': 'application/json',
                    'Content-Length': Buffer.byteLength(postData)
                }
            };

            //console.log(requestSettings);

            return tapisIO.sendRequest(requestSettings, postData);
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject);
        })
        .catch(function(errorObject) {
            console.error(errorObject);
            return Promise.reject(errorObject);
        });
};
*/

/* TESTING
tapisV3.getRepertoireCacheEntries().then(function(data) {
    console.log(data);
    console.log(data.length);
});
*/
