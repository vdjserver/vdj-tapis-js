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

// Models
var ServiceAccount = require('./serviceAccountV3');
tapisV3.serviceAccount = ServiceAccount;
var GuestAccount = require('./guestAccountV3');
tapisV3.guestAccount = GuestAccount;

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
            msg = 'Tapis request failed with error: ' + JSON.stringify(error.response.data);
            return Promise.reject(new Error(msg));
        });

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
tapisV3.createRecord = function(collection, data) {
    //if (tapisSettings.shouldInjectError("tapisIO.createMetadataForType")) return tapisSettings.performInjectError();

    // TODO: error if no collection
    // TODO: error if no data
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

            //console.log(requestSettings);

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
                data: JSON.stringify(postData),
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'X-Tapis-Token': GuestAccount.accessToken()
                }
            };

            //console.log(requestSettings);

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

    //postData = JSON.stringify(postData);

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

//
/////////////////////////////////////////////////////////////////////
//
// AIRR Data Commons functions
//

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

/* TESTING
tapisV3.getRepertoireCacheEntries().then(function(data) {
    console.log(data);
    console.log(data.length);
});
*/
