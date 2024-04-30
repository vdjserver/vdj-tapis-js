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

// Settings
var tapisSettings = require('./tapisSettings');
tapisV3.tapisSettings = tapisSettings;

// Models
//var ServiceAccount = require('./serviceAccount');
//tapisIO.serviceAccount = ServiceAccount;
var ServiceAccount = require('./serviceAccountV3');
tapisV3.serviceAccount = ServiceAccount;
var GuestAccount = require('./guestAccountV3');
tapisV3.guestAccount = GuestAccount;

// Controller
//var authController = require('./authController');
//tapisIO.authController = authController;

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
    // TODO: error if no metadata
    // TODO: if no uuid then assign one
    // TODO: error if no name
    // TODO: error if no value
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

            console.log(requestSettings);

            return tapisV3.sendRequest(requestSettings);
        });
};

// query using service account restricted by user permissions
tapisV3.performUserQuery = function(username, collection, query, projection, page, pagesize, count) {

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

            // TODO: restrict by user

            var requestSettings = {
                url: url,
                method: 'GET',
                headers: {
                    'Accept': 'application/json',
                    'X-Tapis-Token': ServiceAccount.accessToken()
                }
            };

            console.log(requestSettings);

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

tapisV3.performMultiUserQuery = function(username, collection, query, projection, start_page, pagesize) {
    var models = [];
    if (!start_page) start_page = 1;

    //console.log(query);
    var doQuery = function(page) {
        var queryFunction = tapisV3.performUserQuery;
        // TODO: do we large query?
        //if (query && query.length > tapisSettings.large_query_size) queryFunction = tapisV3.performLargeQuery;
        return queryFunction(username, collection, query, projection, page, pagesize)
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
tapisV3.queryProjectMetadata = function(username, project_uuid, meta_name) {
    //if (tapisSettings.shouldInjectError("tapisV3.getProjectMetadata")) return tapisSettings.performInjectError();

    var filter = { "name": meta_name, "associationIds": project_uuid };
    var query = JSON.stringify(filter);
    return tapisV3.performMultiUserQuery(username, 'tapis_meta', query);
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
        var requestSettings = {
            url: 'https://' + tapisSettings.hostnameV3 + '/v3/oauth2/userinfo',
            method: 'GET',
            rejectUnauthorized: false,
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'X-Tapis-Token': accessToken
            }
        };

        console.log(requestSettings);
        return tapisV3.sendRequest(requestSettings);

    } else {
        var requestSettings = {
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
