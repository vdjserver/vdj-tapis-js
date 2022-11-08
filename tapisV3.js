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
// Tapis V3 meta operations
// Primarily in support of VDJServer ADC API
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
