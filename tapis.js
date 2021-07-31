'use strict';

//
// tapis.js
// Wrapper functions for accessing the Tapis APIs
//
// VDJServer
// http://vdjserver.org
//
// Copyright (C) 2020 The University of Texas Southwestern Medical Center
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

var tapisIO  = {};
module.exports = tapisIO;

// Node Libraries
//var Q = require('q');
var _ = require('underscore');
var jsonApprover = require('json-approver');
var FormData = require('form-data');

// Settings
var tapisSettings = require('./tapisSettings');
tapisIO.tapisSettings = tapisSettings;

// Models
var ServiceAccount = require('./serviceAccount');
tapisIO.serviceAccount = ServiceAccount;

//
// Generic send request
//
tapisIO.sendRequest = function(requestSettings, postData) {

    return new Promise(function(resolve, reject) {
        var request = require('https').request(requestSettings, function(response) {

            var output = '';

            response.on('data', function(chunk) {
                output += chunk;
            });

            response.on('end', function() {

                var responseObject;

                if ((response.statusCode >= 400) && (response.statusCode != 404)) {
                    reject(new Error('Request error: ' + output));
                } else if (output.length == 0) {
                    resolve(null);
                } else if (output && jsonApprover.isJSON(output)) {
                    responseObject = JSON.parse(output);
                    resolve(responseObject);
                } else {
                    console.error('TAPIS-API ERROR: Tapis response is not json: ' + output);
                    reject(new Error('Tapis response is not json: ' + output));
                }
            });
        });

        request.on('error', function(error) {
            console.error('TAPIS-API ERROR: Tapis connection error:' + JSON.stringify(error));
            reject(new Error('Tapis connection error:' + JSON.stringify(error)));
        });

        if (postData) {
            // Request body parameters
            request.write(postData);
        }

        request.end();
    });
};

tapisIO.sendTokenRequest = function(requestSettings, postData) {

    return new Promise(function(resolve, reject) {
        var request = require('https').request(requestSettings, function(response) {

            var output = '';

            response.on('data', function(chunk) {
                output += chunk;
            });

            response.on('end', function() {

                var responseObject;

                if (output && jsonApprover.isJSON(output)) {
                    responseObject = JSON.parse(output);
                } else {
                    console.error('TAPIS-API ERROR: Tapis response is not json: ' + output);
                    reject(new Error('Tapis response is not json: ' + output));
                }

                if (responseObject
                    && responseObject.access_token
                    && responseObject.refresh_token
                    && responseObject.token_type
                    && responseObject.expires_in)
                {
                    resolve(responseObject);
                } else {
                    reject(new Error('Tapis response returned an error: ' + output));
                }
            });
        });

        request.on('error', function(error) {
            console.error('TAPIS-API ERROR: Tapis connection error:' + JSON.stringify(error));
            reject(new Error('Tapis connection error:' + JSON.stringify(error)));
        });

        if (postData) {
            // Request body parameters
            request.write(postData);
        }

        request.end();
    });
};


// Fetches a user token based on the supplied auth object
// and returns the auth object with token data on success
tapisIO.getToken = function(auth) {

    var postData = 'grant_type=password&scope=PRODUCTION&username=' + auth.username + '&password=' + auth.password;

    var requestSettings = {
        host:     tapisSettings.hostname,
        method:   'POST',
        auth:     tapisSettings.clientKey + ':' + tapisSettings.clientSecret,
        path:     '/token',
        rejectUnauthorized: false,
        headers: {
            'Content-Type':   'application/x-www-form-urlencoded',
            'Content-Length': Buffer.byteLength(postData)
        }
    };

    return tapisIO.sendTokenRequest(requestSettings, postData);
};

// Refreshes a token and returns it on success
tapisIO.refreshToken = function(auth) {
    if (config.shouldInjectError("tapisIO.refreshToken")) return config.performInjectError();

    var postData = 'grant_type=refresh_token&scope=PRODUCTION&refresh_token=' + auth.refresh_token;

    var requestSettings = {
        host:     tapisSettings.hostname,
        method:   'POST',
        auth:     tapisSettings.clientKey + ':' + tapisSettings.clientSecret,
        path:     '/token',
        rejectUnauthorized: false,
        headers: {
            'Content-Type':   'application/x-www-form-urlencoded',
            'Content-Length': Buffer.byteLength(postData)
        }
    };

    return tapisIO.sendTokenRequest(requestSettings, postData);
};
