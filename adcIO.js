
'use strict';

//
// adcIO.js
// Functions for the AIRR Data Commons
//
// VDJServer Analysis Portal
// VDJ API Service
// https://vdjserver.org
//
// Copyright (C) 2021 The University of Texas Southwestern Medical Center
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

var adcIO  = {};
module.exports = adcIO;

// Server environment config
//var config = require('../config/config');

// Tapis
var tapisSettings = require('vdj-tapis-js/tapisSettings');
var tapisIO = tapisSettings.get_default_tapis();
var config = tapisSettings.config;

// Processing
var webhookIO = require('./webhookIO');

// Node Libraries
var _ = require('underscore');
var csv = require('csv-parser');
var fs = require('fs');
const zlib = require('zlib');
const axios = require('axios');


// check if valid JSON
var isJSON = function(data) {

    var isValid = false;

    try {
        var parsedData = JSON.parse(data);

        if (parsedData && typeof parsedData === 'object' && parsedData !== null) {
            isValid = true;
        }
    }
    catch (e) { isValid = false; }

    return isValid;
}

//
// Generic send request
//
adcIO.sendRequest = function(requestSettings, postData) {

    return new Promise(function(resolve, reject) {
        var request = require('https').request(requestSettings, function(response) {

            var output = '';

            response.on('data', function(chunk) {
                output += chunk;
            });

            response.on('end', function() {

                var responseObject;
                //console.log(output);

                if (output && isJSON(output)) {
                    responseObject = JSON.parse(output);
                }
                else {
                    reject(new Error('ADC repository response is not json. Raw output: ' + output));
                }

                //console.log(responseObject);
                if (responseObject) {
                    resolve(responseObject);
                }
                else {
                    reject(new Error('ADC repository response is empty: ' + JSON.stringify(responseObject)));
                }

            });
        });

        request.on('error', function(error) {
            reject(new Error('ADC repository connection error, ' + error));
        });

        if (postData) {
            // Request body parameters
            request.write(postData);
        }

        request.end();
    });
};

// Get the set of default ADC repositories
// TODO: is this the same as system set?
adcIO.defaultADCRepositories = function() {
}

// Get status of an ADC ASYNC query
adcIO.asyncQueryStatus = async function(repository, query_id) {
    var msg = null;

    // we assume the passed in repository is an object entry
    if (! repository) return Promise.reject('missing repository parameter');
    if (! repository['async_host']) return Promise.reject('repository entry missing async_host');
    if (! repository['async_base_url']) return Promise.reject('repository entry missing async_base_url');
    if (! query_id) return Promise.reject('missing query_id parameter');

    var requestSettings = {
        host:     repository['async_host'],
        method:   'GET',
        path:     repository['async_base_url'] + '/status/' + query_id,
        rejectUnauthorized: false,
        headers: {
            'Content-Type':   'application/json'
        }
    };
    console.log(requestSettings);

    var data = await adcIO.sendRequest(requestSettings, null)
        .catch(function(error) {
            msg = 'VDJ-API ERROR: adcIO.asyncQueryStatus, adcIO.sendRequest error ' + error;
        });
    if (msg) {
        console.error(msg);
        webhookIO.postToSlack(msg);
        return Promise.reject(new Error(msg));
    }

    return Promise.resolve(data);
}

// Query rearrangements from an ADC repository with ASYNC API
adcIO.asyncGetRearrangements = async function(repository, repertoire_id, notification) {
    var msg = null;

    // we assume the passed in repository is an object entry
    if (! repository) return Promise.reject('missing repository entry');
    if (! repertoire_id) return Promise.reject('missing repertoire_id entry');
    if (! repository['async_host']) return Promise.reject('repository entry missing async_host');
    if (! repository['async_base_url']) return Promise.reject('repository entry missing async_base_url');

    // query rearrangements for a repertoire
    var postData = {
      "filters": {
        "op": "=",
        "content": {
          "field": "repertoire_id",
          "value": repertoire_id
        }
      },
      "format":"tsv"
    };
    if (notification) postData["notification"] = notification;

    postData = JSON.stringify(postData);

    var requestSettings = {
        host:     repository['async_host'],
        method:   'POST',
        path:     repository['async_base_url'] + '/rearrangement',
        rejectUnauthorized: false,
        headers: {
            'Content-Type':   'application/json',
            'Content-Length': Buffer.byteLength(postData)
        }
    };
    console.log(requestSettings);

    var data = await adcIO.sendRequest(requestSettings, postData)
        .catch(function(error) {
            msg = 'VDJ-API ERROR: adcIO.asyncGetRearrangements, adcIO.sendRequest error ' + error;
        });
    if (msg) {
        console.error(msg);
        webhookIO.postToSlack(msg);
        return Promise.reject(new Error(msg));
    }

    return Promise.resolve(data);
}

// Query rearrangements from an ADC repository with standard synchronous ADC API and write response to file
// This assumes iReceptor turnkey behavior which dumps the whole database
// This assumes direct access to Corral for writing the output file
adcIO.downloadRearrangements = async function(repository, repertoire_id, filepath) {
    var msg = null;
    var context = 'adcIO.downloadRearrangements';

    // we assume the passed in repository is an object entry
    if (! repository) return Promise.reject('missing repository entry');
    if (! repertoire_id) return Promise.reject('missing repertoire_id entry');

    // download progress
    var progress_size = 10000000;
    var progress_count = 0;
    var axiosProgressFunction = function(axiosProgressEvent) {
        var progress_limit = progress_size * progress_count;
        if (axiosProgressEvent.loaded > progress_limit) {
            progress_count += 1;
            config.log.info(context, 'Downloaded ' + axiosProgressEvent.loaded + ' bytes of data so far.');
        }
    };

    // query rearrangements for a repertoire
    var postData = {
      "filters": {
        "op": "=",
        "content": {
          "field": "repertoire_id",
          "value": repertoire_id
        }
      },
      "format":"tsv"
    };
    postData = JSON.stringify(postData);

    var url = 'https://' + repository['server_host'] + repository['base_url'] + '/rearrangement';
    var requestSettings = {
        url: url,
        method: 'POST',
        data: postData,
        headers: {
            'Content-Type':   'application/json',
        },

        // axios settings for streaming
        responseType: 'stream',
        maxRedirects: 0, // avoid buffering the entire stream
        onDownloadProgress: axiosProgressFunction
    };
    console.log(requestSettings);

    // we do our own request so we can stream
    config.log.info(context, 'Requesting download to file: ' + filepath + ' for repository: ' + repository['repository_id'] + ' for repertoire_id: ' + repertoire_id);
    const writer = fs.createWriteStream(filepath);
    return axios(requestSettings)
        .then(function (response) {
            return new Promise((resolve, reject) => {
              response.data.pipe(writer);
              let error = null;
              writer.on('error', err => {
                error = err;
                writer.close();
                reject(err);
              });
              writer.on('close', () => {
                config.log.info(context, 'Download complete to file: ' + filepath + ' for repository: ' + repository['repository_id'] + ' for repertoire_id: ' + repertoire_id);
                if (!error) {
                  resolve(true);
                }
              });
            });
        });
}

// Query the repertoires from an ADC repository with optional study_id
adcIO.getRepertoires = async function(repository, study_id) {
    var msg = null;

    // we assume the passed in repository is an object entry
    if (! repository) return Promise.resolve(null);
    if (! repository['server_host']) return Promise.reject('repository entry missing server_host');
    if (! repository['base_url']) return Promise.reject('repository entry missing base_url');

    // query on study_id
    var postData = {
        "filters": {
            "op": "=",
            "content": {
                "field": "study.study_id",
                "value": study_id
            }
        }
    };

    postData = JSON.stringify(postData);

    var requestSettings = {
        host:     repository['server_host'],
        method:   'POST',
        path:     repository['base_url'] + '/repertoire',
        rejectUnauthorized: false,
        headers: {
            'Content-Type':   'application/json',
            'Content-Length': Buffer.byteLength(postData)
        }
    };
    console.log(requestSettings);

    var data = await adcIO.sendRequest(requestSettings, postData)
        .catch(function(error) {
            msg = 'VDJ-API ERROR: adcIO.getRepertoires, adcIO.sendRequest error ' + error;
        });
    if (msg) {
        console.error(msg);
        webhookIO.postToSlack(msg);
        return Promise.reject(new Error(msg));
    }
    return Promise.resolve(data);
}

// Query the studies from an ADC repository
adcIO.getStudies = async function(repository) {
    var msg = null;

    // we assume the passed in repository is an object entry
    if (! repository) return Promise.resolve(null);
    if (! repository['server_host']) return Promise.reject('repository entry missing server_host');
    if (! repository['base_url']) return Promise.reject('repository entry missing base_url');

    // do a facets query
    var postData = {
        facets: 'study.study_id',
    };

    postData = JSON.stringify(postData);

    var requestSettings = {
        host:     repository['server_host'],
        method:   'POST',
        path:     repository['base_url'] + '/repertoire',
        rejectUnauthorized: false,
        headers: {
            'Content-Type':   'application/json',
            'Content-Length': Buffer.byteLength(postData)
        }
    };
    console.log(requestSettings);

    var data = await adcIO.sendRequest(requestSettings, postData)
        .catch(function(error) {
            msg = 'VDJ-API ERROR: adcIO.getStudies, adcIO.sendRequest error ' + error;
        });
    if (msg) {
        console.error(msg);
        webhookIO.postToSlack(msg);
        return Promise.reject(new Error(msg));
    }

    return Promise.resolve(data['Facet']);
}

//
// Functions for the ADC download cache
//

adcIO.getCachedStudy = function(study_id) {
}

adcIO.getCachedRepertoiresForStudy = function(study_id) {
}

// 1. iterate
adcIO.createCacheEntries = function() {

}

// send a notification
adcIO.sendNotification = function(notification, data) {

    // pull out host and path from URL
    // TODO: handle http/https
    var fields = notification['url'].split('://');
    fields = fields[1].split('/');
    var host = fields[0];
    fields = notification['url'].split(host);
    var path = fields[1];

    var postData = null;
    var method = 'GET';
    if (data) {
        // put data in request params
        if (notification["method"] == 'GET') {
            method = 'GET';

            // check if URL already has some request params
            var mark;
            if (path.indexOf('?') >= 0) mark = '&';
            else mark = '?';

            var keys = Object.keys(data);
            for (var p = 0; p < keys.length; ++p) {
                path += mark;
                path += keys[p] + '=' + encodeURIComponent(data[keys[p]]);
                mark = '&';
            }
        } else {
            method = 'POST';
            postData = JSON.stringify(data);
        }
    }

    var requestSettings = {
        host:     host,
        method:   method,
        path:     path,
        rejectUnauthorized: false,
        headers: {
            'Content-Type': 'application/json',
            'Accept':   'application/json'
        }
    };

    if (postData) {
        requestSettings['headers']['Content-Length'] = Buffer.byteLength(postData);
    }

    console.log(requestSettings);

    return adcIO.sendRequest(requestSettings, postData);
};
