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
    //if (config.shouldInjectError("tapisIO.refreshToken")) return config.performInjectError();

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

//
/////////////////////////////////////////////////////////////////////
//
// Metadata operations
//

tapisIO.getMetadata = function(uuid) {

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/meta/v2/data/' + uuid,
                rejectUnauthorized: false,
                headers: {
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                }
            };

            return tapisIO.sendRequest(requestSettings, null);
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

tapisIO.updateMetadata = function(uuid, name, value, associationIds) {

    var postData = {
        name: name,
        value: value
    };
    if (associationIds) postData.associationIds = associationIds;

    postData = JSON.stringify(postData);

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'POST',
                path:     '/meta/v2/data/' + uuid,
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
            console.log('tapisIO.updateMetadata error: ' + errorObject);
            return Promise.reject(errorObject);
        });
};

tapisIO.deleteMetadata = function(accessToken, uuid) {

    var requestSettings = {
        host:     tapisSettings.hostname,
        method:   'DELETE',
        path:     '/meta/v2/data/' + uuid,
        rejectUnauthorized: false,
        headers: {
            'Authorization': 'Bearer ' + accessToken
        }
    };

    return tapisIO.sendRequest(requestSettings, null)
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

tapisIO.getMetadataPermissions = function(accessToken, uuid) {

    var requestSettings = {
        host:     tapisSettings.hostname,
        method:   'GET',
        path:     '/meta/v2/data/' + uuid + '/pems',
        rejectUnauthorized: false,
        headers: {
            'Authorization': 'Bearer ' + accessToken
        }
    };

    return tapisIO.sendRequest(requestSettings, null)
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

tapisIO.getMetadataPermissionsForUser = function(accessToken, uuid, username) {

    var requestSettings = {
        host:     tapisSettings.hostname,
        method:   'GET',
        path:     '/meta/v2/data/' + uuid + '/pems/' + username,
        rejectUnauthorized: false,
        headers: {
            'Authorization': 'Bearer ' + accessToken
        }
    };

    return tapisIO.sendRequest(requestSettings, null)
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

//
/////////////////////////////////////////////////////////////////////
//
// Project operations
//

// generic metadata query
tapisIO.getMetadataForType = function(accessToken, projectUuid, type) {
    //if (config.shouldInjectError("tapisIO.getMetadataForType")) return config.performInjectError();

    var models = [];

    var doFetch = function(offset) {
        var requestSettings = {
            host:     tapisSettings.hostname,
            method:   'GET',
            path:   '/meta/v2/data?q='
                + encodeURIComponent('{'
                                     + '"name": "' + type + '",'
                                     + '"associationIds": "' + projectUuid + '"'
                                     + '}')
                + '&limit=50&offset=' + offset,
            rejectUnauthorized: false,
            headers: {
                'Authorization': 'Bearer ' + accessToken
            }
        };

        return tapisIO.sendRequest(requestSettings, null)
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

// generic metadata creation
tapisIO.createMetadataForType = function(projectUuid, type, value) {
    if (config.shouldInjectError("tapisIO.createMetadataForType")) return config.performInjectError();

    var postData = {
        associationIds: [ projectUuid ],
        name: type,
        value: value,
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

// create metadata record for a private project
tapisIO.createProjectMetadata = function(project) {
    if (config.shouldInjectError("tapisIO.createProjectMetadata")) return config.performInjectError();

    var postData = {
        name: 'private_project',
        value: project
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

tapisIO.getProjectMetadata = function(accessToken, projectUuid) {
    if (config.shouldInjectError("tapisIO.getProjectMetadata")) return config.performInjectError();

    var requestSettings = {
        host:     tapisSettings.hostname,
        method:   'GET',
        path:     '/meta/v2/data/' + projectUuid,
        rejectUnauthorized: false,
        headers: {
            'Authorization': 'Bearer ' + accessToken
        }
    };

    return tapisIO.sendRequest(requestSettings, null)
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

//
/////////////////////////////////////////////////////////////////////
//
// Job functions
//

tapisIO.getJobOutput = function(jobId) {

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/jobs/v2/' + jobId,
                rejectUnauthorized: false,
                headers: {
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                }
            };

            return tapisIO.sendRequest(requestSettings, null);
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

tapisIO.launchJob = function(jobDataString) {

    var postData = JSON.stringify(jobDataString);
    console.log(postData);

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'POST',
                path:     '/jobs/v2/'
                ,
                rejectUnauthorized: false,
                headers: {
                    'Content-Length': Buffer.byteLength(postData),
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                },
            };

            console.log(requestSettings);

            return tapisIO.sendRequest(requestSettings, postData);
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

//
/////////////////////////////////////////////////////////////////////
//
// Community data
//

/*
tapisIO.setCommunityFilePermissions = function(projectUuid, filePath, toCommunity) {

    return ServiceAccount.getToken()
        .then(function(token) {
            // get all user permissions
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/files/v2/pems/system/' + tapisSettings.storageSystem + '//projects/' + filePath,
                rejectUnauthorized: false,
                headers: {
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken(),
                }
            };

            return tapisIO.sendRequest(requestSettings, null);
        })
        .then(function(requestObject) {
            var permissionsList = requestObject.result;

            // remove permissions
            var promises = [];
            for (var i = 0; i < permissionsList.length; i++) {
                var entry = permissionsList[i];
                if (entry.username != tapisSettings.serviceAccountKey)
                    promises[i] = tapisIO.setFilePermissions(ServiceAccount.accessToken(), entry.username, 'NONE', false, '/projects/' + filePath);
            }

            return Promise.allSettled(promises);
        })
        .then(function(responseObject) {
            if (toCommunity) {
                // guest account READ only
                var postData = 'username=' + tapisSettings.guestAccountKey + '&permission=READ&recursive=false';

                var requestSettings = {
                    host:     tapisSettings.hostname,
                    method:   'POST',
                    path:     '/files/v2/pems/system/' + tapisSettings.storageSystem + '//projects/' + filePath,
                    rejectUnauthorized: false,
                    headers: {
                        'Content-Length': Buffer.byteLength(postData),
                        'Authorization': 'Bearer ' + ServiceAccount.accessToken(),
                    }
                };

                return tapisIO.sendRequest(requestSettings, postData);
            } else {
                return tapisIO.setFilePermissionsForProjectUsers(projectUuid, filePath, false);
            }
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
}; */

//
tapisIO.createCommunityCacheDirectory = function(directory, subpath) {

    var cache_dir = '/community/cache/';
    if (subpath) cache_dir += subpath;
    var postData = 'action=mkdir&path=' + directory;

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'PUT',
                path:     '/files/v2/media/system/' + tapisSettings.storageSystem + '/' + cache_dir,
                rejectUnauthorized: false,
                headers: {
                    'Content-Length': Buffer.byteLength(postData),
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken(),
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

//
/////////////////////////////////////////////////////////////////////
//
// Statistics cache functions
//

// Statistics cache status
// this should be a singleton metadata entry owned by service account
tapisIO.createStatisticsCache = function() {
    //if (config.shouldInjectError("tapisIO.createStatisticsCache")) return config.performInjectError();

    var postData = {
        name: 'statistics_cache',
        value: {
            enable_cache: false
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

tapisIO.getStatisticsCache = function() {
    //if (config.shouldInjectError("tapisIO.getStatisticsCache")) return config.performInjectError();

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/meta/v2/data?q='
                    + encodeURIComponent(
                        '{"name":"statistics_cache",'
                            + ' "owner":"' + ServiceAccount.username + '"}'
                    )
                    + '&limit=1'
                ,
                rejectUnauthorized: false,
                headers: {
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                }
            };

            return tapisIO.sendRequest(requestSettings, null);
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
}

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
            statistics_job_id: null,
            statistics: null
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

//
/////////////////////////////////////////////////////////////////////
//
// AIRR Data Commons functions
//

// the global/system list of ADC repositories
// this should be a singleton metadata entry owned by service account
tapisIO.getSystemADCRepositories = function() {

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/meta/v2/data?q='
                    + encodeURIComponent(
                        '{"name":"adc_system_repositories",'
                            + ' "owner":"' + ServiceAccount.username + '"}'
                    )
                    + '&limit=1'
                ,
                rejectUnauthorized: false,
                headers: {
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                }
            };

            return tapisIO.sendRequest(requestSettings, null);
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
}

// ADC download cache status
// this should be a singleton metadata entry owned by service account
tapisIO.createADCDownloadCache = function() {
    //if (config.shouldInjectError("tapisIO.createADCDownloadCache")) return config.performInjectError();

    var postData = {
        name: 'adc_cache',
        value: {
            enable_cache: false
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

tapisIO.getADCDownloadCache = function() {
    //if (config.shouldInjectError("tapisIO.getADCDownloadCache")) return config.performInjectError();

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/meta/v2/data?q='
                    + encodeURIComponent(
                        '{"name":"adc_cache",'
                            + ' "owner":"' + ServiceAccount.username + '"}'
                    )
                    + '&limit=1'
                ,
                rejectUnauthorized: false,
                headers: {
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                }
            };

            return tapisIO.sendRequest(requestSettings, null);
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
}

// create metadata entry for cached ADC study
tapisIO.createCachedStudyMetadata = function(repository_id, study_id, should_cache) {

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

// get list of studies cache entries
tapisIO.getStudyCacheEntries = function(repository_id, study_id, should_cache, is_cached) {

    var models = [];

    var query = '{"name":"adc_cache_study"';
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

// create metadata entry for cached ADC rearrangements for a single repertoire
tapisIO.createCachedRepertoireMetadata = function(repository_id, study_id, repertoire_id, should_cache) {

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
tapisIO.getRepertoireCacheEntries = function(repository_id, study_id, repertoire_id, should_cache, not_cached, max_limit) {

    var models = [];

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
