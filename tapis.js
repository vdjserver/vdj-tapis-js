'use strict';

//
// tapis.js
// Wrapper functions for accessing the Tapis APIs
//
// VDJServer
// http://vdjserver.org
//
// Copyright (C) 2020-2023 The University of Texas Southwestern Medical Center
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
var _ = require('underscore');
var jsonApprover = require('json-approver');
var FormData = require('form-data');
const axios = require('axios');

// Settings
var tapisSettings = require('./tapisSettings');
tapisIO.tapisSettings = tapisSettings;

// Models
var ServiceAccount = require('./serviceAccount');
tapisIO.serviceAccount = ServiceAccount;
var GuestAccount = require('./guestAccount');
tapisIO.guestAccount = GuestAccount;
var MetadataPermissions = require('./metadataPermissions');

// Controller
var authController = require('./authController');
tapisIO.authController = authController;

//
// Generic send request
//
tapisIO.sendRequest = function(requestSettings, postData, allow404, trap408) {

    return new Promise(function(resolve, reject) {
        var request = require('https').request(requestSettings, function(response) {

            var output = '';

            response.on('data', function(chunk) {
                output += chunk;
            });

            response.on('end', function() {

                var responseObject = null;

                if (output && jsonApprover.isJSON(output))
                    responseObject = JSON.parse(output);

                // reject on errors
                if (response.statusCode >= 400) {
                    //console.log(response.statusCode, allow404, trap408);
                    if (response.statusCode == 404) {
                        if (!allow404) return reject(new Error('response code: ' + response.statusCode + ', request error: ' + output));
                        else responseObject['statusCode'] = 404;
                    } else if (trap408 && (response.statusCode == 408)) {
                        // Tapis will return a JSON object with the 408 code
                        return reject(responseObject);
                    } else {
                        return reject(new Error('response code: ' + response.statusCode + ', request error: ' + output));
                    }
                }

                // process output
                if (output.length == 0) return resolve(null);

                if (jsonApprover.isJSON(output)) {
                    return resolve(responseObject);
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

// This is specific to sending multi-part form post data, i.e. uploading files
tapisIO.sendFormRequest = function(requestSettings, formData) {

    return new Promise(function(resolve, reject) {
        var request = formData.submit(requestSettings, function(error, response) {

            var output = '';

            response.on('data', function(chunk) {
                output += chunk;
            });

            response.on('end', function() {

                var responseObject;

                if (output && jsonApprover.isJSON(output)) {
                    responseObject = JSON.parse(output);
                }
                else {
                    reject(new Error('Agave response is not json'));
                }

                if (responseObject && responseObject.status && responseObject.status.toLowerCase() === 'success') {
                    resolve(responseObject);
                }
                else {
                    reject(new Error('Agave response returned an error: ' + JSON.stringify(responseObject)));
                }
            });
        });

        request.on('error', function(error) {
            reject(new Error('Agave connection error. ' + JSON.stringify(error)));
        });
    });
};

// This is specific to sending a token request
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

//
// For retrieving unparsed file data
//
tapisIO.sendFileRequest = function(requestSettings, postData) {

    return new Promise(function(resolve, reject) {
        var request = require('https').request(requestSettings, function(response) {

            var output = '';

            response.on('data', function(chunk) {
                output += chunk;
            });

            response.on('end', function() {

                if (response.statusCode == 404) {
                    // file not found
                    resolve(null);
                } else if (response.statusCode >= 400) {
                    // error
                    reject(new Error('Request error: ' + output));
                } else {
                    // do not attempt to parse
                    resolve(output);
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
    //if (tapisSettings.shouldInjectError("tapisIO.refreshToken")) return tapisSettings.performInjectError();

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

tapisIO.validateToken = function(token) {

    var requestSettings = {
        host:   tapisSettings.hostname,
        method: 'GET',
        path:   '/systems/v2/',
        rejectUnauthorized: false,
        headers: {
            'Authorization': 'Bearer ' + token,
        }
    };

    return tapisIO.sendRequest(requestSettings, null)
        .then(function(responseObject) {
            return Promise.resolve();
        })
        .catch(function(errorObject) {
            return Promise.reject(new Error('Unable to validate token.'));
        });
};

//
/////////////////////////////////////////////////////////////////////
//
// Notifications
//

// send a notification
tapisIO.sendNotification = function(notification, data) {

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

    //console.log(requestSettings);

    return tapisIO.sendRequest(requestSettings, postData);
};

//
/////////////////////////////////////////////////////////////////////
//
// Apps
//
tapisIO.getApplication = function(name) {
    //if (tapisSettings.shouldInjectError("tapisIO.getApplication")) return tapisSettings.performInjectError();

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/apps/v2/' + name,
                rejectUnauthorized: false,
                headers: {
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                }
            };

            return tapisIO.sendRequest(requestSettings, null, true);
        })
        .then(function(responseObject) {
            if (responseObject['statusCode'] == 404) return Promise.resolve(responseObject);
            else return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
}

//
/////////////////////////////////////////////////////////////////////
//
// User operations
//

tapisIO.isDuplicateUsername = function(username) {
    //if (config.shouldInjectError("tapisIO.isDuplicateUsername")) return config.performInjectError();

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:   tapisSettings.hostname,
                method: 'GET',
                path:   '/profiles/v2/' + username,
                rejectUnauthorized: false,
                headers: {
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                }
            };

            return tapisIO.sendRequest(requestSettings, null);
        })
        .then(function(responseObject) {
            return Promise.resolve(true);
        })
        .catch(function(errorObject) {
            return Promise.resolve(false);
        });
};

tapisIO.createUser = function(user) {
    //if (tapisSettings.shouldInjectError("tapisIO.createUser")) return tapisSettings.performInjectError();

    var postData = 'username='  + user.username
                 + '&password=' + user.password
                 + '&email='    + user.email;

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'POST',
                path:     '/profiles/v2/',
                rejectUnauthorized: false,
                headers: {
                    'Content-Type':   'application/x-www-form-urlencoded',
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

tapisIO.getTapisUserProfile = function(accessToken, username) {
    //if (tapisSettings.shouldInjectError("tapisIO.getTapisUserProfile")) return tapisSettings.performInjectError();

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:   tapisSettings.hostname,
                method: 'GET',
                path:   '/profiles/v2/' + username,
                rejectUnauthorized: false,
                headers: {
                    'Authorization': 'Bearer ' + accessToken
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

tapisIO.createUserProfile = function(user, userAccessToken) {
    //if (tapisSettings.shouldInjectError("tapisIO.createUserProfile")) return tapisSettings.performInjectError();

    var postData = {
        name: 'profile',
        value: user
    };

    postData = JSON.stringify(postData);

    var requestSettings = {
        host:     tapisSettings.hostname,
        method:   'POST',
        path:     '/meta/v2/data',
        rejectUnauthorized: false,
        headers: {
            'Content-Type':   'application/json',
            'Content-Length': Buffer.byteLength(postData),
            'Authorization': 'Bearer ' + userAccessToken
        }
    };

    return tapisIO.sendRequest(requestSettings, postData)
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

tapisIO.getUserProfile = function(username) {
    //if (tapisSettings.shouldInjectError("tapisIO.getUserProfile")) return tapisSettings.performInjectError();

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/meta/v2/data?q=' + encodeURIComponent('{"name":"profile","owner":"' + username + '"}') + '&limit=1',
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

tapisIO.createUserVerificationMetadata = function(username) {
    //if (tapisSettings.shouldInjectError("tapisIO.createUserVerificationMetadata")) return tapisSettings.performInjectError();

    var postData = {
        name: 'userVerification',
        value: {
            'username': username,
            'isVerified': false,
        },
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

tapisIO.getUserVerificationMetadata = function(username) {
    //if (tapisSettings.shouldInjectError("tapisIO.getUserVerificationMetadata")) return tapisSettings.performInjectError();

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/meta/v2/data?q='
                    + encodeURIComponent(
                        '{"name":"userVerification",'
                            + ' "value.username":"' + username + '",'
                            + ' "owner":"' + ServiceAccount.username + '"'
                            + '}'
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
};

tapisIO.verifyUser = function(username, verificationId) {
    //if (tapisSettings.shouldInjectError("tapisIO.verifyUser")) return tapisSettings.performInjectError();

    var postData = {
        name: 'userVerification',
        value: {
            'username': username,
            'isVerified': true,
        },
    };

    postData = JSON.stringify(postData);

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'POST',
                path:     '/meta/v2/data/' + verificationId,
                rejectUnauthorized: false,
                headers: {
                    'Content-Type':   'application/json',
                    'Content-Length': Buffer.byteLength(postData),
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken(),
                },
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

tapisIO.createPasswordResetMetadata = function(username) {
    //if (tapisSettings.shouldInjectError("tapisIO.createPasswordResetMetadata")) return tapisSettings.performInjectError();

    var postData = {
        name: 'passwordReset',
        value: {
            'username': username
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

tapisIO.getPasswordResetMetadata = function(uuid) {

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/meta/v2/data?q='
                    + encodeURIComponent(
                        '{"name":"passwordReset",'
                            + ' "uuid":"' + uuid + '",'
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
};

tapisIO.updateUserPassword = function(user) {

    var postData = 'username='  + user.username
                 + '&password=' + user.password
                 + '&email='    + user.email;

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'PUT',
                path:     '/profiles/v2/' + user.username + '/',
                rejectUnauthorized: false,
                headers: {
                    'Content-Type':   'application/x-www-form-urlencoded',
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

tapisIO.createFeedbackMetadata = function(feedback, username, email) {

    var valueData = {
        feedbackMessage: feedback,
    };

    if (username.length > 0) {
        valueData.username = username;
    }

    if (email.length > 0) {
        valueData.email = email;
    }

    var postData = {
        name: 'feedback',
        value: valueData,
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
                    'Authorization':  'Bearer ' + ServiceAccount.accessToken()
                },
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

            return tapisIO.sendRequest(requestSettings, null, true);
        })
        .then(function(responseObject) {
            if (responseObject['statusCode'] == 404) return Promise.resolve(responseObject);
            else return Promise.resolve(responseObject.result);
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
            console.error('tapisIO.updateMetadata error: ' + errorObject);
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
// Meta/V3 operations
//

// general query
tapisIO.performQuery = function(collection, query, projection, page, pagesize, count) {

    return GuestAccount.getToken()
        .then(function(token) {
            var mark = false;
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/meta/v3/' + tapisSettings.mongo_dbname + '/' + collection,
                rejectUnauthorized: false,
                headers: {
                    'Accept':   'application/json',
                    'Authorization': 'Bearer ' + GuestAccount.accessToken()
                }
            };
            if (count) {
                requestSettings['path'] += '/_size';
            }
            if (query != null) {
                if (mark) requestSettings['path'] += '&';
                else requestSettings['path'] += '?';
                mark = true;
                requestSettings['path'] += 'filter=' + encodeURIComponent(query);
            }
            if (projection != null) {
                if (mark) requestSettings['path'] += '&';
                else requestSettings['path'] += '?';
                mark = true;
                requestSettings['path'] += 'keys=' + encodeURIComponent(JSON.stringify(projection));
            }
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
            var sort = {};
            if (sort) {
                if (mark) requestSettings['path'] += '&';
                else requestSettings['path'] += '?';
                mark = true;
                requestSettings['path'] += 'sort=' + encodeURIComponent(JSON.stringify(sort));
            }

            //console.log(requestSettings);

            return tapisIO.sendRequest(requestSettings, null, false, true);
        });
};

// general large queries
tapisIO.performLargeQuery = function(collection, query, projection, page, pagesize) {

    var postData = query;
    if (! postData) return Promise.reject(new Error('TAPIS-API ERROR: Empty query passed to tapisIO.performLargeQuery'));

    return GuestAccount.getToken()
        .then(function(token) {
            var mark = false;
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'POST',
                path:     '/meta/v3/' + tapisSettings.mongo_dbname + '/' + collection + '/_filter',
                rejectUnauthorized: false,
                headers: {
                    'Accept':   'application/json',
                    'Authorization': 'Bearer ' + GuestAccount.accessToken(),
                    'Content-Type': 'application/json',
                    'Content-Length': Buffer.byteLength(postData)
                }
            };
            if (projection != null) {
                if (mark) requestSettings['path'] += '&';
                else requestSettings['path'] += '?';
                mark = true;
                requestSettings['path'] += 'keys=' + encodeURIComponent(JSON.stringify(projection));
            }
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
            var sort = {};
            if (sort) {
                if (mark) requestSettings['path'] += '&';
                else requestSettings['path'] += '?';
                mark = true;
                requestSettings['path'] += 'sort=' + encodeURIComponent(JSON.stringify(sort));
            }

            //console.log(requestSettings);

            return tapisIO.sendRequest(requestSettings, postData, false, true);
        });
};

// General query that performs multiple requests to retrieve all of the results.
// Will utilize the appropriate function based upon the size of the query.
// This should not be utilized for queries that may return a large amount of data
// because the data is pulled into memory.
tapisIO.performMultiQuery = function(collection, query, projection, start_page, pagesize) {
    var models = [];

    //console.log(query);
    var doQuery = function(page) {
        var queryFunction = tapisIO.performQuery;
        if (query && query.length > tapisSettings.large_query_size) queryFunction = tapisIO.performLargeQuery;
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
tapisIO.performAggregation = function(collection, aggregation, query, field, page, pagesize) {

    return GuestAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/meta/v3/' + tapisSettings.mongo_dbname + '/' + collection + '/_aggrs/' + aggregation,
                rejectUnauthorized: false,
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'Authorization': 'Bearer ' + GuestAccount.accessToken()
                }
            };

            requestSettings['path'] += '?avars=';
            requestSettings['path'] += encodeURIComponent('{"match":' + query + ',"field":"' + field + '"}');
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

            //console.log(requestSettings);

            return tapisIO.sendRequest(requestSettings, null, false, true);
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject);
        })
        .catch(function(errorObject) {
            console.log('tapisIO.performAggregation');
            //console.log(errorObject);
            //if (errorObject['http status code'] == 408) console.log('got timeout');
            return Promise.reject(errorObject);
        });
};

// general large aggregation
tapisIO.performLargeAggregation = function(collection, aggregation, query, field, page, pagesize) {

    var postData = '{"match":' + query + ',"field":"' + field + '"}';
    //console.log(postData);

    return GuestAccount.getToken()
        .then(function(token) {
            var mark = false;
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'POST',
                path:     '/meta/v3/' + tapisSettings.mongo_dbname + '/' + collection + '/_aggrs/' + aggregation,
                rejectUnauthorized: false,
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'Authorization': 'Bearer ' + GuestAccount.accessToken(),
                    'Content-Length': Buffer.byteLength(postData)
                }
            };
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

            //console.log(requestSettings);

            return tapisIO.sendRequest(requestSettings, postData, false, true);
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject);
        })
        .catch(function(errorObject) {
            console.log('tapisIO.performLargeAggregation');
            //console.log(errorObject);
            //if (errorObject['http status code'] == 408) console.log('got timeout');
            return Promise.reject(errorObject);
        });
};

// record info about ADC query performed
tapisIO.recordQuery = function(query) {

    return ServiceAccount.getToken()
        .then(function(token) {

            // TAPIS BUG: POST gives error unless array
            var postData = JSON.stringify([query]);

            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'POST',
                path:     '/meta/v3/' + tapisSettings.mongo_dbname + '/query',
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
            // TAPIS BUG: we eat the error here to avoid node termination
            console.error('TAPIS-API ERROR: (tapisIO.recordQuery) error: ' + errorObject);
            return Promise.resolve(null);
        });
};

// delete document in the database
tapisIO.deleteDocument = async function(collection, document_id) {

    return ServiceAccount.getToken()
        .then(function(token) {

            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'DELETE',
                path:     '/meta/v3/' + tapisSettings.mongo_dbname + '/' + collection
                    + '/' + document_id,
                rejectUnauthorized: false,
                headers: {
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                }
            };

            //console.log(requestSettings);

            return tapisIO.sendRequest(requestSettings, null);
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject);
        })
        .catch(function(errorObject) {
            console.error(errorObject);
            return Promise.reject(errorObject);
        });
};

//
/////////////////////////////////////////////////////////////////////
//
// ADC Extension API for asynchronous queries
// LRQ Meta/V3 operations
//

tapisIO.performAsyncQuery = function(collection, query, projection, notification) {

    var postData = {
        name: "query",
        queryType: "SIMPLE",
        query: [ query ]
    };
    if (notification) postData['notification'] = notification;
    postData = JSON.stringify(postData);
    console.log(postData);

    return ServiceAccount.getToken()
        .then(function(token) {
            var mark = false;
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'POST',
                path:     '/meta/v3/' + tapisSettings.mongo_dbname + '/' + collection + '/_lrq',
                rejectUnauthorized: false,
                headers: {
                    'Content-Type': 'application/json',
                    'Accept':   'application/json',
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken(),
                    'Content-Length': Buffer.byteLength(postData)
                }
            };

            console.log(requestSettings);

            return tapisIO.sendRequest(requestSettings, postData);
        });
};

tapisIO.performAsyncAggregation = function(name, collection, query, notification) {

    var postData = {
        name: name,
        queryType: "AGGREGATION",
        query: query
    };
    if (notification) postData['notification'] = notification;
    postData = JSON.stringify(postData);

    return ServiceAccount.getToken()
        .then(function(token) {
            var mark = false;
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'POST',
                path:     '/meta/v3/' + tapisSettings.mongo_dbname + '/' + collection + '/_lrq',
                rejectUnauthorized: false,
                headers: {
                    'Content-Type': 'application/json',
                    'Accept':   'application/json',
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken(),
                    'Content-Length': Buffer.byteLength(postData)
                }
            };

            console.log(requestSettings);
            console.log(postData);

            return tapisIO.sendRequest(requestSettings, postData);
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject);
        })
        .catch(function(errorObject) {
            console.error('performAsyncAggregation: ' + errorObject);
            return Promise.reject(errorObject);
        });
};

tapisIO.getLRQStatus = function(lrq_id) {

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/meta/v3/LRQ/vdjserver.org/' + lrq_id,
                rejectUnauthorized: false,
                headers: {
                    'Accept':   'application/json',
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                }
            };

            //console.log(requestSettings);

            return tapisIO.sendRequest(requestSettings, null);
        });
};

// create metadata record for async query
tapisIO.createAsyncQueryMetadata = function(endpoint, collection, body, query_aggr, count_aggr) {

    var postData = {
        name: 'async_query',
        value: {
            endpoint: endpoint,
            collection: collection,
            lrq_id: null,
            status: 'PENDING',
            message: null,
            notification: null,
            raw_file: null,
            final_file: null,
            download_url: null,
            body: body,
            query_aggr: query_aggr,
            count_aggr: count_aggr
        }
    };
    if (body['notification']) postData['value']['notification'] = body['notification'];

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

tapisIO.getAsyncQueryMetadata = function(lrq_id) {

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/meta/v2/data?q='
                    + encodeURIComponent(
                        '{"name":"async_query",'
                            + ' "value.lrq_id":"' + lrq_id + '"}'
                    )
                    + '&limit=1'
                ,
                rejectUnauthorized: false,
                headers: {
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                }
            };

            console.log(requestSettings);

            return tapisIO.sendRequest(requestSettings, null);
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

tapisIO.getAsyncQueryMetadataWithStatus = function(status) {

    var models = [];

    var doFetch = function(offset) {
        return ServiceAccount.getToken()
            .then(function(token) {
                var requestSettings = {
                    host:     tapisSettings.hostname,
                    method:   'GET',
                    path:     '/meta/v2/data?q='
                        + encodeURIComponent(
                            '{"name":"async_query",'
                                + ' "value.status":"' + status
                                + '"}')
                        + '&limit=50&offset=' + offset,
                    rejectUnauthorized: false,
                    headers: {
                        'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                    }
                };

                return tapisIO.sendRequest(requestSettings, null);
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

//
/////////////////////////////////////////////////////////////////////
//
// File operations
//

// generic get contents of file, should only be used for small files
// as contents are brought into memory
tapisIO.getFileContents = function(filepath) {

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/files/v2/media/system'
                    + '/' + tapisSettings.storageSystem + '/'
                    + filepath,
                rejectUnauthorized: false,
                headers: {
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                },
            };

            return tapisIO.sendFileRequest(requestSettings, null);
        })
        .then(function(fileData) {
            return Promise.resolve(fileData);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

// will delete single file or directory tree
tapisIO.deleteFile = function(filepath) {

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'DELETE',
                path:     '/files/v2/media/system'
                    + '/' + tapisSettings.storageSystem + '/'
                    + filepath,
                rejectUnauthorized: false,
                headers: {
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                },
            };

            return tapisIO.sendRequest(requestSettings, null);
        })
        .then(function() {
            return Promise.resolve();
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
    //if (tapisSettings.shouldInjectError("tapisIO.getMetadataForType")) return tapisSettings.performInjectError();

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
    //if (tapisSettings.shouldInjectError("tapisIO.createMetadataForType")) return tapisSettings.performInjectError();

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
    //if (tapisSettings.shouldInjectError("tapisIO.createProjectMetadata")) return tapisSettings.performInjectError();

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
    //if (tapisSettings.shouldInjectError("tapisIO.getProjectMetadata")) return tapisSettings.performInjectError();

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

tapisIO.createProjectDirectory = function(directory) {

    var postData = 'action=mkdir&path=' + directory;

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'PUT',
                path:     '/files/v2/media/system/' + tapisSettings.storageSystem + '//projects/',
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

tapisIO.addUsernameToMetadataPermissions = function(username, accessToken, uuid) {

    var postData = 'username=' + username + '&permission=READ_WRITE';

    var requestSettings = {
        host:     tapisSettings.hostname,
        method:   'POST',
        path:     '/meta/v2/data/' + uuid + '/pems',
        rejectUnauthorized: false,
        headers: {
            'Content-Length': Buffer.byteLength(postData),
            'Authorization': 'Bearer ' + accessToken,
        },
    };

    return tapisIO.sendRequest(requestSettings, postData)
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

tapisIO.removeUsernameFromMetadataPermissions = function(username, accessToken, uuid) {

    var requestSettings = {
        host:     tapisSettings.hostname,
        method:   'DELETE',
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

tapisIO.getProjectFileMetadata = function(projectUuid) {

    var models = [];

    var doFetch = function(offset) {
        return ServiceAccount.getToken()
            .then(function(token) {
                var requestSettings = {
                    host:   tapisSettings.hostname,
                    method: 'GET',
                    path:   '/meta/v2/data?q='
                        + encodeURIComponent('{'
                                             + '"name": { $in: ["projectFile", "projectJobFile"] },'
                                             + '"value.projectUuid":"' + projectUuid + '"'
                                             + '}')
                        + '&limit=50&offset=' + offset
                    ,
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

tapisIO.uploadFileToProjectDirectory = function(projectUuid, filename, filedata) {

    // filedata should be data stored in a Buffer()
    var form = new FormData();
    form.append('fileToUpload', filedata);
    form.append('filename', filename);

    return ServiceAccount.getToken()
        .then(function(token) {
            var formHeaders = form.getHeaders();
            formHeaders.Authorization = 'Bearer ' + ServiceAccount.accessToken();
            var requestSettings = {
                host:     tapisSettings.hostname,
                protocol: 'https:',
                method:   'POST',
                path:     '/files/v2/media/system/' + tapisSettings.storageSystem
                    + '//projects/' + projectUuid + '/files',
                rejectUnauthorized: false,
                headers: formHeaders
            };

            return tapisIO.sendFormRequest(requestSettings, form);
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

tapisIO.uploadFileToProjectTempDirectory = function(projectUuid, filename, filedata) {

    // filedata should be data stored in a Buffer()
    var form = new FormData();
    form.append('fileToUpload', filedata);
    form.append('filename', filename);

    return ServiceAccount.getToken()
        .then(function(token) {
            var formHeaders = form.getHeaders();
            formHeaders.Authorization = 'Bearer ' + ServiceAccount.accessToken();
            var requestSettings = {
                host:     tapisSettings.hostname,
                protocol: 'https:',
                method:   'POST',
                path:     '/files/v2/media/system/' + tapisSettings.storageSystem
                    + '//projects/' + projectUuid + '/deleted',
                rejectUnauthorized: false,
                headers: formHeaders
            };

            return tapisIO.sendFormRequest(requestSettings, form);
        })
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

tapisIO.getProjectFiles = function(projectUuid) {

    var models = [];

    var doFetch = function(offset) {
        return ServiceAccount.getToken()
            .then(function(token) {
                var requestSettings = {
                    host:   tapisSettings.hostname,
                    method: 'GET',
                    path:   '/meta/v2/data?q='
                        + encodeURIComponent('{'
                                             + '"name": { $in: ["projectFile"] },'
                                             + '"associationIds":"' + projectUuid + '"'
                                             + '}')
                        + '&limit=50&offset=' + offset
                    ,
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
// Retrieve all project associated metadata
// This relies upon associationIds having the project uuid
// This performs multiple requests to get all of the records
//
tapisIO.getAllProjectAssociatedMetadata = function(projectUuid) {

    var models = [];

    var doFetch = function(offset) {
        return ServiceAccount.getToken()
            .then(function(token) {
                var requestSettings = {
                    host:   tapisSettings.hostname,
                    method: 'GET',
                    path:   '/meta/v2/data?q='
                        + encodeURIComponent('{'
                                             + '"associationIds":"' + projectUuid + '"'
                                             + '}')
                        + '&limit=100&offset=' + offset
                    ,
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

tapisIO.getFilePermissions = function(accessToken, filePath) {

    var requestSettings = {
        host:     tapisSettings.hostname,
        method:   'GET',
        path:     '/files/v2/pems/system/' + tapisSettings.storageSystem + '//projects/' + filePath,
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

tapisIO.getFileListings = function(accessToken, projectUuid) {

    var requestSettings = {
        host:     tapisSettings.hostname,
        method:   'GET',
        path:     '/files/v2/listings/system/' + tapisSettings.storageSystem + '//projects/' + projectUuid + '/files',
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

tapisIO.enumerateFileListings = function(projectUuid) {

    var pathList = [];
    var dirStack = [];

    var doFetch = function(offset, filePath) {
        //console.log(dirStack);
        return ServiceAccount.getToken()
            .then(function(token) {
                var requestSettings = {
                    host:     tapisSettings.hostname,
                    method:   'GET',
                    path:     '/files/v2/listings/system/' + tapisSettings.storageSystem + '//projects/' + projectUuid + filePath
                        + '?limit=100&offset=' + offset,
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
                    // parse results between directories and files
                    for (var i = 0; i < result.length; ++i) {
                        var obj = result[i];
                        if (obj.name == '.') continue;
                        if (obj.type == 'dir') {
                            var path = obj.path.replace('/projects/' + projectUuid, '');
                            //console.log(path);
                            // don't recurse down into the job files
                            if (filePath != 'analyses') dirStack.push(path);
                            pathList.push(path);
                        } else if (obj.type == 'file') {
                            var path = obj.path.replace('/projects/' + projectUuid, '');
                            pathList.push(path);
                        } else {
                            console.error('VDJ-API ERROR: Unknown file type: ' + obj);
                        }
                    }
                    // maybe more data
                    var newOffset = offset + result.length;
                    return doFetch(newOffset, filePath);
                } else {
                    // nothing left to enumerate
                    if (dirStack.length == 0)
                        return Promise.resolve(pathList);
                    else
                        return doFetch(0, dirStack.pop());
                }
            })
            .catch(function(errorObject) {
                return Promise.reject(errorObject);
            });
    }

    pathList.push('');
    return doFetch(0, '');
};

tapisIO.getFileHistory = function(relativePath) {

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/files/v2/history/system/' + tapisSettings.storageSystem + '//projects/' + relativePath,
                rejectUnauthorized: false,
                headers: {
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken(),
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

tapisIO.getProjectFileContents = function(projectUuid, fileName) {

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/files/v2/media/system'
                    + '/' + tapisSettings.storageSystem
                    + '//projects/' + projectUuid
                    + '/files'
                    + '/' + fileName,
                rejectUnauthorized: false,
                headers: {
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                },
            };

            return tapisIO.sendFileRequest(requestSettings, null);
        })
        .then(function(fileData) {
            return Promise.resolve(fileData);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

tapisIO.addUsernameToFullFilePermissions = function(username, accessToken, filePath, recursive) {

    var postData = {
        'username': username,
        'permission': 'ALL',
        'recursive': recursive,
    };

    postData = JSON.stringify(postData);

    var requestSettings = {
        host:     tapisSettings.hostname,
        method:   'POST',
        path:     '/files/v2/pems/system/' + tapisSettings.storageSystem + '//projects/' + filePath,
        rejectUnauthorized: false,
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(postData),
            'Authorization': 'Bearer ' + accessToken,
        }
    };

    return tapisIO.sendRequest(requestSettings, postData)
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

tapisIO.setFilePermissionsForProjectUsers = function(projectUuid, filePath, recursive) {

    return ServiceAccount.getToken()
        .then(function(token) {
            // get list of users from project metadata permissions
            return tapisIO.getMetadataPermissions(ServiceAccount.accessToken(), projectUuid);
        })
        .then(function(projectPermissions) {
            var metadataPermissions = new MetadataPermissions();

            var projectUsernames = metadataPermissions.getUsernamesFromMetadataResponse(projectPermissions);

            var promises = [];
            for (var i = 0; i < projectUsernames.length; i++) {
                var username = projectUsernames[i];
                promises[i] = tapisIO.addUsernameToFullFilePermissions(
                    username,
                    ServiceAccount.accessToken(),
                    filePath,
                    recursive
                );
            }

            return Promise.allSettled(promises);
        })
        .then(function() {
            return Promise.resolve();
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

tapisIO.setFilePermissions = function(accessToken, username, permission, recursive, filePath) {

    var postData = {
        'username': username,
        'permission': permission,
        'recursive': recursive,
    };

    postData = JSON.stringify(postData);

    var requestSettings = {
        host:     tapisSettings.hostname,
        method:   'POST',
        path:     '/files/v2/pems/system/' + tapisSettings.storageSystem + '/' + filePath,
        rejectUnauthorized: false,
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(postData),
            'Authorization': 'Bearer ' + accessToken,
        },
    };

    return tapisIO.sendRequest(requestSettings, postData)
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

tapisIO.removeUsernameFromFilePermissions = function(username, accessToken, filePath) {

    var postData = {
        'username': username,
        'permission': 'NONE',
        'recursive': true,
    };

    postData = JSON.stringify(postData);

    var requestSettings = {
        host:     tapisSettings.hostname,
        method:   'POST',
        path:     '/files/v2/pems/system/' + tapisSettings.storageSystem + '//projects/' + filePath,
        rejectUnauthorized: false,
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(postData),
            'Authorization': 'Bearer ' + accessToken,
        },
    };

    return tapisIO.sendRequest(requestSettings, postData)
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

tapisIO.removeAllFilePermissions = function(accessToken, filePath, recursive) {

    var postData = {
        'username': '*',
        'permission': 'NONE',
        'recursive': recursive,
    };

    postData = JSON.stringify(postData);

    var requestSettings = {
        host:     tapisSettings.hostname,
        method:   'POST',
        path:     '/files/v2/pems/system/' + tapisSettings.storageSystem + '//projects/' + filePath,
        rejectUnauthorized: false,
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(postData),
            'Authorization': 'Bearer ' + accessToken,
        },
    };

    return tapisIO.sendRequest(requestSettings, postData)
        .then(function(responseObject) {
            return Promise.resolve(responseObject.result);
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

tapisIO.createFileMetadata = function(fileUuid, projectUuid, fileType, name, length, readDirection, tags) {

    var postData = {
        associationIds: [
            fileUuid,
            projectUuid,
        ],
        name: 'projectFile',
        owner: '',
        value: {
            'projectUuid': projectUuid,
            'fileType': fileType,
            'name': name,
            'length': length,
            'isDeleted': false,
            'readDirection': readDirection,
            'publicAttributes': {
                'tags': tags,
            },
        },
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

tapisIO.getFileDetail = function(relativePath) {

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/files/v2/listings/system/' + tapisSettings.storageSystem + '//projects/' + relativePath,
                rejectUnauthorized: false,
                headers: {
                    'Authorization': 'Bearer ' + ServiceAccount.accessToken()
                },
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

tapisIO.getProjectFileMetadataByFilename = function(projectUuid, fileUuid) {

    var models = [];

    var doFetch = function(offset) {
        return ServiceAccount.getToken()
            .then(function(token) {
                var requestSettings = {
                    host:   tapisSettings.hostname,
                    method: 'GET',
                    path:   '/meta/v2/data?q='
                        + encodeURIComponent('{'
                                             + '"name": "projectFile",'
                                             + '"value.projectUuid": "' + projectUuid + '",'
                                             + '"associationIds": { $in: ["' + fileUuid + '"] }'
                                             + '}')
                        + '&limit=50&offset=' + offset
                    ,
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

tapisIO.getJobsForProject = function(projectUuid) {

    var models = [];

    var doFetch = function(offset) {
        return ServiceAccount.getToken()
            .then(function(token) {
                var requestSettings = {
                    host:   tapisSettings.hostname,
                    method: 'GET',
                    path:   '/jobs/v2/?archivePath.like=/projects/' + projectUuid + '*'
                        + '&limit=50&offset=' + offset,
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

// Collect list of repertoire metadata for project.
// This function transforms the normalized metadata
// records into the denormalized AIRR metadata format.

/* TODO: Need to airr-js library

tapisIO.gatherRepertoireMetadataForProject = function(projectUuid, keep_uuids) {

    var msg = null;
    var repertoireMetadata = [];
    var subjectMetadata = {};
    var sampleMetadata = {};
    var dpMetadata = {};
    var projectMetadata = null;

    return ServiceAccount.getToken()
        .then(function(token) {
            // get the project metadata
            return tapisIO.getProjectMetadata(ServiceAccount.accessToken(), projectUuid);
        })
        .then(function(_projectMetadata) {
            projectMetadata = _projectMetadata;

            // get repertoire objects
            return tapisIO.getMetadataForType(ServiceAccount.accessToken(), projectUuid, 'repertoire');
        })
        .then(function(models) {
            // put into AIRR format
            var study = projectMetadata.value;
            var blank = airr.repertoireTemplate();

            // only the AIRR fields
            for (var o in blank['study']) {
                blank['study'][o] = study[o];
            }
            // always save vdjserver project uuid in custom field
            blank['study']['vdjserver_uuid'] = projectUuid;
            // also save any vdjserver keywords
            if (study['vdjserver_keywords'])
                blank['study']['vdjserver_keywords'] = study['vdjserver_keywords'];

            for (var i in models) {
                var model = models[i].value;
                model['study'] = blank['study']
                repertoireMetadata.push(model);
            }

            // get subject objects
            return tapisIO.getMetadataForType(ServiceAccount.accessToken(), projectUuid, 'subject');
        })
        .then(function(models) {
            for (var i in models) {
                subjectMetadata[models[i].uuid] = models[i].value;
            }

            // get sample processing objects
            return tapisIO.getMetadataForType(ServiceAccount.accessToken(), projectUuid, 'sample_processing');
        })
        .then(function(models) {
            for (var i in models) {
                sampleMetadata[models[i].uuid] = models[i].value;
            }

            // get data processing objects
            return tapisIO.getMetadataForType(ServiceAccount.accessToken(), projectUuid, 'data_processing');
        })
        .then(function(models) {
            for (var i in models) {
                dpMetadata[models[i].uuid] = models[i].value;
            }
        })
        .then(function() {
            // put into AIRR format
            for (var i in repertoireMetadata) {
                var rep = repertoireMetadata[i];
                var subject = subjectMetadata[rep['subject']['vdjserver_uuid']];
                if (! subject) {
                    console.error('VDJ-API ERROR: tapisIO.gatherRepertoireMetadataForProject, cannot collect subject: '
                                  + rep['subject']['vdjserver_uuid'] + ' for repertoire: ' + rep['repertoire_id']);
                }
                if (keep_uuids) subject['vdjserver_uuid'] = rep['subject']['vdjserver_uuid'];
                rep['subject'] = subject;

                var samples = [];
                for (var j in rep['sample']) {
                    var sample = sampleMetadata[rep['sample'][j]['vdjserver_uuid']];
                    if (! sample) {
                        console.error('VDJ-API ERROR: tapisIO.gatherRepertoireMetadataForProject, cannot collect sample: '
                                      + rep['sample'][j]['vdjserver_uuid'] + ' for repertoire: ' + rep['repertoire_id']);
                    }
                    if (keep_uuids) sample['vdjserver_uuid'] = rep['sample'][j]['vdjserver_uuid'];
                    samples.push(sample);
                }
                rep['sample'] = samples;

                var dps = [];
                for (var j in rep['data_processing']) {
                    var dp = dpMetadata[rep['data_processing'][j]['vdjserver_uuid']];
                    if (! dp) {
                        console.error('VDJ-API ERROR: tapisIO.gatherRepertoireMetadataForProject, cannot collect data_processing: '
                                      + rep['data_processing'][j]['vdjserver_uuid'] + ' for repertoire: ' + rep['repertoire_id']);
                    }
                    if (keep_uuids) dp['vdjserver_uuid'] = rep['data_processing'][j]['vdjserver_uuid'];
                    dps.push(dp);
                }
                rep['data_processing'] = dps;
            }

            return repertoireMetadata;
        });
};
*/

// set permissions on a metadata object
tapisIO.addMetadataPermissionsForProjectUsers = function(projectUuid, metadataUuid) {

    return ServiceAccount.getToken()
        .then(function(token) {
            return tapisIO.getMetadataPermissions(ServiceAccount.accessToken(), projectUuid);
        })
        .then(function(projectPermissions) {
            var metadataPermissions = new MetadataPermissions();

            var projectUsernames = metadataPermissions.getUsernamesFromMetadataResponse(projectPermissions);

            var promises = [];
            for (var i = 0; i < projectUsernames.length; i++) {
                var username = projectUsernames[i];
                promises[i] = tapisIO.addUsernameToMetadataPermissions(
                    username,
                    ServiceAccount.accessToken(),
                    metadataUuid
                );
            }

            return Promise.allSettled(promises);
        })
        .then(function() {
            return Promise.resolve();
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

tapisIO.createMetadataForTypeWithPermissions = function(projectUuid, type, value) {
    var object = null;
    return tapisIO.createMetadataForType(projectUuid, type, value)
        .then(function(_obj) {
            object = _obj;
            return tapisIO.addMetadataPermissionsForProjectUsers(projectUuid, object['uuid']);
        })
        .then(function() {
            return Promise.resolve(object);
        });
};

// delete all metadata for type for a project
tapisIO.deleteAllMetadataForType = function(projectUuid, type) {

    return ServiceAccount.getToken()
        .then(function(token) {
            return tapisIO.getMetadataForType(ServiceAccount.accessToken(), projectUuid, type);
        })
        .then(function(metadataList) {

            console.log('VDJ-API INFO: tapisIO.deleteAllMetadataForType - deleting ' + metadataList.length + ' metadata entries for type: ' + type);
            var promises = [];
            for (var i = 0; i < metadataList.length; i++) {
                var metadata = metadataList[i];
                promises[i] = tapisIO.deleteMetadata(ServiceAccount.accessToken(), metadata.uuid);
            }

            return Promise.allSettled(promises);
        })
        .then(function() {
            return Promise.resolve();
        })
        .catch(function(errorObject) {
            return Promise.reject(errorObject);
        });
};

//
/////////////////////////////////////////////////////////////////////
//
// Postit operations
//

tapisIO.createPublicFilePostit = function(url, unlimited, maxUses, lifetime) {

    var postData = {
        url: url,
        method: 'GET'
    };
    if (unlimited) {
        postData["unlimited"] = true;
    } else {
        postData["maxUses"] = maxUses;
        postData["lifetime"] = lifetime;
    }
    postData = JSON.stringify(postData);

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'POST',
                path:     '/postits/v2/',
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

tapisIO.getPostit = function(uuid) {

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/postits/v2/listing/' + uuid,
                rejectUnauthorized: false,
                headers: {
                    'Content-Type':   'application/json',
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

tapisIO.deletePostit = function(postit_id) {

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'DELETE',
                path:     '/postits/v2/' + postit_id,
                rejectUnauthorized: false,
                headers: {
                    'Content-Type':   'application/json',
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
    //console.log(postData);

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

            //console.log(requestSettings);

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
// Project load/unload from VDJServer ADC data repository
//

//
// Right now, all the project load/unload metadata is owned by the
// vdj account, no permissions for project users are given.
//

tapisIO.createProjectLoadMetadata = function(projectUuid, collection) {

    var postData = {
        name: 'projectLoad',
        associationIds: [ projectUuid ],
        value: {
            collection: collection,
            shouldLoad: true,
            isLoaded: false,
            repertoireMetadataLoaded: false,
            rearrangementDataLoaded: false
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

// there should be only a single metadata record
tapisIO.getProjectLoadMetadata = function(projectUuid, collection) {

    return ServiceAccount.getToken()
        .then(function(token) {
            var requestSettings = {
                host:     tapisSettings.hostname,
                method:   'GET',
                path:     '/meta/v2/data?q='
                    + encodeURIComponent(
                        '{'
                            + '"name":"projectLoad",'
                            + '"value.collection":"' + collection + '",'
                            + '"associationIds":"' + projectUuid + '"'
                            + '}'
                    )
                    + '&limit=1',
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

// query project load records
tapisIO.queryProjectLoadMetadata = function(projectUuid, collection, shouldLoad, isLoaded, repertoireMetadataLoaded, rearrangementDataLoaded) {

    var query = '{"name":"projectLoad"';
    if (projectUuid) query += ',"associationIds":"' + projectUuid + '"';
    if (collection) query += ',"value.collection":"' + collection + '"';
    if (shouldLoad === false) query += ',"value.shouldLoad":false';
    else if (shouldLoad === true) query += ',"value.shouldLoad":true';
    if (isLoaded === false) query += ',"value.isLoaded":false';
    else if (isLoaded === true) query += ',"value.isLoaded":true';
    if (repertoireMetadataLoaded === false) query += ',"value.repertoireMetadataLoaded":false';
    else if (repertoireMetadataLoaded === true) query += ',"value.repertoireMetadataLoaded":true';
    if (rearrangementDataLoaded === false) query += ',"value.rearrangementDataLoaded":false';
    else if (rearrangementDataLoaded === true) query += ',"value.rearrangementDataLoaded":true';
    query += '}';

    var models = [];

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

// get list of projects to be loaded
tapisIO.getProjectsToBeLoaded = function(collection) {

    var models = [];

    var doFetch = function(offset) {
        return ServiceAccount.getToken()
            .then(function(token) {
                var requestSettings = {
                    host:     tapisSettings.hostname,
                    method:   'GET',
                    path:     '/meta/v2/data?q='
                        + encodeURIComponent('{"name":"projectLoad","value.collection":"' + collection +  '","value.shouldLoad":true,"value.isLoaded":false}')
                        + '&limit=50&offset=' + offset,
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

// status record for a rearrangement load
tapisIO.createRearrangementLoadMetadata = function(projectUuid, repertoire_id, collection) {

    var postData = {
        name: 'rearrangementLoad',
        associationIds: [ projectUuid ],
        value: {
            repertoire_id: repertoire_id,
            collection: collection,
            isLoaded: false,
            load_set: 0
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

// get list of repertoires that need their rearrangement data to be loaded
tapisIO.getRearrangementsToBeLoaded = function(projectUuid, collection) {

    var models = [];

    var doFetch = function(offset) {
        return ServiceAccount.getToken()
            .then(function(token) {
                var requestSettings = {
                    host:     tapisSettings.hostname,
                    method:   'GET',
                    path:     '/meta/v2/data?q='
                        + encodeURIComponent(
                            '{'
                                + '"name":"rearrangementLoad",'
                                + '"value.collection":"' + collection + '",'
                                + '"associationIds":"' + projectUuid + '"'
                                + '}'
                        )
                        + '&limit=50&offset=' + offset,
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
// Statistics cache functions
//

// Statistics cache status
// this should be a singleton metadata entry owned by service account
tapisIO.createStatisticsCache = function() {
    //if (tapisSettings.shouldInjectError("tapisIO.createStatisticsCache")) return tapisSettings.performInjectError();

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
    //if (tapisSettings.shouldInjectError("tapisIO.getStatisticsCache")) return tapisSettings.performInjectError();

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
    //if (tapisSettings.shouldInjectError("tapisIO.createADCDownloadCache")) return tapisSettings.performInjectError();

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
    //if (tapisSettings.shouldInjectError("tapisIO.getADCDownloadCache")) return tapisSettings.performInjectError();

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
