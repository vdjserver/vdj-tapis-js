
'use strict';

//
// authControllerV3.js
// Handle security and authorization checks
// Tapis V3
//
// VDJServer Analysis Portal
// VDJ Web API service
// https://vdjserver.org
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

var AuthController = {};
module.exports = AuthController;

// Processing
var tapisIO = require('./tapisV3');
var tapisSettings = tapisIO.tapisSettings;
var ServiceAccount = tapisIO.serviceAccount;
var webhookIO = require('./webhookIO');

// attach config object for reporting errors
AuthController.set_config = function(config) {
    if (config) {
        console.log('vdj-tapis (AuthController) config object set for app: ' + config.name);
    }
    AuthController.config = config;
    return AuthController;
};

// Extract token from header
AuthController.extractToken = function(req) {
    const context = 'AuthController.extractToken';
    //config.log.info(context, req['headers']);

    // extract the token from the authorization header
    if (! req['headers']['authorization']) {
        let msg = 'missing authorization header';
        msg = AuthController.config.log.error(context, msg);
        webhookIO.postToSlack(msg);
        return false;
    }
    var fields = req['headers']['authorization'].split(' ');
    if (fields.length != 2) {
        let msg = 'invalid authorization header: ' + req['headers']['authorization'];
        msg = AuthController.config.log.error(context, msg);
        webhookIO.postToSlack(msg);
        return false;
    }
    if (fields[0].toLowerCase() != 'bearer') {
        let msg = 'invalid authorization header: ' + req['headers']['authorization'];
        msg = AuthController.config.log.error(context, msg);
        webhookIO.postToSlack(msg);
        return false;
    }
    return fields[1];
}

//
// Security handlers, these are called by the openapi
// middleware. Return true if authentication is valid,
// otherwise return false. The middleware will throw
// a generic 401 error, which the errorMiddleware returns
// to the client
//

// Verify a Tapis token
// Sets the associated user profile for the token in req.user
AuthController.userAuthorization = function(req, scopes, definition) {
    const context = 'AuthController.userAuthorization';
    //config.log.info(context, 'start');

    var token = AuthController.extractToken(req);
    if (!token) return false;

    // get my profile and username from the token
    // return a promise
    return tapisIO.getTapisUserProfile(token, 'me')
        .then(function(userProfile) {
            //config.log.info(context, JSON.stringify(userProfile));
            // save the user profile
            req['user'] = userProfile['result'];

            // service account does not need the verification record
            if (req['user']['username'] == tapisSettings.serviceAccountKey) return true;

            // now check that the user account has been verified
            return tapisIO.getUserVerificationMetadata(req['user']['username'])
                .then(function(userVerificationMetadata) {
                    if (userVerificationMetadata && userVerificationMetadata[0] && userVerificationMetadata[0].value.isVerified === true) {
                        // valid
                        return true;
                    }
                    else {
                        var msg = 'access by unverified user: ' + req['user']['username'];
                        msg = AuthController.config.log.error(context, msg);
                        webhookIO.postToSlack(msg);
                        return false;
                    }
                });
        })
        .catch(function(error) {
            var msg = 'invalid token: ' + token + ', error: ' + error;
            msg = AuthController.config.log.error(context, msg);
            webhookIO.postToSlack(msg);
            return false;
        });
}

// Requires the user account to have admin privileges.
// Currently, only the service account has that.
AuthController.adminAuthorization = function(req, scopes, definition) {
    const context = 'AuthController.adminAuthorization';
    //config.log.info(context, 'start');

    var token = AuthController.extractToken(req);
    if (!token) return false;

    // get my profile and username from the token
    // return a promise
    return tapisIO.getTapisUserProfile(token, 'me')
        .then(function(userProfile) {
            // save the user profile
            req['user'] = userProfile['result'];

            if (req['user']['username'] == tapisSettings.serviceAccountKey) {
                // valid
                return true;
            }
            else {
                var msg = 'access by unauthorized user: ' + req['user']['username']
                    + ', route: ' + JSON.stringify(req.route.path);
                msg = AuthController.config.log.error(context, msg);
                webhookIO.postToSlack(msg);
                return false;
            }
        })
        .catch(function(error) {
            var msg = 'invalid token: ' + token + ', error: ' + error;
            msg = AuthController.config.log.error(context, msg);
            webhookIO.postToSlack(msg);
            return false;
        });
}

// Verify a user has access to project
AuthController.projectAuthorization = function(req, scopes, definition) {
    const context = 'AuthController.projectAuthorization';
    //config.log.info(context, 'start');

    var token = AuthController.extractToken(req);
    if (!token) return false;

    // check body and params for project uuid
    var project_uuid;
    if (req.body) project_uuid = req.body.project_uuid;
    if (project_uuid == undefined)
        if (req.params) project_uuid = req.params.project_uuid;
    if (project_uuid == undefined) {
        var msg = 'missing project uuid, route ' + JSON.stringify(req.route.path);
        msg = AuthController.config.log.error(context, msg);
        webhookIO.postToSlack(msg);
        return false;
    }

    // verify the user token
    // return a promise
    return AuthController.userAuthorization(req, scopes, definition)
        .then(function(result) {
            if (!result) return result;

            // verify the user has access to project
            return tapisIO.getProjectMetadata(req['user']['username'], project_uuid);
        })
        .then(function(projectMetadata) {
            //config.log.info(context, JSON.stringify(projectMetadata));

            // make sure its project metadata and not some random uuid
            // TODO: should disallow old VDJServer V1 projects at some point
            if (projectMetadata && (projectMetadata.length == 1) && ((projectMetadata[0].name == 'private_project') || (projectMetadata[0].name == 'public_project') || (projectMetadata[0].name == 'project') || (projectMetadata[0].name == 'archive_project'))) {
                // save the project metadata
                req['project_metadata'] = projectMetadata[0];
                return true;
            }
            else {
                return Promise.reject(new Error('invalid project metadata'));
            }
        })
        .catch(function(error) {
            var msg = 'project: ' + project_uuid + ', route: '
                + JSON.stringify(req.route.path) + ', error: ' + error;
            msg = AuthController.config.log.error(context, msg);
            webhookIO.postToSlack(msg);
            return false;
        });
}

//
// verify a valid and active username account
//
AuthController.verifyUser = function(username) {
    const context = 'AuthController.verifyUser';

    if (username == undefined) return false;
    if (!username) return false;

    // return a promise
    return tapisIO.getUserVerificationMetadata(username)
        .then(function(userVerificationMetadata) {
            if (userVerificationMetadata && userVerificationMetadata[0] && userVerificationMetadata[0].value.isVerified === true) {
                // valid
                return true;
            }
            else {
                return false;
            }
        })
        .catch(function(error) {
            var msg = 'error validating user: ' + username + ', error ' + error;
            msg = AuthController.config.log.error(context, msg);
            webhookIO.postToSlack(msg);
            return false;
        })
        ;
}

/* TODO: is this needed?
//
// verify user has access to metadata entry
//
AuthController.verifyMetadataAccess = function(uuid, accessToken, username) {
    const context = 'AuthController.verifyMetadataAccess';

    if (uuid == undefined) return false;
    if (accessToken == undefined) return false;
    if (username == undefined) return false;

    return tapisIO.getMetadataPermissionsForUser(accessToken, uuid, username)
        .then(function(metadataPermissions) {
            // we can read the metadata, but do we have write permission?
            if (metadataPermissions && metadataPermissions.permission.write)
                return true;
            else {
                return false;
            }
        })
        .catch(function(error) {
            var msg = 'uuid: ' + uuid
                + ', error validating user: ' + username + ', error ' + error;
            msg = AuthController.config.log.error(context, msg);
            webhookIO.postToSlack(msg);
            return false;
        });
}
*/
