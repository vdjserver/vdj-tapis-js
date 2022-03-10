
'use strict';

//
// authController.js
// Handle security and authorization checks
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

var AuthController = {};
module.exports = AuthController;

// Processing
var tapisIO = require('./tapis');
var ServiceAccount = tapisIO.serviceAccount;

// Extract token from header
AuthController.extractToken = function(req) {
    // extract the token from the authorization header
    if (! req['headers']['authorization']) {
        var msg = 'TAPIS-API ERROR: AuthController.userAuthorization - missing authorization header';
        console.error(msg);
        return false;
    }
    var fields = req['headers']['authorization'].split(' ');
    if (fields.length != 2) {
        let msg = 'TAPIS-API ERROR: AuthController.userAuthorization - invalid authorization header: ' + req['headers']['authorization'];
        console.error(msg);
        return false;
    }
    if (fields[0].toLowerCase() != 'bearer') {
        let msg = 'TAPIS-API ERROR: AuthController.userAuthorization - invalid authorization header: ' + req['headers']['authorization'];
        console.error(msg);
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
    var token = AuthController.extractToken(req);
    if (!token) return false;

    // get my profile and username from the token
    // return a promise
    return tapisIO.getAgaveUserProfile(token, 'me')
        .then(function(userProfile) {
            // save the user profile
            req['user'] = userProfile;

            // now check that the user account has been verified
            return tapisIO.getUserVerificationMetadata(req['user']['username']);
        })
        .then(function(userVerificationMetadata) {
            if (userVerificationMetadata && userVerificationMetadata[0] && userVerificationMetadata[0].value.isVerified === true) {
                // valid
                return true;
            }
            else {
                var msg = 'TAPIS-API ERROR: AuthController.userAuthorization - access by unverified user: ' + req['user']['username'];
                console.error(msg);
                return false;
            }
        })
        .catch(function(error) {
            var msg = 'TAPIS-API ERROR: AuthController.userAuthorization - invalid token: ' + token + ', error: ' + error;
            console.error(msg);
            return false;
        });
}

// Requires the user account to have admin privileges.
// Currently, only the service account has that.
AuthController.adminAuthorization = function(req, scopes, definition) {
    var token = AuthController.extractToken(req);
    if (!token) return false;

    // get my profile and username from the token
    // return a promise
    return tapisIO.getAgaveUserProfile(token, 'me')
        .then(function(userProfile) {
            // save the user profile
            req['user'] = userProfile;

            if (userProfile.username == ServiceAccount.username) {
                // valid
                return true;
            }
            else {
                var msg = 'TAPIS-API ERROR: AuthController.adminAuthorization - access by unauthorized user: ' + req['user']['username']
                    + ', route: ' + JSON.stringify(req.route.path);
                console.error(msg);
                return false;
            }
        })
        .catch(function(error) {
            var msg = 'TAPIS-API ERROR: AuthController.adminAuthorization - invalid token: ' + token + ', error: ' + error;
            console.error(msg);
            return false;
        });
}

// Verify a user has access to project
AuthController.projectAuthorization = function(req, scopes, definition) {
    var token = AuthController.extractToken(req);
    if (!token) return false;

    // check body and params for project uuid
    var project_uuid;
    if (req.body) project_uuid = req.body.project_uuid;
    if (project_uuid == undefined)
        if (req.params) project_uuid = req.params.project_uuid;
    if (project_uuid == undefined) {
        var msg = 'TAPIS-API ERROR: AuthController.authForProject - missing project uuid, route ' + JSON.stringify(req.route.path);
        console.error(msg);
        return false;
    }

    // verify the user token
    // return a promise
    return AuthController.userAuthorization(req, scopes, definition)
        .then(function(result) {
            if (!result) return result;

            // verify the user has access to project
            return tapisIO.getProjectMetadata(token, project_uuid);
        })
        .then(function(projectMetadata) {
            // make sure its project metadata and not some random uuid
            // TODO: should disallow old VDJServer V1 projects at some point
            if (projectMetadata && (projectMetadata.name == 'private_project') || (projectMetadata.name == 'public_project') || (projectMetadata.name == 'project')) {
                return tapisIO.getMetadataPermissionsForUser(token, project_uuid, req['user']['username']);
            }
            else {
                return Promise.reject(new Error('invalid project metadata'));
            }
        })
        .then(function(projectPermissions) {
            // we can read the project metadata, but do we have write permission?
            if (projectPermissions && projectPermissions.permission.write)
                return true;
            else {
                return Promise.reject(new Error('user does not have write permission for project'));
            }
        })
        .catch(function(error) {
            var msg = 'TAPIS-API ERROR: AuthController.authForProject - project: ' + project_uuid + ', route: '
                + JSON.stringify(req.route.path) + ', error: ' + error;
            console.error(msg);
            return false;
        });
}

//
// verify a valid and active username account
//
AuthController.verifyUser = function(username) {

    if (username == undefined) return false;

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
            var msg = 'TAPIS-API ERROR: AuthController.verifyUser - error validating user: ' + username + ', error ' + error;
            console.error(msg);
            return false;
        })
        ;
}

//
// verify user has access to metadata entry
//
AuthController.verifyMetadataAccess = function(uuid, accessToken, username) {

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
            var msg = 'TAPIS-API ERROR: AuthController.verifyMetadataAccess - uuid: ' + uuid
                + ', error validating user: ' + username + ', error ' + error;
            console.error(msg);
            return false;
        });
}

