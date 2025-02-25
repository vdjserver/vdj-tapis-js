'use strict';

//
// serviceAccount.js
// Tapis service account
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

var tapisSettings = require('./tapisSettings');
var ServiceAccountV3 = {
    username: tapisSettings.serviceAccountKey,
    password: tapisSettings.serviceAccountSecret,
    tapisToken: null,
    created: null
};

module.exports = ServiceAccountV3;

// Processing
var TapisTokenV3 = require('./tapisTokenV3');
var tapisV3 = require('./tapisV3');

ServiceAccountV3.getToken = function() {

    var that = this;

    if (tapisSettings.serviceAccountJWT) {
        // if long-lived token is provided, use that
        that.tapisToken = { "access_token": tapisSettings.serviceAccountJWT };
        return Promise.resolve(that.tapisToken);
    } else {
        return tapisV3.getToken(this)
            .then(function(responseObject) {
                that.tapisToken = new TapisTokenV3(responseObject.access_token);
                return Promise.resolve(that.tapisToken);
            })
            .catch(function(errorObject) {
                console.error('TAPIS-V3-API ERROR: Unable to login with service account. ' + errorObject);
                return Promise.reject(errorObject);
            });
    }
}

ServiceAccountV3.accessToken = function() {
    return this.tapisToken.access_token;
}
