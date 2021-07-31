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
var ServiceAccount = {
    username: tapisSettings.serviceAccountKey,
    password: tapisSettings.serviceAccountSecret,
    tapisToken: null
};

module.exports = ServiceAccount;

// Processing
var TapisToken = require('./tapisToken');
var tapisIO = require('./tapis');

ServiceAccount.getToken = function() {

    var that = this;

    return tapisIO.getToken(this)
        .then(function(responseObject) {
            that.tapisToken = new TapisToken(responseObject);
            return Promise.resolve(that.tapisToken);
        })
        .catch(function(errorObject) {
            console.error('TAPIS-API ERROR: Unable to login with service account. ' + errorObject);
            return Promise.reject(errorObject);
        });
}

ServiceAccount.accessToken = function() {
    return this.tapisToken.access_token;
}
