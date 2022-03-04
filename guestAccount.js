'use strict';

//
// guestAccount.js
// guest account for performing queries
//
// VDJServer Community Data Portal
// ADC API for VDJServer
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

var tapisSettings = require('./tapisSettings');
var GuestAccount = {
    username: tapisSettings.guestAccountKey,
    password: tapisSettings.guestAccountSecret,
    tapisToken: null
};

module.exports = GuestAccount;

// Processing
var TapisToken = require('./tapisToken');
var tapisIO = require('./tapis');

GuestAccount.getToken = function() {

    var that = this;

    return tapisIO.getToken(this)
        .then(function(responseObject) {
            that.tapisToken = new TapisToken(responseObject);
            return Promise.resolve(that.tapisToken);
        })
        .catch(function(errorObject) {
            console.error('TAPIS-API ERROR: Unable to login with guest account. ' + errorObject);
            return Promise.reject(errorObject);
        });
}

GuestAccount.accessToken = function() {
    return this.tapisToken.access_token;
}
