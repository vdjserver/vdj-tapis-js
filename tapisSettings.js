'use strict';

//
// tapisSettings.js
// Tapis configuration settings
//
// VDJServer
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

function parseBoolean(value)
{
    if (value == 'true') return true;
    else if (value == 1) return true;
    else return false;
}

var tapisSettings = {
    // Tapis V3 Auth Settings
    clientKeyV3:    process.env.TAPIS_V3_CLIENT_KEY,
    clientSecretV3: process.env.TAPIS_V3_CLIENT_SECRET,
    hostnameV3:     process.env.TAPIS_V3_HOST,

    // WSO2 Auth Settings (Tapis V2)
    clientKey:    process.env.WSO2_CLIENT_KEY,
    clientSecret: process.env.WSO2_CLIENT_SECRET,
    hostname:     process.env.WSO2_HOST,

    // VDJ Service Account User
    serviceAccountKey: process.env.VDJ_SERVICE_ACCOUNT,
    serviceAccountSecret: process.env.VDJ_SERVICE_ACCOUNT_SECRET,

    // VDJ Guest Account User
    guestAccountKey: process.env.VDJ_GUEST_ACCOUNT,
    guestAccountSecret: process.env.VDJ_GUEST_ACCOUNT_SECRET,

    // VDJ Backbone Location
    vdjBackbone: process.env.VDJ_BACKBONE_HOST,

    // Agave Misc.
    storageSystem: process.env.AGAVE_STORAGE_SYSTEM,

    // host URL for Tapis notifications
    notifyHost: process.env.AGAVE_NOTIFY_HOST,

    // Email
    fromAddress: process.env.EMAIL_ADDRESS,
    replyToAddress: process.env.REPLYTO_EMAIL_ADDRESS,

    // Debug
    debugConsole: parseBoolean(process.env.DEBUG_CONSOLE),

    // Mongodb, meta/v3 settings
    mongo_hostname: process.env.MONGODB_HOST,
    mongo_dbname: process.env.MONGODB_DB,
    mongo_username: process.env.MONGODB_USER,
    mongo_userSecret: process.env.MONGODB_SECRET,
    mongo_queryCollection: process.env.MONGODB_QUERY_COLLECTION,
    mongo_loadCollection: process.env.MONGODB_LOAD_COLLECTION,
    // max pagesize
    max_size: 1000,
    // to deterimine if GET or POST is used for query
    large_query_size: 2 * 1024,
    large_lrq_query_size: 50 * 1024
};
module.exports = tapisSettings;

// TODO: not implemented
// Error injection enabled
if (tapisSettings.errorInjection) {
    global.errorInjection = require('./errorInjection');
    tapisSettings.performInjectError = function() {
        return global.errorInjection.performInjectError();
    };
}
tapisSettings.injectError = function(error) {
    if (tapisSettings.errorInjection) return global.errorInjection.setCurrentError(error);
    else return null;
};
tapisSettings.shouldInjectError = function(value) {
    if (tapisSettings.errorInjection) return global.errorInjection.shouldInjectError(value);
    else return false;
};
