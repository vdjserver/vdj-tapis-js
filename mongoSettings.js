'use strict';

//
// mongoSettings.js
// Application configuration settings
//
// VDJServer Community Data Portal
// ADC API for VDJServer
// https://vdjserver.org
//
// Copyright (C) 2020-2025 The University of Texas Southwestern Medical Center
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

// local DB, use tapisSettings for Tapis DB
var mongoSettings = {
    // MongoDB Settings
    // old settings
    hostname: process.env.MONGODB_HOST,
    dbname: process.env.MONGODB_DB,
    username: process.env.MONGODB_USER,
    userSecret: process.env.MONGODB_SECRET,

    hostMode: process.env.MONGODB_HOST_MODE,
    queryHost: process.env.MONGODB_QUERY_HOST,
    queryDatabase: process.env.MONGODB_QUERY_DB,
    queryCollection: process.env.MONGODB_QUERY_COLLECTION,

    loadHost: process.env.MONGODB_LOAD_HOST,
    loadDatabase: process.env.MONGODB_LOAD_DB,
    loadCollection: process.env.MONGODB_LOAD_COLLECTION,

    queryTimeout: Number(process.env.MONGODB_QUERY_TIMEOUT),

    // constructed connect url
    url: null
};

module.exports = mongoSettings;

mongoSettings.set_config = function(config) {
    var context = 'mongo';

    if (config) {
        config.log.info(context, 'config object set for app: ' + config.name);
        mongoSettings.config = config;
    }

    config.log.info(context, 'Using DB: ' + mongoSettings.dbname);

    config.log.info(context, 'Host mode: ' + mongoSettings.hostMode);

    config.log.info(context, 'Using query host: ' + mongoSettings.queryHost);
    config.log.info(context, 'Using query database: ' + mongoSettings.queryDatabase);
    config.log.info(context, 'Using query collection: ' + mongoSettings.queryCollection);

    config.log.info(context, 'Using load host: ' + mongoSettings.loadHost);
    config.log.info(context, 'Using load database: ' + mongoSettings.loadDatabase);
    config.log.info(context, 'Using load collection: ' + mongoSettings.loadCollection);

    config.log.info(context, 'Using DB timeout: ' + mongoSettings.queryTimeout);

    if (mongoSettings.hostMode == 'load') {
        config.log.info(context, 'ADC Repository is in load mode.');

        if (mongoSettings.username) {
            mongoSettings.url = 'mongodb://'
                + mongoSettings.username + ':' + mongoSettings.userSecret + '@'
                + mongoSettings.loadHost + ':27017/' + mongoSettings.loadDatabase;
        } else {
            mongoSettings.url = 'mongodb://'
                + mongoSettings.loadHost + ':27017/' + mongoSettings.loadDatabase;
        }
    } else {
        // otherwise, default to query
        config.log.info(context, 'ADC Repository is in query mode.');

        if (mongoSettings.username) {
            mongoSettings.url = 'mongodb://'
                + mongoSettings.username + ':' + mongoSettings.userSecret + '@'
                + mongoSettings.queryHost + ':27017/' + mongoSettings.queryDatabase;
        } else {
            mongoSettings.url = 'mongodb://'
                + mongoSettings.queryHost + ':27017/' + mongoSettings.queryDatabase;
        }

    }

    return mongoSettings;
}
