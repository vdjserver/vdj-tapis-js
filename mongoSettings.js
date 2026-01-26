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

// ADC DB, use tapisSettings for Tapis DB
var mongoSettings = {
    // MongoDB Settings

    // ADC query
    queryHost: process.env.MONGODB_QUERY_HOST,
    queryDatabase: process.env.MONGODB_QUERY_DB,
    queryUsername: process.env.MONGODB_QUERY_USER,
    queryUserSecret: process.env.MONGODB_QUERY_SECRET,
    queryCollection: process.env.MONGODB_QUERY_COLLECTION,

    // ADC load
    loadHost: process.env.MONGODB_LOAD_HOST,
    loadDatabase: process.env.MONGODB_LOAD_DB,
    loadUsername: process.env.MONGODB_QUERY_USER,
    loadUserSecret: process.env.MONGODB_QUERY_SECRET,
    loadCollection: process.env.MONGODB_LOAD_COLLECTION,

    queryTimeout: Number(process.env.MONGODB_QUERY_TIMEOUT),

    // constructed connect url
    query_url: null,
    load_url: null
};

module.exports = mongoSettings;

mongoSettings.set_config = function(config) {
    var context = 'mongoSettings';

    if (config) {
        config.log.info(context, 'config object set for app: ' + config.name, true);
        mongoSettings.config = config;
    }

    config.log.info(context, 'Using query host: ' + mongoSettings.queryHost, true);
    config.log.info(context, 'Using query database: ' + mongoSettings.queryDatabase, true);
    config.log.info(context, 'Using query username: ' + mongoSettings.queryUsername, true);
    config.log.info(context, 'Using query collection: ' + mongoSettings.queryCollection, true);

    config.log.info(context, 'Using load host: ' + mongoSettings.loadHost, true);
    config.log.info(context, 'Using load database: ' + mongoSettings.loadDatabase, true);
    config.log.info(context, 'Using load username: ' + mongoSettings.loadUsername, true);
    config.log.info(context, 'Using load collection: ' + mongoSettings.loadCollection, true);

    config.log.info(context, 'Using DB timeout: ' + mongoSettings.queryTimeout, true);

    // query DB
    if (mongoSettings.queryUsername) {
        mongoSettings.query_url = 'mongodb://'
            + mongoSettings.queryUsername + ':' + mongoSettings.queryUserSecret + '@'
            + mongoSettings.queryHost + ':27017/' + mongoSettings.queryDatabase;
    } else {
        mongoSettings.query_url = 'mongodb://'
            + mongoSettings.queryHost + ':27017/' + mongoSettings.queryDatabase;
    }

    // load DB
    if (mongoSettings.queryUsername) {
        mongoSettings.load_url = 'mongodb://'
            + mongoSettings.loadUsername + ':' + mongoSettings.loadUserSecret + '@'
            + mongoSettings.loadHost + ':27017/' + mongoSettings.loadDatabase;
    } else {
        mongoSettings.load_url = 'mongodb://'
            + mongoSettings.loadHost + ':27017/' + mongoSettings.loadDatabase;
    }

    return mongoSettings;
}
