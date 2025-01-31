'use strict';

//
// pgSettings.js
// Postgresql configuration settings
//
// VDJServer Community Data Portal
// ADC API for VDJServer
// https://vdjserver.org
//
// Copyright (C) 2024 The University of Texas Southwestern Medical Center
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
var pgSettings = {
    // Postgresql Settings
    hostname: process.env.MONGODB_HOST,
    dbname: process.env.MONGODB_DB,
    username: process.env.MONGODB_USER,
    userSecret: process.env.POSTGRES_PASSWORD,
    url: null
};

module.exports = pgSettings;

pgSettings.set_config = function(config) {
    var context = 'postgres';

    if (config) {
        config.log.info(context, 'config object set for app: ' + config.name);
        pgSettings.config = config;
    }

    config.log.info(context, 'Using DB: ' + pgSettings.dbname);
    
    if (pgSettings.username) {
        pgSettings.url = 'postgres://'
            + pgSettings.username + ':' + pgSettings.userSecret + '@'
            + pgSettings.hostname + ':5432/' + pgSettings.dbname;
    } else {
        pgSettings.url = 'postgres://'
            + pgSettings.hostname + ':5432/' + pgSettings.dbname;
    }

    return pgSettings;
}
