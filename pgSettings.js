'use strict';

//
// pgSettings.js
// Postgresql configuration settings
// This is shared between VDJServer and AIRR Knowledge
//
// VDJServer Community Data Portal
// ADC API for VDJServer
// https://vdjserver.org
//
// AIRR Knowledge
// AK API
// https://airr-knowledge.org
//
// Copyright (C) 2026 The University of Texas Southwestern Medical Center
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

// Postgresql Settings
var pgSettings = {
    // AIRR Knowledge
    // ADC API query
    hostname: process.env.POSTGRES_HOST,
    port: process.env.POSTGRES_PORT,
    dbname: process.env.POSTGRES_DB,
    username: process.env.POSTGRES_USER,
    userSecret: process.env.POSTGRES_PASSWORD,
    url: null,

    // AIRR Knowledge
    query_timeout: Number(process.env.POSTGRES_QUERY_TIMEOUT),
    max_results: 10000,
    download_timeout: 600000,
    max_download_results: 1000000,

    // ADC API
    // load database
    load_hostname: process.env.POSTGRES_LOAD_HOST,
    load_port: process.env.POSTGRES_LOAD_PORT,
    load_dbname: process.env.POSTGRES_LOAD_DB,
    load_username: process.env.POSTGRES_LOAD_USER,
    load_userSecret: process.env.POSTGRES_LOAD_PASSWORD,
    load_url: null
};

module.exports = pgSettings;

pgSettings.set_config = function(config) {
    var context = 'postgres';

    if (!pgSettings.query_timeout) pgSettings.query_timeout = 180000; // 3 min default

    if (config) {
        config.log.info(context, 'pgSettings config object set for app: ' + config.name, true);
        pgSettings.config = config;
        config.info.query_timeout = pgSettings.query_timeout;
        config.info.max_results = pgSettings.max_results;
        config.info.download_timeout = pgSettings.download_timeout;
        config.info.max_download_results = pgSettings.max_download_results;
    }

    if (!pgSettings.port) pgSettings.port = 5432;
    if (pgSettings.username) {
        pgSettings.url = 'postgres://'
            + pgSettings.username + ':' + pgSettings.userSecret + '@'
            + pgSettings.hostname + ':' + pgSettings.port + '/' + pgSettings.dbname;
    } else {
        pgSettings.url = 'postgres://'
            + pgSettings.hostname + ':' + pgSettings.port + '/' + pgSettings.dbname;
    }

    config.log.info(context, 'Using Postgres host: ' + pgSettings.hostname, true);
    config.log.info(context, 'Using Postgres port: ' + pgSettings.port, true);
    config.log.info(context, 'Using Postgres DB: ' + pgSettings.dbname, true);
    config.log.info(context, 'Using Postgres username: ' + pgSettings.username, true);

    if (pgSettings.load_dbname) {
        if (!pgSettings.load_port) pgSettings.load_port = 5432;
        if (pgSettings.load_username) {
            pgSettings.load_url = 'postgres://'
                + pgSettings.load_username + ':' + pgSettings.load_userSecret + '@'
                + pgSettings.load_hostname + ':' + pgSettings.load_port + '/' + pgSettings.load_dbname;
        } else {
            pgSettings.load_url = 'postgres://'
                + pgSettings.load_hostname + ':' + pgSettings.load_port + '/' + pgSettings.load_dbname;
        }

        config.log.info(context, 'Using Postgres LOAD host: ' + pgSettings.hostname, true);
        config.log.info(context, 'Using Postgres LOAD port: ' + pgSettings.port, true);
        config.log.info(context, 'Using Postgres LOAD DB: ' + pgSettings.dbname, true);
        config.log.info(context, 'Using Postgres LOAD username: ' + pgSettings.username, true);
    } else {
        config.log.info(context, 'Postgres LOAD database not defined.');
    }

    return pgSettings;
}

pgSettings.pg_connection = function() {
    return {
        user: pgSettings.username,
        host: pgSettings.hostname,
        database: pgSettings.dbname,
        password: pgSettings.userSecret,
        port: pgSettings.port,
        statement_timeout: pgSettings.query_timeout
    };
}

pgSettings.pg_download_connection = function() {
    return {
        user: pgSettings.username,
        host: pgSettings.hostname,
        database: pgSettings.dbname,
        password: pgSettings.userSecret,
        port: pgSettings.port,
        statement_timeout: pgSettings.download_timeout
    };
}
