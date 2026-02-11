
'use strict';

//
// pgIO.js
// Functions for direct access to Postgresql
//
// These functions should be relatively agnostic to the application.
//
// VDJServer Analysis Portal
// VDJ API Service
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

var pgIO  = {};
module.exports = pgIO;

// Server environment config
var pgSettings = require('./pgSettings');

// Tapis
var tapisSettings = require('vdj-tapis-js/tapisSettings');
var tapisIO = tapisSettings.get_default_tapis();
var config = tapisSettings.config;
var ServiceAccount = tapisIO.serviceAccount;
var GuestAccount = tapisIO.guestAccount;
var webhookIO = require('vdj-tapis-js/webhookIO');

// Node Libraries
var _ = require('underscore');
var postgres = require('postgres');
var csv = require('csv-parser');
var fs = require('fs');
const zlib = require('zlib');

// get connection
pgIO.getConnection = function() {

    const sql = postgres(pgSettings.url);

    return sql;
}

// test connection
pgIO.testConnection = async function() {

    try {
	const sql = pgIO.getConnection();
	const users = await sql`
        select * from pg_user
        `
	console.log(users);
	return Promise.resolve();
    } catch (e) {
	console.error(e);
	return Promise.reject();
    }
}
