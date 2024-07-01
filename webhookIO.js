'use strict';

//
// webhookIO.js
// Post messages to Slack
//
// VDJServer Community Data Portal
// Statistics API service
// https://vdjserver.org
//
// Copyright (C) 2021 The University of Texas Southwestern Medical Center
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

var moment = require('moment-timezone');
var request = require('request');

var webhookIO = {};
module.exports = webhookIO;

webhookIO.environment = 'VDJServer TAPIS API';

webhookIO.postToSlack = function(eventMessage) {

    if (!process.env.SLACK_WEBHOOK_URL) return;

    request({
        url: process.env.SLACK_WEBHOOK_URL,
        json: {
            text: 'Event: ' + eventMessage + '\n'
                  + 'Environment: ' + webhookIO.environment + '\n'
                  + 'Timestamp: ' + moment().tz('America/Chicago').format()
                  ,
            username: 'VDJ Telemetry Bot',
        },
        method: 'POST',
    },
    function(requestError, response, body) {
        console.log('Posted slack webhook for message: "' + eventMessage + '"');
    })
    ;
};
