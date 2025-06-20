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

var webhookIO = {};
module.exports = webhookIO;

var tapisSettings = require('./tapisSettings');
var tapisIO = tapisSettings.get_default_tapis();
var config = tapisSettings.config;

var moment = require('moment-timezone');
const axios = require('axios');

webhookIO.postToSlack = async function(eventMessage) {

    if (!process.env.SLACK_WEBHOOK_URL) return;

    var postData = {
        text: 'Event: ' + eventMessage + '\n'
              + 'Environment: ' + tapisSettings.vdjBackbone + '\n'
              + 'Timestamp: ' + moment().tz('America/Chicago').format()
              ,
        username: 'VDJ Telemetry Bot',
    };

    var requestSettings = {
        url: process.env.SLACK_WEBHOOK_URL,
        method: 'POST',
        data: postData,
        headers: {
            'Content-Type':   'application/json'
        }
    };

    var response = await axios(requestSettings)
        .catch(function(error) {
            var msg = 'Failed to send slack message: ' + JSON.stringify(error);
            return Promise.reject(new Error(msg));
        });

    console.log('Posted slack webhook for message: "' + eventMessage + '"');

    return Promise.resolve(response.data);
};
