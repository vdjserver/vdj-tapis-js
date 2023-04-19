
'use strict';

//
// metadataPermissions.js
//
// VDJServer Analysis Portal
// VDJ API Service
// https://vdjserver.org
//
// Copyright (C) 2020-2023 The University of Texas Southwestern Medical Center
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

var _ = require('underscore');

var MetadataPermissions = function(attributes) {

    if (!attributes) {
        attributes = {};
    }

    this.username = attributes.username || '';
};

MetadataPermissions.prototype.getUuidsFromMetadataResponse = function(metadata) {

    var uuids = [];

    for (var i = 0; i < metadata.length; i++) {
        uuids.push(metadata[i].uuid);
    }

    return uuids;
};

MetadataPermissions.prototype.getJobUuidsFromMetadataResponse = function(metadata) {

    var uuids = [];

    for (var i = 0; i < metadata.length; i++) {
        uuids.push(metadata[i].value.jobUuid);
    }

    return uuids;
};

MetadataPermissions.prototype.getJobUuidsFromProjectResponse = function(metadata) {

    var uuids = [];

    for (var i = 0; i < metadata.length; i++) {
        uuids.push(metadata[i].id);
    }

    return uuids;
};

MetadataPermissions.prototype.getUsernamesFromMetadataResponse = function(metadata) {

    var usernames = [];

    for (var i = 0; i < metadata.length; i++) {
        usernames.push(metadata[i].username);
    }

    usernames = _.without(usernames, 'vdj');

    return usernames;
};

module.exports = MetadataPermissions;
