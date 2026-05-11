'use strict';

//
// airrkb_postgres_query.js
// Functions specific to AIRR Knowledge for Postgres.
//
// AIRR Knowledge
// https://airr-knowledge.org
//
// VDJServer
// https://vdjserver.org
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

var AKPostgresQuery = {};
module.exports = AKPostgresQuery;

// Server environment config
var pgSettings = require('./pgSettings');

// Tapis
var tapisSettings = require('vdj-tapis-js/tapisSettings');
var tapisIO = tapisSettings.get_default_tapis();
var ServiceAccount = tapisIO.serviceAccount;
var GuestAccount = tapisIO.guestAccount;
var authController = tapisIO.authController;
var webhookIO = require('vdj-tapis-js/webhookIO');

var config = tapisSettings.config;

var pgIO = require('vdj-tapis-js/pgIO');

AKPostgresQuery.formatField = function(name) {
    let format_name = null;
    let fields = name.split('.');
    if (fields.length == 1) return name;
    if ((fields[0] == 'tcr') && (fields[1] == 'receptor')) {
        if (fields.length == 4) {
            if (fields[2] == 'trb_chain') format_name = 'chb.' + fields[3];
            if (fields[2] == 'tra_chain') format_name = 'cha.' + fields[3];
            if (fields[2] == 'trg_chain') format_name = 'chg.' + fields[3];
            if (fields[2] == 'trd_chain') format_name = 'chd.' + fields[3];
        }
        if ((fields.length == 3) && (fields[2] == 'akc_id')) format_name = 't.' + fields[2];
    }
    if ((fields[0] == 'assay') && (fields.length > 1)) {
        const path = fields.splice(1).map(item => `'${item}'`).join(',');
        format_name = 'qa.assay_object #>> Array[' + path + ']';
    }
    return format_name;
}

// Construct postgres query based upon the AKC filters parameters. The
// filters parameter is a JSON object that can be any number of nested
// levels, so we recursively construct the query.

AKPostgresQuery.constructWhereClause = function(filter, error, values) {
    var context = 'AKPostgresQuery.constructQueryOperation';

    console.log(filter);

    // no filter is empty query
    if (!filter) {
       return 'TRUE';
    }

    if (!filter['op']) {
        error['message'] = 'missing op';
        return null;
    }
    if (!filter['content']) {
        error['message'] = 'missing content';
        return null;
    }

    let paramIndex = 1;

    var content = filter['content'];

    // TODO: do we need to handle value being an array when a single value is expected?
    // TODO: validate queryable field names?

    // determine type from schema, default is string
    var content_type = null;
    var content_properties = null;
    // if (content['field'] != undefined) {
    //     content_properties = airr.specForQueryField(schema, content['field']);
    //     if (content_properties) content_type = content_properties['type'];
    //     if (!content_properties) {
    //         config.log.info(context, content['field'] + ' is not found in AIRR schema.');

    //         // TODO: avoid symbols?
    //         var symbols = /${}/;
    //         if (symbols.test(content['field'])) {
    //             error['message'] = 'field name contains invalid symbols: ' + content['field'];
    //             config.log.error(context, 'HACK? field name contains invalid symbols: ' + content['field']);
    //         }
    //     } else content_type = content_properties['type'];

    //     // Check if query field is required. By default, the ADC API can reject
    //     // queries on the rearrangement endpoint for optional fields.
    //     if (check_query_support) {
    //         var support = false;
    //         if (content_properties != undefined) {
    //             if (content_properties['x-airr'] != undefined) {
    //                 if ((content_properties['x-airr']['adc-query-support'] != undefined) &&
    //                     (content_properties['x-airr']['adc-query-support'])) {
    //                     // need to support query
    //                     support = true;
    //                 }
    //             }
    //         }
    //         if (!support) {
    //             // optional field, reject
    //             config.log.info(context, content['field'] + ' is an optional query field.');
    //             error['message'] = "query not supported on field: " + content['field'];
    //             return null;
    //         }
    //     }
    // }
    //config.log.info(context, 'props: ' + content_properties);

    // if not in schema then maybe its a custom field
    // so use the same type as the value.
    if (!content_type) content_type = typeof content['value'];
    //config.log.info(context, 'type: ' + content_type);

    // verify the value type against the field type
    // stringify the value properly for the query
    var content_value = content['value'];
    // if (content['value'] != undefined) {
    //     if (content['value'] instanceof Array) {
    //         // we do not bother checking the types of array elements
    //         content_value = JSON.stringify(content['value']);
    //     } else {
    //         // if the field is an array
    //         // then check if items are basic type
    //         if (content_type == 'array') {
    //             if (content_properties && content_properties['items'] && content_properties['items']['type'])
    //                 content_type = content_properties['items']['type'];
    //         }

    //         switch(content_type) {
    //         case 'integer':
    //         case 'number':
    //             if (((typeof content['value']) != 'integer') && ((typeof content['value']) != 'number')) {
    //                 error['message'] = "value has wrong type '" + typeof content['value'] + "', should be integer or number.";
    //                 return null;
    //             }
    //             content_value = content['value'];
    //             break;
    //         case 'boolean':
    //             if ((typeof content['value']) != 'boolean') {
    //                 error['message'] = "value has wrong type '" + typeof content['value'] + "', should be boolean.";
    //                 return null;
    //             }
    //             content_value = content['value'];
    //             break;
    //         case 'string':
    //             if ((typeof content['value']) != 'string') {
    //                 error['message'] = "value has wrong type '" + typeof content['value'] + "', should be string.";
    //                 return null;
    //             }
    //             content_value = '"' + content['value'] + '"';
    //             break;
    //         default:
    //             error['message'] = "unsupported content type: " + content_type;
    //             return null;
    //         }
    //     }
    // }
    //config.log.info(context, 'value: ' + content_value);

    // query operators
    switch(filter['op']) {
    case '=':
        if (content['field'] == undefined) {
            error['message'] = 'missing field for = operator';
            return null;
        }
        if (content_value == undefined) {
            error['message'] = 'missing value for = operator';
            return null;
        }
        let content_field = AKPostgresQuery.formatField(content['field']);
        if (content_field == null) {
            error['message'] = 'invalid field: ' + content['field'];
            return null;
        }
        values.push(content_value);
        paramIndex = values.length;
        return content_field + ' = ' + `$${paramIndex}`;

    // case '!=':
    //     if (content['field'] == undefined) {
    //         error['message'] = 'missing field for != operator';
    //         return null;
    //     }
    //     if (content_value == undefined) {
    //         error['message'] = 'missing value for != operator';
    //         return null;
    //     }
    //     return '{"' + content['field'] + '": { "$ne":' + content_value + '}}';

    // case '<':
    //     if (content['field'] == undefined) {
    //         error['message'] = 'missing field for < operator';
    //         return null;
    //     }
    //     if (content_value == undefined) {
    //         error['message'] = 'missing value for < operator';
    //         return null;
    //     }
    //     return '{"' + content['field'] + '": { "$lt":' + content_value + '}}';

    // case '<=':
    //     if (content['field'] == undefined) {
    //         error['message'] = 'missing field for <= operator';
    //         return null;
    //     }
    //     if (content_value == undefined) {
    //         error['message'] = 'missing value for <= operator';
    //         return null;
    //     }
    //     return '{"' + content['field'] + '": { "$lte":' + content_value + '}}';

    // case '>':
    //     if (content['field'] == undefined) {
    //         error['message'] = 'missing field for > operator';
    //         return null;
    //     }
    //     if (content_value == undefined) {
    //         error['message'] = 'missing value for > operator';
    //         return null;
    //     }
    //     return '{"' + content['field'] + '": { "$gt":' + content_value + '}}';

    // case '>=':
    //     if (content['field'] == undefined) {
    //         error['message'] = 'missing field for >= operator';
    //         return null;
    //     }
    //     if (content_value == undefined) {
    //         error['message'] = 'missing value for >= operator';
    //         return null;
    //     }
    //     return '{"' + content['field'] + '": { "$gte":' + content_value + '}}';

    // case 'contains':
    //     if (content_type != 'string') {
    //         error['message'] = "'contains' operator only valid for strings";
    //         return null;
    //     }
    //     if (content['field'] == undefined) {
    //         error['message'] = "missing field for 'contains' operator";
    //         return null;
    //     }
    //     if (content_value == undefined) {
    //         error['message'] = "missing value for 'contains' operator";
    //         return null;
    //     }

    //     // VDJServer optimization for substring searches on junction_aa
    //     if (content['field'] == 'junction_aa') {
    //         if (content['value'].length < 4) {
    //             error['message'] = "value for 'contains' operator on 'junction_aa' field is too small, length is ("
    //                 + content['value'].length + ") characters, minimum is 4.";
    //             return null;
    //         } else {
    //             return '{"vdjserver_junction_suffixes": {"$regex": "^' + content['value'] + '"}}';
    //         }
    //     }

    //     if (disable_contains) {
    //         error['message'] = "'contains' operator not supported for '" + content['field'] + "' field.";
    //         return null;
    //     } else {
    //         return '{"' + content['field'] + '": { "$regex":' + escapeString(content_value) + ', "$options": "i"}}';
    //     }

    // case 'is': // is missing
    // case 'is missing':
    //     if (content['field'] == undefined) {
    //         error['message'] = "missing field for 'is missing' operator";
    //         return null;
    //     }
    //     return '{"' + content['field'] + '": { "$exists": false } }';

    // case 'not': // is not missing
    // case 'is not missing':
    //     if (content['field'] == undefined) {
    //         error['message'] = "missing field for 'is not missing' operator";
    //         return null;
    //     }
    //     return '{"' + content['field'] + '": { "$exists": true } }';

    // case 'in':
    //     if (content_value == undefined) {
    //         error['message'] = "missing value for 'in' operator";
    //         return null;
    //     }
    //     if (! (content['value'] instanceof Array)) {
    //         error['message'] = "value for 'in' operator is not an array";
    //         return null;
    //     }
    //     if (content['field'] == undefined) {
    //         error['message'] = "missing field for 'in' operator";
    //         return null;
    //     }
    //     return '{"' + content['field'] + '": { "$in":' + content_value + '}}';

    // case 'exclude':
    //     if (content_value == undefined) {
    //         error['message'] = "missing value for 'exclude' operator";
    //         return null;
    //     }
    //     if (! (content['value'] instanceof Array)) {
    //         error['message'] = "value for 'exclude' operator is not an array";
    //         return null;
    //     }
    //     if (content['field'] == undefined) {
    //         error['message'] = "missing field for 'exclude' operator";
    //         return null;
    //     }
    //     return '{"' + content['field'] + '": { "$nin":' + content_value + '}}';

    // case 'and': {
    //     if (! (content instanceof Array)) {
    //         error['message'] = "content for 'and' operator is not an array";
    //         return null;
    //     }
    //     if (content.length < 2) {
    //         error['message'] = "content for 'and' operator needs at least 2 elements";
    //         return null;
    //     }

    //     let exp_list = [];
    //     for (let i = 0; i < content.length; ++i) {
    //         let exp = AKPostgresQuery.constructQueryOperation(airr, schema, content[i], error, check_query_support, disable_contains);
    //         if (exp == null) return null;
    //         exp_list.push(exp);
    //     }
    //     return '{ "$and":[' + exp_list + ']}';
    // }

    // case 'or': {
    //     if (! (content instanceof Array)) {
    //         error['message'] = "content for 'or' operator is not an array";
    //         return null;
    //     }
    //     if (content.length < 2) {
    //         error['message'] = "content for 'or' operator needs at least 2 elements";
    //         return null;
    //     }

    //     let exp_list = [];
    //     for (let i = 0; i < content.length; ++i) {
    //         let exp = AKPostgresQuery.constructQueryOperation(airr, schema, content[i], error, check_query_support, disable_contains);
    //         if (exp == null) return null;
    //         exp_list.push(exp);
    //     }
    //     return '{ "$or":[' + exp_list + ']}';
    // }

    default:
        error['message'] = 'unknown operator in filters: ' + filter['op'];
        return null;
    }

    // should not get here
    //return null;
}

