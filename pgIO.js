
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

var airrkb = require('vdj-tapis-js/airrkb_postgres_query');

// Node Libraries
var _ = require('underscore');
var postgres = require('postgres');
const { Pool } = require('pg');
var csv = require('csv-parser');
var fs = require('fs');
const zlib = require('zlib');

// interactive query connection pool
var pg_pool = null;
pgIO.getPoolConnection = function() {

    if (!pg_pool) {
        const credentials = pgSettings.pg_connection();
        pg_pool = new Pool(credentials);
    }
    return pg_pool;
}

pgIO.endPoolConnection = function() {
    if (pg_pool) {
        pg_pool.end();
        pg_pool = null;
    }
}

// download connection pool
var pg_download_pool = null;
pgIO.getDownloadPoolConnection = function() {

    if (!pg_download_pool) {
        const credentials = pgSettings.pg_download_connection();
        pg_download_pool = new Pool(credentials);
    }
    return pg_download_pool;
}

pgIO.endDownloadPoolConnection = function() {
    if (pg_download_pool) {
        pg_download_pool.end();
        pg_download_pool = null;
    }
}

// test connection
pgIO.testConnection = async function() {
    let pool = pgIO.getPoolConnection();

    try {
        const res = await pool.query("SELECT NOW() as now");
        console.log("Current time with pool:", res.rows[0].now);
        Promise.resolve();
    } catch (err) {
        console.error("Database error", err);
        Promise.reject(err);
    }
}

pgIO.performQueryOperation = async function(filters, error, count_only=false, download_row_handler) {
    let context = 'pgIO.performQueryOperation';
    let download_mode = (count_only || download_row_handler);
    let pool;
    if (download_mode) pool = pgIO.getDownloadPoolConnection();
    else pool = pgIO.getPoolConnection();

    // TODO: field lists should come from schema
    let select_fields = [];
    let header_fields = [];
    let tra_fields = ['species', 'complete_vdj', 'sequence', 'sequence_aa', 'locus', 'v_call', 'd_call', 'j_call', 'c_call', 'junction_aa', 'akc_id'];
    let trb_fields = ['species', 'complete_vdj', 'sequence', 'sequence_aa', 'locus', 'v_call', 'd_call', 'j_call', 'c_call', 'junction_aa', 'akc_id'];
    let trg_fields = ['species', 'complete_vdj', 'sequence', 'sequence_aa', 'locus', 'v_call', 'd_call', 'j_call', 'c_call', 'junction_aa', 'akc_id'];
    let trd_fields = ['species', 'complete_vdj', 'sequence', 'sequence_aa', 'locus', 'v_call', 'd_call', 'j_call', 'c_call', 'junction_aa', 'akc_id'];
    let epitope_fields = ['sequence_aa', 'source_protein', 'source_organism', 'akc_id'];

    let queryText = 'SELECT ';
    if (count_only) {
        queryText += ' COUNT(*) ';
    } else {
        // For fields in SQL columns, to avoid name conflict, not for fields in the JSON object
        for (let i in tra_fields) select_fields.push('cha.' + tra_fields[i] + ' AS tra_chain_' + tra_fields[i]);
        for (let i in trb_fields) select_fields.push('chb.' + trb_fields[i] + ' AS trb_chain_' + trb_fields[i]);
        for (let i in trg_fields) select_fields.push('chg.' + trg_fields[i] + ' AS trg_chain_' + trg_fields[i]);
        for (let i in trd_fields) select_fields.push('chd.' + trd_fields[i] + ' AS trd_chain_' + trd_fields[i]);
        for (let i in epitope_fields) select_fields.push('e.' + epitope_fields[i] + ' AS epitope_' + epitope_fields[i]);

        // For headers in output file
        header_fields.push('documents');
        for (let i in epitope_fields) header_fields.push('epitope_' + epitope_fields[i]);
        for (let i in tra_fields) header_fields.push('tra_chain_' + tra_fields[i]);
        for (let i in trb_fields) header_fields.push('trb_chain_' + trb_fields[i]);
        for (let i in trg_fields) header_fields.push('trg_chain_' + trg_fields[i]);
        for (let i in trd_fields) header_fields.push('trd_chain_' + trd_fields[i]);

        queryText += select_fields.join(', ');
        queryText += ', c.akc_id AS complex_akc_id, t.akc_id AS receptor_akc_id, qa.assay_object';
    }

    // construct where clause
    let values = [];
    let clause = airrkb.constructWhereClause(filters, error, values);

    config.log.info(context, clause);
    config.log.info(context, values);
    if (!clause) return Promise.resolve(null);

    if (clause.includes('qa.assay_object')) {
        queryText += ' FROM "QueryAssay" qa';
        queryText += ' JOIN "Assay_tcr_complexes" atc ON atc.assay_akc_id = qa.akc_id';
        queryText += ' JOIN "TCRpMHCComplex" c ON c.akc_id = atc.tcr_complexes_akc_id';
        queryText += ' LEFT OUTER JOIN "TCellReceptor" t ON t.akc_id = c.tcr';
        queryText += ' LEFT OUTER JOIN "Chain" chb ON chb.akc_id = t.trb_chain';
        queryText += ' LEFT OUTER JOIN "Chain" cha ON cha.akc_id = t.tra_chain';
        queryText += ' LEFT OUTER JOIN "Chain" chg ON chg.akc_id = t.trg_chain';
        queryText += ' LEFT OUTER JOIN "Chain" chd ON chd.akc_id = t.trd_chain';
        queryText += ' LEFT OUTER JOIN "Epitope" e ON e.akc_id = c.epitope';
        queryText += ' WHERE TRUE';

    } else {
        queryText += ' FROM "TCRpMHCComplex" c';
        queryText += ' JOIN "TCellReceptor" t ON c.tcr = t.akc_id';
        queryText += ' LEFT OUTER JOIN "Chain" chb ON t.trb_chain = chb.akc_id';
        queryText += ' LEFT OUTER JOIN "Chain" cha ON t.tra_chain = cha.akc_id';
        queryText += ' LEFT OUTER JOIN "Chain" chg ON t.trg_chain = chg.akc_id';
        queryText += ' LEFT OUTER JOIN "Chain" chd ON t.trd_chain = chd.akc_id';
        queryText += ' LEFT OUTER JOIN "Epitope" e ON c.epitope = e.akc_id';
        queryText += ' JOIN "Assay_tcr_complexes" atc ON atc.tcr_complexes_akc_id = c.akc_id';
        queryText += ' JOIN "QueryAssay" qa ON atc.assay_akc_id = qa.akc_id';
        queryText += ' WHERE TRUE';
    }

    if (clause) 
        if (download_mode)
            queryText += ' AND (' + clause + ')';
        else
            queryText += ' AND (' + clause + ') LIMIT ' + (pgSettings.max_results + 1);
    else {
        console.log(error);
        return Promise.resolve(null);
    }

    // perform the query
    console.log(queryText);

    let partial = false;
    let results = [];
    try {
        if (! download_mode) {
            // check cost to avoid inefficient queries
            // TODO: cost limit should be a config variable
            const cost = await pool.query("EXPLAIN (FORMAT JSON) " + queryText, values);
            let query_cost = cost.rows[0];
            //config.log.info(context, JSON.stringify(query_cost,null,2));
            if ((query_cost['QUERY PLAN']) && (query_cost['QUERY PLAN'].length > 0)) {
                let total_cost = query_cost['QUERY PLAN'][0]['Plan']['Total Cost'];
                config.log.info(context, 'query cost: ' + total_cost);
                // if (total_cost > 1000000) {
                //     error['message'] = 'Query is too inefficient to be executed.';
                //     return Promise.resolve(null);
                // }
            }
        }

        // perform query
        const res = await pool.query(queryText, values);

        if (count_only) {
            return Promise.resolve(res.rows[0]);
        }

        // simple hack to check partial results, ask for max + 1
        console.log(res.rows.length);
        if (res.rows.length == (pgSettings.max_results + 1)) partial = true;
        console.log(partial);

        // format for output response
        for (let i in res.rows) {
            let row = res.rows[i];

            if (download_mode) {
                download_row_handler(header_fields, row);
                continue;
            }

            if (i == pgSettings.max_results) break;

            let obj = { tcr: { receptor: null, epitope: null, mhc: null }, bcr: null, assay: null };
            if (row['complex_akc_id']) obj['akc_id'] = row['complex_akc_id'];
            if (row['tra_chain_akc_id']) {
                if (!obj['tcr']['receptor']) obj['tcr']['receptor'] = {};
                if (row['receptor_akc_id']) obj['tcr']['receptor']['akc_id'] = row['receptor_akc_id'];
                obj['tcr']['receptor']['tra_chain'] = {};
                for (let j in tra_fields) obj['tcr']['receptor']['tra_chain'][tra_fields[j]] = row['tra_chain_' + tra_fields[j]];
            }
            if (row['trb_chain_akc_id']) {
                if (!obj['tcr']['receptor']) obj['tcr']['receptor'] = {};
                if (row['receptor_akc_id']) obj['tcr']['receptor']['akc_id'] = row['receptor_akc_id'];
                obj['tcr']['receptor']['trb_chain'] = {};
                for (let j in trb_fields) obj['tcr']['receptor']['trb_chain'][trb_fields[j]] = row['trb_chain_' + trb_fields[j]];
            }
            if (row['trg_chain_akc_id']) {
                if (!obj['tcr']['receptor']) obj['tcr']['receptor'] = {};
                if (row['receptor_akc_id']) obj['tcr']['receptor']['akc_id'] = row['receptor_akc_id'];
                obj['tcr']['receptor']['trg_chain'] = {};
                for (let j in trg_fields) obj['tcr']['receptor']['trg_chain'][trg_fields[j]] = row['trg_chain_' + trg_fields[j]];
            }
            if (row['trd_chain_akc_id']) {
                if (!obj['tcr']['receptor']) obj['tcr']['receptor'] = {};
                if (row['receptor_akc_id']) obj['tcr']['receptor']['akc_id'] = row['receptor_akc_id'];
                obj['tcr']['receptor']['trd_chain'] = {};
                for (let j in trd_fields) obj['tcr']['receptor']['trd_chain'][trd_fields[j]] = row['trd_chain_' + trd_fields[j]];
            }
            if (row['epitope_akc_id']) {
                if (!obj['tcr']['epitope']) obj['tcr']['epitope'] = {};
                for (let j in epitope_fields) obj['tcr']['epitope'][epitope_fields[j]] = row['epitope_' + epitope_fields[j]];
            }
            if (row['assay_object']) {
                obj['assay'] = row['assay_object'];
            }
            results.push(obj);
        }

        if (download_mode) return Promise.resolve();
        else {
            config.log.info(context, 'Returning ' + results.length + ' query results.');
            return Promise.resolve({ partial: partial, results: results });
        }
    } catch (err) {
        if (err.message.includes('timeout')) return Promise.reject({ status: 'timeout', message: 'Query timeout.' });
        else return Promise.reject({ status: 'error', message: err.message });
    }
}

pgIO.performQueryToFile = async function(filters, filename, format) {
    var context = 'pgIO.performQueryToFile';

    return new Promise(async function(resolve, reject) {

        var writable = fs.createWriteStream(filename)
            .on('error', function(e) { let msg = config.log.error(context, 'caught error: ' + e); return reject(new Error(msg)); });
    
        writable.on('finish', function() {
            config.log.info(context, 'finish of write stream');
            return resolve(cnt);
        });
    
        // we rely on closure
        var first = true;
        var cnt = 0;
        var row_handler = function(headers, row) {
            cnt += 1;

            // write data
            switch (format) {
                case 'tsv': {
                    // write headers
                    if (first) {
                        //console.log(headers);
                        //console.log(row['assay_object']);
                        writable.write(headers.join('\t'));
                        writable.write('\n');
                    }

                    // write row
                    let vals = [];
                    for (let i = 0; i < headers.length; ++i) {
                        let p = headers[i];
                        //if (config.debug) console.log(p, entry[p]);
                        if (p == 'documents') {
                            if (row['assay_object']['investigation']['documents']) {
                                vals.push(row['assay_object']['investigation']['documents'].join(','));
                            } else vals.push('');
                        } else {
                            if (row[p] == undefined) vals.push('');
                            else vals.push(row[p]);
                        }
                    }
                    writable.write(vals.join('\t'));
                    writable.write('\n');
                    break;
                }
            }
            first = false;
        }

        var error = { message: '' };
        await pgIO.performQueryOperation(filters, error, false, row_handler)
            .catch(function(error) {
                return reject(error);
            });

        // the finish event will resolve the promise
        writable.end();
    });    
}
