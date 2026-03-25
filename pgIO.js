
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
const { Pool } = require('pg');
var csv = require('csv-parser');
var fs = require('fs');
const zlib = require('zlib');

// get connection
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

pgIO.restrictedQueryOperation = async function(cdr3_value) {
    let context = 'pgIO.restrictedQueryOperation';
    let pool = pgIO.getPoolConnection();

    // TODO: field lists should come from schema
    let select_fields = [];
    let tra_fields = ['species', 'complete_vdj', 'sequence', 'sequence_aa', 'locus', 'v_call', 'd_call', 'j_call', 'c_call', 'junction_aa', 'akc_id'];
    for (let i in tra_fields) select_fields.push('cha.' + tra_fields[i] + ' AS tra_chain_' + tra_fields[i]);
    
    let trb_fields = ['species', 'complete_vdj', 'sequence', 'sequence_aa', 'locus', 'v_call', 'd_call', 'j_call', 'c_call', 'junction_aa', 'akc_id'];
    for (let i in trb_fields) select_fields.push('chb.' + trb_fields[i] + ' AS trb_chain_' + trb_fields[i]);

    let epitope_fields = ['sequence_aa', 'source_protein', 'source_organism', 'akc_id'];
    for (let i in epitope_fields) select_fields.push('e.' + epitope_fields[i] + ' AS epitope_' + epitope_fields[i]);

    let queryText = 'SELECT ';
    queryText += select_fields.join(', ');
    queryText += ', qa.assay_object';
    queryText += ' FROM "TCRpMHCComplex" c';
    queryText += ' JOIN "TCellReceptor" t ON c.tcr = t.akc_id';
    queryText += ' JOIN "Chain" chb ON t.trb_chain = chb.akc_id';
    queryText += ' LEFT OUTER JOIN "Chain" cha ON t.tra_chain = cha.akc_id';
    queryText += ' LEFT OUTER JOIN "Epitope" e ON c.epitope = e.akc_id';
    queryText += ' JOIN "Assay_tcr_complexes" atc ON atc.tcr_complexes_akc_id = c.akc_id';
    queryText += ' JOIN "QueryAssay" qa ON atc.assay_akc_id = qa.akc_id';
    queryText += ' WHERE TRUE';

    let values = [];
    let paramIndex = 1;

    values.push(cdr3_value);
    queryText += ` AND chb.junction_aa = $${paramIndex}`
    ++paramIndex;

    config.log.info(context, queryText);
    let results = [];
    try {
        const res = await pool.query(queryText, values);

        // format for output response
        for (let i in res.rows) {
            let row = res.rows[i];
            let obj = { tcr: { receptor: null, epitope: null, mhc: null }, bcr: null, assay: null };
            if (row['tra_chain_akc_id']) {
                if (!obj['tcr']['receptor']) obj['tcr']['receptor'] = {};
                obj['tcr']['receptor']['tra_chain'] = {};
                for (let j in tra_fields) obj['tcr']['receptor']['tra_chain'][tra_fields[j]] = row['tra_chain_' + tra_fields[j]];
            }
            if (row['trb_chain_akc_id']) {
                if (!obj['tcr']['receptor']) obj['tcr']['receptor'] = {};
                obj['tcr']['receptor']['trb_chain'] = {};
                for (let j in trb_fields) obj['tcr']['receptor']['trb_chain'][trb_fields[j]] = row['trb_chain_' + trb_fields[j]];
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

        config.log.info(context, 'Returning ' + results.length + ' query results.');
        return Promise.resolve(results);
    } catch (err) {
        console.error(err);
        return Promise.reject(err);
    }

}

/*
def get_query_for_locus(locus):
    query = f"""
    SELECT
        c.akc_id,
        c.epitope,
        e.sequence_aa,
        e.source_protein,
        e.source_organism,
        ch.junction_aa,
        ch.species,
        ch.v_call,
        ch.j_call
    FROM "TCRpMHCComplex" c
    JOIN "TCellReceptor" t
        ON c.tcr = t.akc_id
    JOIN "Chain" ch
        ON t.{locus}_chain = ch.akc_id
    JOIN "Epitope" e
        ON c.epitope = e.akc_id
    WHERE ch.junction_aa = ANY(%s)
    """
    return query
 
*/
