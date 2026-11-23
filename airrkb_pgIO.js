
'use strict';

//
// airrkb_pgIO.js
// Functions for direct access to Postgresql for AIRR Knowledge
//
// AIRR Knowledge
// https://airr-knowledge.org
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

pgIO.recordQuery = async function(queryRecord) {
    let context = 'pgIO.recordQuery';
    let pool = pgIO.getPoolConnection();

    let queryText = 'INSERT INTO "QueryRecord" (endpoint, ip, status, message, record_count, start_time, elapsed_time, query) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);'
    let values = [ queryRecord['endpoint'],
        queryRecord['ip'],
        queryRecord['status'],
        queryRecord['message'], 
        queryRecord['record_count'],
        new Date(queryRecord['start_time']),
        queryRecord['elapsed_time'],
        queryRecord['query'] ];

    try {
        const res = await pool.query(queryText, values);
    } catch (err) {
        config.log.error(context, 'Unable to save query record: ' + err + '\ndata: ' + JSON.stringify(queryRecord));
    }
    return Promise.resolve();
}

pgIO.performQueryOperation = async function(complex_filter, assay_filter, error, count_only=false, download_row_handler) {
    let context = 'pgIO.performQueryOperation';
    let download_mode = (count_only || download_row_handler);
    let pool;
    if (download_mode) pool = pgIO.getDownloadPoolConnection();
    else pool = pgIO.getPoolConnection();

    let result_obj = { partial: false, Assay: null, TCRpMHC: [], AntibodyAntigen: [] };

    // do assay filter first if there is one
    let assay_results = {};
    if (assay_filter) {
        let queryText = 'SELECT qa.akc_id, qa.assay_object';
        queryText += ' FROM "QueryAssay" qa';
        queryText += ' WHERE TRUE';

        // construct where clause
        let values = [];
        let clause = airrkb.constructWhereClause('assay', assay_filter['filters'], error, values);

        // add where clause
        if (clause) queryText += ' AND (' + clause + ')';
        else {
            console.log(error);
            return null;
        }

        // perform the query
        console.log(queryText);
        console.log(values);
        try {
            const res = await pool.query(queryText, values);

            if (count_only) {
                return Promise.resolve(res.rows[0]);
            }

            // format for output response
            for (const row of res.rows) {
                assay_results[row['akc_id']] = row['assay_object'];
            }

            config.log.info(context, 'Query obtained ' + Object.keys(assay_results).length + ' assay results.');

        } catch (err) {
            if (err.message.includes('timeout')) return Promise.reject({ status: 'timeout', message: 'Query timeout.' });
            else return Promise.reject({ status: 'error', message: err.message });
        }

        // if the assay results are empty then no need to do complex query
        if (Object.keys(assay_results).length == 0) {
            config.log.info(context, 'Skipping complex search as no assay results.');
            return Promise.resolve(result_obj);
        } else result_obj['Assay'] = assay_results;
    }

    // TODO: field lists should come from schema
    let complex_fields = ['species', 'paired_chain', 'akc_id'];
    let chain_fields = ['species', 'complete_vdj', 'sequence', 'sequence_aa', 'locus', 'v_subgroup', 'v_gene', 'v_call', 'd_call', 'j_subgroup', 'j_gene', 'j_call', 'c_call', 'junction_aa', 'akc_id'];
    let antigen_fields = ['source_molecule', 'source_species', 'akc_id'];
    let epitope_fields = ['sequence_aa', 'modifications', 'akc_id'];
    let mhc_fields = ['mhc_class', 'mhc_label', 'akc_id'];

    if (complex_filter) {
        let table_name = null;
        let join_clause = "";
        let select_fields = [];
        let header_fields = [];
        let queryText = 'SELECT ';
        if (count_only) queryText += ' COUNT(*) ';
        else {
            for (let i in complex_fields) select_fields.push('c.' + complex_fields[i] + ' AS complex_' + complex_fields[i]);
            for (let i in antigen_fields) select_fields.push('a.' + antigen_fields[i] + ' AS antigen_' + antigen_fields[i]);
            for (let i in epitope_fields) select_fields.push('e.' + epitope_fields[i] + ' AS epitope_' + epitope_fields[i]);

            // For headers in output file
            header_fields.push('documents');
            for (let i in antigen_fields) header_fields.push('antigen_' + antigen_fields[i]);
            for (let i in epitope_fields) header_fields.push('epitope_' + epitope_fields[i]);
        }

        if (complex_filter['receptor_type'] == 'alpha-beta') {
            if (complex_filter['paired_chain_only'])
                table_name = 'PairedAlphaBetaReceptorComposite';
            else
                table_name = 'AlphaBetaReceptorComposite';

            join_clause += ' FROM "' + table_name + '" c';
            join_clause += ' LEFT OUTER JOIN "BetaChain" chb ON c.trb_chain = chb.hash_infer_vdj_sequence_aa';
            join_clause += ' LEFT OUTER JOIN "AlphaChain" cha ON c.tra_chain = cha.hash_infer_vdj_sequence_aa';
            join_clause += ' LEFT OUTER JOIN "Antigen" a ON c.antigen = a.akc_id';
            join_clause += ' LEFT OUTER JOIN "Epitope" e ON c.epitope = e.akc_id';
            join_clause += ' LEFT OUTER JOIN "MajorHistocompatibilityComplex" m ON c.mhc = m.akc_id';
            join_clause += ' JOIN "Assay_receptor_composites" arc ON arc.receptor_composites_akc_id = c.akc_id';
            join_clause += ' JOIN "QueryAssay" qa ON qa.akc_id = arc.assay_akc_id';

            if (! count_only) {
                for (let i in chain_fields) select_fields.push('cha.' + chain_fields[i] + ' AS tra_chain_' + chain_fields[i]);
                for (let i in chain_fields) select_fields.push('chb.' + chain_fields[i] + ' AS trb_chain_' + chain_fields[i]);
                for (let i in mhc_fields) select_fields.push('m.' + mhc_fields[i] + ' AS mhc_' + mhc_fields[i]);

                for (let i in mhc_fields) header_fields.push('mhc_' + mhc_fields[i]);
                for (let i in chain_fields) header_fields.push('tra_chain_' + chain_fields[i]);
                for (let i in chain_fields) header_fields.push('trb_chain_' + chain_fields[i]);
            }

        } else if (complex_filter['receptor_type'] == 'gamma-delta') {
            if (complex_filter['paired_chain_only'])
                table_name = 'PairedGammaDeltaReceptorComposite';
            else
                table_name = 'GammaDeltaReceptorComposite';

            join_clause += ' FROM "' + table_name + '" c';
            join_clause += ' LEFT OUTER JOIN "GammaChain" chg ON c.trg_chain = chg.hash_infer_vdj_sequence_aa';
            join_clause += ' LEFT OUTER JOIN "DeltaChain" chd ON c.trd_chain = chd.hash_infer_vdj_sequence_aa';
            join_clause += ' LEFT OUTER JOIN "Antigen" a ON c.antigen = a.akc_id';
            join_clause += ' LEFT OUTER JOIN "Epitope" e ON c.epitope = e.akc_id';
            join_clause += ' LEFT OUTER JOIN "MajorHistocompatibilityComplex" m ON c.mhc = m.akc_id';
            join_clause += ' JOIN "Assay_receptor_composites" arc ON arc.receptor_composites_akc_id = c.akc_id';
            join_clause += ' JOIN "QueryAssay" qa ON qa.akc_id = arc.assay_akc_id';

            if (! count_only) {
                for (let i in chain_fields) select_fields.push('chg.' + chain_fields[i] + ' AS trg_chain_' + chain_fields[i]);
                for (let i in chain_fields) select_fields.push('chd.' + chain_fields[i] + ' AS trd_chain_' + chain_fields[i]);
                for (let i in mhc_fields) select_fields.push('m.' + mhc_fields[i] + ' AS mhc_' + mhc_fields[i]);

                for (let i in mhc_fields) header_fields.push('mhc_' + mhc_fields[i]);
                for (let i in chain_fields) header_fields.push('trg_chain_' + chain_fields[i]);
                for (let i in chain_fields) header_fields.push('trd_chain_' + chain_fields[i]);
            }

        } else if (complex_filter['receptor_type'] == 'heavy-kappa') {
            if (complex_filter['paired_chain_only'])
                table_name = 'PairedBCellReceptorComposite';
            else
                table_name = 'BCellReceptorComposite';

            join_clause += ' FROM "' + table_name + '" c';
            join_clause += ' LEFT OUTER JOIN "HeavyChain" chh ON c.igh_chain = chh.hash_infer_vdj_sequence_aa';
            join_clause += ' LEFT OUTER JOIN "KappaChain" chk ON c.igk_chain = chk.hash_infer_vdj_sequence_aa';
            join_clause += ' LEFT OUTER JOIN "Antigen" a ON c.antigen = a.akc_id';
            join_clause += ' LEFT OUTER JOIN "Epitope" e ON c.epitope = e.akc_id';
            join_clause += ' JOIN "Assay_receptor_composites" arc ON arc.receptor_composites_akc_id = c.akc_id';
            join_clause += ' JOIN "QueryAssay" qa ON qa.akc_id = arc.assay_akc_id';

            if (! count_only) {
                for (let i in chain_fields) select_fields.push('chh.' + chain_fields[i] + ' AS igh_chain_' + chain_fields[i]);
                for (let i in chain_fields) select_fields.push('chk.' + chain_fields[i] + ' AS igk_chain_' + chain_fields[i]);

                for (let i in chain_fields) header_fields.push('igh_chain_' + chain_fields[i]);
                for (let i in chain_fields) header_fields.push('igk_chain_' + chain_fields[i]);
            }
        } else if (complex_filter['receptor_type'] == 'heavy-lambda') {
            if (complex_filter['paired_chain_only'])
                table_name = 'PairedBCellReceptorComposite';
            else
                table_name = 'BCellReceptorComposite';

            join_clause += ' FROM "' + table_name + '" c';
            join_clause += ' LEFT OUTER JOIN "HeavyChain" chh ON c.igh_chain = chh.hash_infer_vdj_sequence_aa';
            join_clause += ' LEFT OUTER JOIN "LambdaChain" chl ON c.igl_chain = chl.hash_infer_vdj_sequence_aa';
            join_clause += ' LEFT OUTER JOIN "Antigen" a ON c.antigen = a.akc_id';
            join_clause += ' LEFT OUTER JOIN "Epitope" e ON c.epitope = e.akc_id';
            join_clause += ' JOIN "Assay_receptor_composites" arc ON arc.receptor_composites_akc_id = c.akc_id';
            join_clause += ' JOIN "QueryAssay" qa ON qa.akc_id = arc.assay_akc_id';

            if (! count_only) {
                for (let i in chain_fields) select_fields.push('chh.' + chain_fields[i] + ' AS igh_chain_' + chain_fields[i]);
                for (let i in chain_fields) select_fields.push('chl.' + chain_fields[i] + ' AS igl_chain_' + chain_fields[i]);

                for (let i in chain_fields) header_fields.push('igh_chain_' + chain_fields[i]);
                for (let i in chain_fields) header_fields.push('igl_chain_' + chain_fields[i]);
            }
        }

        if (! table_name) {
            error['message'] = "Could not determine receptor type.";
            return null;
        }

        // add select fields
        if (! count_only) {
            queryText += select_fields.join(', ');
            queryText += ', arc.assay_akc_id, qa.assay_object';
        }

        // add join clause
        queryText += join_clause;
        queryText += ' WHERE TRUE';

        // construct where clause
        let values = [];
        let clause = airrkb.constructWhereClause('complex', complex_filter['filters'], error, values);

        if (assay_filter) {
            let assay_clause = airrkb.constructWhereClause('assay', assay_filter['filters'], error, values);
            if (assay_clause) clause = '(' + clause + ') AND (' + assay_clause + ')';
        }

        // add where clause
        if (clause) 
            if (download_mode)
                queryText += ' AND (' + clause + ')';
            else
                queryText += ' AND (' + clause + ') LIMIT ' + (pgSettings.max_results + 1);
        else {
            console.log(error);
            return null;
        }

        // perform the query
        console.log(queryText);

        let partial = false;
        let results = [];
        assay_results = {};
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
            let prev_obj = null;
            let i = 0;
            for (const row of res.rows) {
                //let row = res.rows[i];

                if (download_mode) {
                    download_row_handler(header_fields, row);
                    continue;
                }

                if (i == pgSettings.max_results) break;

                // we rely on the results being in akc_id order for the composite
                if (i > 0) {
                    if (row['complex_akc_id'] == prev_obj['akc_id']) {
                        // same complex found in another assay
                        if (! prev_obj['assays'].includes(row['assay_akc_id']))
                            prev_obj['assays'].push(row['assay_akc_id']);
                        continue;
                    }
                }

                let obj = { receptor_type: complex_filter['receptor_type'] };
                prev_obj = obj;
                if (i % 1000 == 0) console.log(i);
                ++i;
                if (row['complex_akc_id']) {
                    for (let j in complex_fields) obj[complex_fields[j]] = row['complex_' + complex_fields[j]];
                }
                if (row['tra_chain_akc_id']) {
                    obj['tra_chain'] = {};
                    for (let j in chain_fields) obj['tra_chain'][chain_fields[j]] = row['tra_chain_' + chain_fields[j]];
                }
                if (row['trb_chain_akc_id']) {
                    obj['trb_chain'] = {};
                    for (let j in chain_fields) obj['trb_chain'][chain_fields[j]] = row['trb_chain_' + chain_fields[j]];
                }
                if (row['trg_chain_akc_id']) {
                    obj['trg_chain'] = {};
                    for (let j in chain_fields) obj['trg_chain'][chain_fields[j]] = row['trg_chain_' + chain_fields[j]];
                }
                if (row['trd_chain_akc_id']) {
                    obj['trd_chain'] = {};
                    for (let j in chain_fields) obj['trd_chain'][chain_fields[j]] = row['trd_chain_' + chain_fields[j]];
                }
                if (row['igh_chain_akc_id']) {
                    obj['igh_chain'] = {};
                    for (let j in chain_fields) obj['igh_chain'][chain_fields[j]] = row['igh_chain_' + chain_fields[j]];
                }
                if (row['igk_chain_akc_id']) {
                    obj['igk_chain'] = {};
                    for (let j in chain_fields) obj['igk_chain'][chain_fields[j]] = row['igk_chain_' + chain_fields[j]];
                }
                if (row['igl_chain_akc_id']) {
                    obj['igl_chain'] = {};
                    for (let j in chain_fields) obj['igl_chain'][chain_fields[j]] = row['igl_chain_' + chain_fields[j]];
                }
                if (row['epitope_akc_id']) {
                    if (!obj['epitope']) obj['epitope'] = {};
                    for (let j in epitope_fields) obj['epitope'][epitope_fields[j]] = row['epitope_' + epitope_fields[j]];
                }
                if (row['antigen_akc_id']) {
                    if (!obj['antigen']) obj['antigen'] = {};
                    for (let j in antigen_fields) obj['antigen'][antigen_fields[j]] = row['antigen_' + antigen_fields[j]];
                }
                if (row['mhc_akc_id']) {
                    if (!obj['mhc']) obj['mhc'] = {};
                    for (let j in mhc_fields) obj['mhc'][mhc_fields[j]] = row['mhc_' + mhc_fields[j]];
                }
                if (row['assay_akc_id']) {
                    obj['assays'] = [ row['assay_akc_id'] ];
                    assay_results[row['assay_akc_id']] = row['assay_object'];
                }
                results.push(obj);
            }

            if (download_mode) return Promise.resolve();
            else {
                let result_obj = { partial: partial, Assay: assay_results };
                if (complex_filter['receptor_type'] == 'alpha-beta') result_obj['TCRpMHC'] = results;
                else if (complex_filter['receptor_type'] == 'gamma-delta') result_obj['TCRpMHC'] = results;
                else if (complex_filter['receptor_type'] == 'heavy-kappa') result_obj['AntibodyAntigen'] = results;
                else if (complex_filter['receptor_type'] == 'heavy-lambda') result_obj['AntibodyAntigen'] = results;
                config.log.info(context, 'Returning ' + results.length + ' complex results.');
                return Promise.resolve(result_obj);
            }
        } catch (err) {
            if (err.message.includes('timeout')) return Promise.reject({ status: 'timeout', message: 'Query timeout.' });
            else return Promise.reject({ status: 'error', message: err.message });
        }

    }
}

pgIO.performQueryToFile = async function(complex_filter, assay_filter, filename, format) {
    var context = 'pgIO.performQueryToFile';

    var writable = fs.createWriteStream(filename)
        .on('error', function(e) { let msg = config.log.error(context, 'caught error: ' + e); return Promise.reject(new Error(msg)); });

    writable.on('finish', function() {
        config.log.info(context, 'finish of write stream');
        return Promise.resolve(cnt);
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
    await pgIO.performQueryOperation(complex_filter, assay_filter, error, false, row_handler)
        .catch(function(error) {
            return Promise.reject(error);
        });

    // the finish event will resolve the promise
    writable.end();
}

pgIO.ontologyTissueList = async function(error) {
    let context = 'pgIO.ontologyTissueList';
    let pool = pgIO.getPoolConnection();

    // perform query
    try {
        let results = [];
        let queryText = `SELECT distinct(assay_object -> 'specimen' -> 'tissue' -> 'term_id') AS term_id, assay_object -> 'specimen' -> 'tissue' -> 'term_label' AS term_label FROM "QueryAssay";`;
        const res = await pool.query(queryText);

        for (const row of res.rows) {
            if (row['term_id']) results.push(row);
        }

        config.log.info(context, 'Returning ' + results.length + ' ontology results.');
        return Promise.resolve(results);
    } catch (err) {
        if (err.message.includes('timeout')) return Promise.reject({ status: 'timeout', message: 'Query timeout.' });
        else return Promise.reject({ status: 'error', message: err.message });
    }
}

pgIO.ontologyDiseaseList = async function(error) {
    let context = 'pgIO.ontologyDiseaseList';
    let pool = pgIO.getPoolConnection();

    // perform query
    try {
        let results = [];
        let queryText = `SELECT DISTINCT
            event.value -> 'disease' ->> 'term_id' AS term_id,
            event.value -> 'disease' ->> 'term_label' AS term_label
            FROM "QueryAssay" t
            CROSS JOIN LATERAL jsonb_each(t.assay_object -> 'life_events') AS event
            WHERE event.value -> 'disease' ->> 'term_id' IS NOT NULL;`

        const res = await pool.query(queryText);

        for (const row of res.rows) {
            if (row['term_id']) results.push(row);
        }

        config.log.info(context, 'Returning ' + results.length + ' ontology results.');
        return Promise.resolve(results);
    } catch (err) {
        if (err.message.includes('timeout')) return Promise.reject({ status: 'timeout', message: 'Query timeout.' });
        else return Promise.reject({ status: 'error', message: err.message });
    }
}

pgIO.ontologyMHCList = async function(error) {
    let context = 'pgIO.ontologyMHCList';
    let pool = pgIO.getPoolConnection();

    // perform query
    try {
        let results = [];
        let queryText = `SELECT mhc_ref AS term_id, mhc_label AS term_label FROM "MajorHistocompatibilityComplex";`;
        const res = await pool.query(queryText);

        for (const row of res.rows) {
            if (row['term_id']) results.push(row);
        }

        config.log.info(context, 'Returning ' + results.length + ' ontology results.');
        return Promise.resolve(results);
    } catch (err) {
        if (err.message.includes('timeout')) return Promise.reject({ status: 'timeout', message: 'Query timeout.' });
        else return Promise.reject({ status: 'error', message: err.message });
    }
}

pgIO.ontologyAntigenOrganismList = async function(error) {
    let context = 'pgIO.ontologyAntigenOrganismList';
    let pool = pgIO.getPoolConnection();

    // perform query
    try {
        let results = [];
        let queryText = `SELECT distinct(source_species) AS term_id, t.term_label FROM "Antigen" a JOIN "TaxonomicSpecies" t ON a.source_species = t.term_id;`;
        const res = await pool.query(queryText);

        for (const row of res.rows) {
            if (row['term_id']) results.push(row);
        }

        config.log.info(context, 'Returning ' + results.length + ' ontology results.');
        return Promise.resolve(results);
    } catch (err) {
        if (err.message.includes('timeout')) return Promise.reject({ status: 'timeout', message: 'Query timeout.' });
        else return Promise.reject({ status: 'error', message: err.message });
    }
}

pgIO.ontologyAssayList = async function(error) {
    let context = 'pgIO.ontologyAssayList';
    let pool = pgIO.getPoolConnection();

    // perform query
    try {
        let results = [];
        let queryText = `SELECT distinct(assay_object -> 'assay_type' -> 'term_id') AS term_id, assay_object -> 'assay_type' -> 'term_label' AS term_label FROM "QueryAssay";`;
        const res = await pool.query(queryText);

        for (const row of res.rows) {
            if (row['term_id']) results.push(row);
        }

        config.log.info(context, 'Returning ' + results.length + ' ontology results.');
        return Promise.resolve(results);
    } catch (err) {
        if (err.message.includes('timeout')) return Promise.reject({ status: 'timeout', message: 'Query timeout.' });
        else return Promise.reject({ status: 'error', message: err.message });
    }
}
