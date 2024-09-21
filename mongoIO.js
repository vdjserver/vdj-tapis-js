
'use strict';

//
// mongoIO.js
// Functions for direct access to MongoDB
// These are specifically for loading data into the VDJServer ADC Data Repository
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

var mongoIO  = {};
module.exports = mongoIO;

// Server environment config
//var config = require('../config/config');
var mongoSettings = require('./mongoSettings');

// Tapis
var tapisSettings = require('vdj-tapis-js/tapisSettings');
var tapisIO = tapisSettings.get_default_tapis();
var config = tapisSettings.config;
var ServiceAccount = tapisIO.serviceAccount;
var GuestAccount = tapisIO.guestAccount;
var webhookIO = require('vdj-tapis-js/webhookIO');
var adc_mongo_query = require('vdj-tapis-js/adc_mongo_query');

// Node Libraries
var _ = require('underscore');
var MongoClient = require('mongodb').MongoClient;
var csv = require('csv-parser');
var fs = require('fs');
const zlib = require('zlib');

var airr = require('airr-js');

// endpoint specific processing
var rearrangement = {};
rearrangement.cleanRecord = function(record, airr_schema, projection, all_fields) {
    if (!record) return;
    if ((typeof record) != 'object') return;

    if (!record['sequence_id']) {
        if (record['_id']['$oid']) record['sequence_id'] = record['_id']['$oid'];
        else record['sequence_id'] = record['_id'];
    }

    // gene calls, join back to string
    if ((typeof record['v_call']) == "object") record['v_call'] = record['v_call'].join(',');
    if ((typeof record['d_call']) == "object") record['d_call'] = record['d_call'].join(',');
    if ((typeof record['j_call']) == "object") record['j_call'] = record['j_call'].join(',');

    // TODO: general this a bit in case we add more
    if (record['_id']) delete record['_id'];
    if (record['_etag']) delete record['_etag'];
    if (record['vdjserver_junction_suffixes'])
        if (projection['vdjserver_junction_suffixes'] == undefined)
            delete record['vdjserver_junction_suffixes'];

    // add any missing required fields
    if (all_fields.length > 0) {
        airr.addFields(record, all_fields, airr_schema);
    }
    // apply projection
    var keys = Object.keys(record);
    if (Object.keys(projection).length > 0) {
        for (var p = 0; p < keys.length; ++p)
            if (projection[keys[p]] == undefined)
                delete record[keys[p]];
    } 
    return record;
}

var endpoint_map = {
    "rearrangement": rearrangement
};

// test connection
mongoIO.testConnection = async function() {

    return new Promise(function(resolve, reject) {
        // get connection to database
        MongoClient.connect(mongoSettings.url, async function(err, db) {
            if (err) {
                resolve(false)
            } else {
                db.close();
                resolve(true);
            }
        });
    });
}

//
// Clean object by removing fields with null or empty string values
//
mongoIO.cleanObject = function(obj) {
    if (!obj) return;
    if ((typeof obj) != 'object') return;

    var keys = Object.keys(obj);
    for (var k = 0; k < keys.length; ++k) {
        var key = keys[k];
        if (obj[key] == null) {
            delete obj[key];
            continue;
        }
        if ((typeof obj[key]) == 'string') {
            if (obj[key].length == 0) {
                delete obj[key];
                continue;
            }
        }
        if (Array.isArray(obj[key]))
            for (var entry in obj[key])
                mongoIO.cleanObject(obj[key][entry]);
        if ((typeof obj[key]) == 'object')
            mongoIO.cleanObject(obj[key]);
    }
}

function getAllSubstrings(str,size) {
  var i, j, result = [];
  size = (size || 4);
  for (i = 0; i < str.length; i++) {
      for (j = str.length; j-i>=size; j--) {
          result.push(str.slice(i, j));
      }
  }
  return result;
}

function getAllSuffixes(str,size) {
    var i, j, result = [];
    size = (size || 4);
    if (str.length < size) return null;
    for (i = 0; i <= (str.length - size); i++) {
        result.push(str.slice(i));
    }
    return result;
}

// TODO: need to use germline DB for this
function parseGene(str) {
    var result = {
        gene: null,
        subgroup: null
    };
    var aidx = str.indexOf('*');
    if (aidx < 0) return null;
    result.gene = str.slice(0,aidx);

    var didx = result.gene.indexOf('-');
    if (didx >= 0) result.subgroup = result.gene.slice(0,didx);
    else {
        // maybe it's mouse with an S separator
        // else just use gene as subgroup
        var sidx = result.gene.indexOf('S');
        if (sidx >= 0) result.subgroup = result.gene.slice(0,sidx);
        else result.subgroup = result.gene;
    }
    
    return result;
}

//
// Generic mongo operations
//

// perform a query

// perform an aggregation
mongoIO.performAggregation = async function(collection_name, agg) {
    var context = 'mongoIO.performAggregation';

    return new Promise(function(resolve, reject) {

        MongoClient.connect(mongoSettings.url, async function(err, db) {
            if (err) return reject(new Error('Could not connect to database.'));

            config.log.info(context, "Connected successfully to mongodb");

            var v1airr = db.db(mongoSettings.dbname);
            var collection = v1airr.collection(collection_name);

            // perform a facets aggregation query
            var records = await collection.aggregate(agg).toArray()
                .catch(function(error) {
                    db.close();
                    return reject(new Error('Error performing aggregation: ' + error));
                });

            config.log.info(context, 'Retrieve ' + records.length + ' records.');

            db.close();
            return resolve(records);
        });
    });
}

// perform a query count
mongoIO.queryCount = async function(collection_name, query) {
    var context = 'mongoIO.queryCount';

    return new Promise(function(resolve, reject) {

        MongoClient.connect(mongoSettings.url, async function(err, db) {
            if (err) return reject(new Error('Could not connect to database.'));

            config.log.info(context, "Connected successfully to mongodb");

            var v1airr = db.db(mongoSettings.dbname);
            var collection = v1airr.collection(collection_name);

            // perform a facets aggregation query
            var cnt = await collection.find(query).count()
                .catch(function(error) {
                    db.close();
                    return reject(new Error('Error performing query count: ' + error));
                });

            config.log.info(context, 'Would retrieve ' + cnt + ' records.');

            db.close();
            return resolve([{ count: cnt }]);
        });
    });
}

mongoIO.processRearrangementRow = function(row, rep, dp_id, load_set) {
    // identifiers
    if (row['sequence_id']) delete row['sequence_id'];
    if (!row['repertoire_id']) row['repertoire_id'] = rep['repertoire_id'];
    if (row['repertoire_id'].length == 0) row['repertoire_id'] = rep['repertoire_id'];
    if (!row['data_processing_id'])
        row['data_processing_id'] = dp_id;
    if (row['data_processing_id'].length == 0)
        row['data_processing_id'] = dp_id;
    row['vdjserver_load_set'] = load_set

    // change V gene calls to an array, add gene and subgroup
    if ((typeof row['v_call']) == 'string') {
        var fields = row['v_call'].split(',');
        if (fields.length > 1) {
            row["v_call"] = fields;
            var genes = [];
            var subgroups = []
            for (var i = 0; i < fields.length; ++i) {
                var c = fields[i];
                var result = parseGene(c);
                if (!result) {
                    genes.push(null);
                    subgroups.push(null);
                } else {
                    genes.push(result.gene);
                    subgroups.push(result.subgroup);
                }
            }
            row["v_gene"] = genes;
            row["v_subgroup"] = subgroups;
        } else {
            var result = parseGene(row['v_call']);
            if (result) {
                row["v_gene"] = result.gene;
                if (result.subgroup) row["v_subgroup"] = result.subgroup;
            }
        }
    } else if (row['v_call']) {
        var genes = [];
        var subgroups = []
        for (var i = 0; i < row['v_call'].length; ++i) {
            var c = row['v_call'][i];
            var result = parseGene(c);
            if (!result) {
                genes.push(null);
                subgroups.push(null);
            } else {
                genes.push(result.gene);
                subgroups.push(result.subgroup);
            }
        }
        row["v_gene"] = genes;
        row["v_subgroup"] = subgroups;
    }

    // change D gene calls to an array, add gene and subgroup
    if ((typeof row['d_call']) == 'string') {
        var fields = row['d_call'].split(',');
        if (fields.length > 1) {
            //printjson(fields);
            row["d_call"] = fields;
            var genes = [];
            var subgroups = []
            for (var i = 0; i < fields.length; ++i) {
                var c = fields[i];
                var result = parseGene(c);
                if (!result) {
                    genes.push(null);
                    subgroups.push(null);
                } else {
                    genes.push(result.gene);
                    subgroups.push(result.subgroup);
                }
            }
            row["d_gene"] = genes;
            row["d_subgroup"] = subgroups;
        } else {
            var result = parseGene(row['d_call']);
            if (result) {
                row["d_gene"] = result.gene;
                if (result.subgroup) row["d_subgroup"] = result.subgroup;
            }
        }
    } else if (row['d_call']) {
        var genes = [];
        var subgroups = []
        for (var i = 0; i < row['d_call'].length; ++i) {
            var c = row['d_call'][i];
            var result = parseGene(c);
            if (!result) {
                genes.push(null);
                subgroups.push(null);
            } else {
                genes.push(result.gene);
                subgroups.push(result.subgroup);
            }
        }
        row["d_gene"] = genes;
        row["d_subgroup"] = subgroups;
    }

    // change J gene calls to an array, add gene and subgroup
    if ((typeof row['j_call']) == 'string') {
        var fields = row['j_call'].split(',');
        if (fields.length > 1) {
            //printjson(fields);
            row["j_call"] = fields;
            var genes = [];
            var subgroups = []
            for (var i = 0; i < fields.length; ++i) {
                var c = fields[i];
                var result = parseGene(c);
                if (!result) {
                    genes.push(null);
                    subgroups.push(null);
                } else {
                    genes.push(result.gene);
                    subgroups.push(result.subgroup);
                }
            }
            row["j_gene"] = genes;
            row["j_subgroup"] = subgroups;
        } else {
            var result = parseGene(row['j_call']);
            if (result) {
                row["j_gene"] = result.gene;
                if (result.subgroup) row["j_subgroup"] = result.subgroup;
            }
        }
    } else if (row['j_call']) {
        var genes = [];
        var subgroups = []
        for (var i = 0; i < row['j_call'].length; ++i) {
            var c = row['j_call'][i];
            var result = parseGene(c);
            if (!result) {
                genes.push(null);
                subgroups.push(null);
            } else {
                genes.push(result.gene);
                subgroups.push(result.subgroup);
            }
        }
        row["j_gene"] = genes;
        row["j_subgroup"] = subgroups;
    }

    // junction substrings
    if (row['junction_aa']) {
        if (row['junction_aa'].length > 3) {
            //var result = getAllSubstrings(row['junction_aa'], 4);
            //row["vdjserver_junction_substrings"] = result;
            var result = getAllSuffixes(row['junction_aa'], 4);
            row["vdjserver_junction_suffixes"] = result;
        }
    }

    return;
}

mongoIO.processFile = async function(filename, rep, dp_id, dataLoad, load_set, load_set_start, loadCollection) {
    var context = 'mongoIO.processFile';
    var records = [];
    var rows = [];
    var total_cnt = 0;

    var schema = airr.get_schema('Rearrangement');
    //console.log(schema.spec('sequence_id'));

    var mapValues = function(map) {
        return schema.map_value(map);
    };

    return new Promise(function(resolve, reject) {

    var readable = fs.createReadStream(filename)
        .on('error', async function(e) {
            reject(e);
        })
        .pipe(zlib.createGunzip())
        .on('error', async function(e) {
            reject(e);
        })
        .pipe(csv({separator:'\t', mapValues: mapValues}))
        .on('error', async function(e) {
            reject(e);
        })
        .on('data', async function(row) {
            rows.push(row);
            if (rows.length == 10000) {
                // pause the stream while we insert the data
                readable.pause();

                if (load_set >= load_set_start) {
                    config.log.info(context, 'inserting load set: ' + load_set);
                    // process and cleanup records
                    for (var r = 0; r < rows.length; ++r) {
                        //if (r == 0) console.log(rows[r]);
                        mongoIO.processRearrangementRow(rows[r], rep, dp_id, load_set);
                        mongoIO.cleanObject(rows[r]);
                        records.push(rows[r]);
                    }

                    // perform the database insert
                    await mongoIO.insertRearrangement(records, loadCollection);

                    // update rearrangement data load record
                    var retry = false;
                    dataLoad['value']['load_set'] = load_set + 1;
                    await tapisIO.updateDocument(dataLoad.uuid, dataLoad.name, dataLoad.value)
                        .catch(function(error) {
                            var msg = 'tapisIO.updateDocument error: ' + error;
                            msg = config.log.error(context, msg);
                            webhookIO.postToSlack(msg);
                            retry = true;
                        });
                    if (retry) {
                        config.log.info(context, 'retrying updateDocument');
                        await tapisIO.updateDocument(dataLoad.uuid, dataLoad.name, dataLoad.value)
                            .catch(function(error) {
                                var msg = 'tapisIO.updateDocument error: ' + error;
                                msg = config.log.error(context, msg);
                                webhookIO.postToSlack(msg);
                                readable.destroy();
                                return reject(msg);
                            });
                    }
                } else {
                    config.log.info(context, 'skipping load set: ' + load_set);
                }
                total_cnt += records.length;
                ++load_set;
                records = [];
                rows = [];
                // resume the stream
                readable.resume();
            }
        })
        .on('end', async function() {
            if (rows.length > 0) {
                if (load_set >= load_set_start) {
                    config.log.info(context, 'end file, inserting load set: ' + load_set);
                    // process and cleanup records
                    for (var r = 0; r < rows.length; ++r) {
                        //if (r == 0) console.log(rows[r]);
                        mongoIO.processRearrangementRow(rows[r], rep, dp_id, load_set);
                        mongoIO.cleanObject(rows[r]);
                        records.push(rows[r]);
                    }

                    // perform the database insert
                    await mongoIO.insertRearrangement(records, loadCollection);

                    // update rearrangement data load record
                    var retry = false;
                    dataLoad['value']['load_set'] = load_set + 1;
                    await tapisIO.updateDocument(dataLoad.uuid, dataLoad.name, dataLoad.value)
                        .catch(function(error) {
                            var msg = 'tapisIO.updateDocument error: ' + error;
                            msg = config.log.error(context, msg);
                            webhookIO.postToSlack(msg);
                            retry = true;
                        });
                    if (retry) {
                        config.log.info(context, 'retrying updateDocument');
                        await tapisIO.updateDocument(dataLoad.uuid, dataLoad.name, dataLoad.value)
                            .catch(function(error) {
                                var msg = 'tapisIO.updateDocument error: ' + error;
                                msg = config.log.error(context, msg);
                                webhookIO.postToSlack(msg);
                                return reject(msg);
                            });
                    }

                } else {
                    config.log.info(context, 'end file, skipping load set: ' + load_set);
                }
                total_cnt += records.length;
                ++load_set;
                records = [];
                rows = [];
            }
            config.log.info(context, 'file successfully processed: ' + filename + ', rearrangement count: ' + total_cnt);
            return resolve(load_set);
        });
    });
}

// Delete all rearrangements for a repertoire_id or for
// just a given load_set.
mongoIO.deleteLoadSet = async function(repertoire_id, load_set, loadCollection) {
    var context = 'mongoIO.deleteLoadSet';

    config.log.info(context, 'repertoire: ' + repertoire_id + ' load set: ' + load_set);

    return new Promise(function(resolve, reject) {
        // get connection to database
        MongoClient.connect(mongoSettings.url, async function(err, db) {
            if (err) {
                var msg = "Could not connect to database: " + err;
                msg = config.log.error(context, msg);
                webhookIO.postToSlack(msg);
                reject(new Error(msg))
            } else {
                var v1airr = db.db(mongoSettings.dbname);
                var collection = v1airr.collection(loadCollection);

                // delete load_set for repertoire
                var filter = {"repertoire_id":repertoire_id}
                if (load_set != null)
                    if (load_set >= 0)
                        filter['vdjserver_load_set'] = {"$gte": load_set};

                //console.log(filter);

                var result = await collection.deleteMany(filter);
                config.log.info(context, 'deleted rearrangements: ' + result);
                db.close();
                resolve(result);
            }
        });
    });
}

// Insert rearrangement records
mongoIO.insertRearrangement = async function(records, loadCollection) {
    var context = 'mongoIO.insertRearrangement';

    return new Promise(function(resolve, reject) {
        // get connection to database
        const client = new MongoClient(mongoSettings.url, { socketTimeoutMS: 0 });
        client.connect(async function(err, db) {
            if (err) {
                var msg = "Could not connect to database: " + err;
                msg = config.log.error(context, msg);
                webhookIO.postToSlack(msg);
                reject(new Error(msg))
            } else {
                var v1airr = db.db(mongoSettings.dbname);
                //var collection = v1airr.collection('rearrangement');
                var collection = v1airr.collection(loadCollection);

                var result = await collection.insertMany(records);

                config.log.info(context, 'Inserted rearrangements: ' + JSON.stringify(result['result']));
                db.close();
                resolve(result);
            }
        });
    });
}

// Delete repertoire for given repertoire_id
mongoIO.deleteRepertoire = async function(repertoire_id, loadCollection) {
    var context = 'mongoIO.deleteRepertoire';

    return new Promise(function(resolve, reject) {
        // get connection to database
        MongoClient.connect(mongoSettings.url, async function(err, db) {
            if (err) {
                var msg = "Could not connect to database: " + err;
                msg = config.log.error(context, msg);
                webhookIO.postToSlack(msg);
                reject(new Error(msg))
            } else {
                var v1airr = db.db(mongoSettings.dbname);
                var collection = v1airr.collection(loadCollection);

                // delete than insert repertoire
                var filter = {"repertoire_id":repertoire_id}
                //console.log(filter);

                var result = await collection.deleteMany(filter);
                config.log.info(context, 'Deleted repertoire: ' + JSON.stringify(result));
                db.close();
                resolve(result);
            }
        });
    });
}

// Insert repertoire
mongoIO.insertRepertoire = async function(repertoire, loadCollection) {
    var context = 'mongoIO.insertRepertoire';

    return new Promise(function(resolve, reject) {
        // get connection to database
        MongoClient.connect(mongoSettings.url, async function(err, db) {
            if (err) {
                var msg = "Could not connect to database: " + err;
                msg = config.log.error(context, msg);
                webhookIO.postToSlack(msg);
                reject(new Error(msg))
            } else {
                var v1airr = db.db(mongoSettings.dbname);
                var collection = v1airr.collection(loadCollection);

                mongoIO.cleanObject(repertoire);

                // do insert
                var result = await collection.insertOne(repertoire);
                config.log.info(context, 'Inserted repertoire: ' + JSON.stringify(result['result']));
                db.close();
                resolve(result);
            }
        });
    });
}

//
// Load a set of repertoire metadata objects
//
mongoIO.loadRepertoireMetadata = async function(repertoireMetadata, collection) {
    var loadCollection = 'repertoire' + collection;
    for (var i in repertoireMetadata) {
        var rep = repertoireMetadata[i];
        var result = await mongoIO.deleteRepertoire(rep['repertoire_id'], loadCollection);
        result = await mongoIO.insertRepertoire(rep, loadCollection);
    }
}

//
// Load rearrangement data for a repertoire
//
mongoIO.loadRearrangementData = async function(dataLoad, repertoire, primaryDP, jobOutput) {
    var context = 'mongoIO.loadRearrangementData';
    var filePath = '/vdjZ' + jobOutput['archivePath'];
    var files = primaryDP['data_processing_files'];
    var dp_id = primaryDP['data_processing_id'];
    var loadCollection = 'rearrangement' + dataLoad['value']['collection'];
    var load_set_start = dataLoad['value']['load_set'];
    var load_set = 0;
    var total_cnt = 0;

    // delete starting load set in case it has partial records
    await mongoIO.deleteLoadSet(repertoire['repertoire_id'], load_set_start, loadCollection);

    // loop through files and load
    for (var i = 0; i < files.length; ++i) {
        var filename = filePath + '/' + files[i];
        config.log.info(context, 'processing file: ' + filename + ' load set start: ' + load_set_start);

        var result = await mongoIO.processFile(filename, repertoire, dp_id, dataLoad, load_set, load_set_start, loadCollection)
            .catch(function(error) {
                // pass reject to next level
                return Promise.reject(error);
            });
        load_set = result;
        //console.log(result);
    }

    // update rearrangement data load record
    dataLoad['value']['isLoaded'] = true;
    await tapisIO.updateDocument(dataLoad.uuid, dataLoad.name, dataLoad.value)
        .catch(function(error) {
            var msg = 'tapisIO.updateDocument error: ' + error;
            msg = config.log.error(context, msg);
            webhookIO.postToSlack(msg);
            return Promise.reject(msg);
        });
}

//
// Unload rearrangement data for a repertoire
//
mongoIO.unloadRearrangementData = async function(dataLoad, repertoire) {
    var loadCollection = 'rearrangement' + dataLoad['value']['collection'];

    // delete all rearrangements for repertoire
    return mongoIO.deleteLoadSet(repertoire['repertoire_id'], null, loadCollection);
}

mongoIO.performAsyncQueryToFile = async function(metadata, filename) {
    var context = 'mongoIO.performQueryToFile';
    var facets = false;
    var body = metadata['value']['body'];

    return new Promise(function(resolve, reject) {

        // the query should have already been constructed upon submission request so we don't expect any errors at this point.
        // TODO: if async API every used for more than rearrangements, this needs to be parameterized
        let airr_schema = airr.get_schema('Rearrangement')['definition'];
        let error = { message: '' };
        let query = adc_mongo_query.constructQueryOperation(airr, airr_schema, body['filters'], error, false, true);
        let parsed_query = JSON.parse(query);
        let from = null;
        if (body['from'] != null) from = body['from'];
        let size = null;
        if (body['size'] != null) size = body['size'];
        let format = null;
        if (body['format'] != null) format = body['format'];

        // AIRR fields
        var all_fields = [];
        if (body['include_fields']) {
            airr.collectFields(airr_schema, body['include_fields'], all_fields, null);
        }
        // collect all AIRR schema fields
        var schema_fields = [];
        airr.collectFields(airr_schema, 'airr-schema', schema_fields, null);
    
        // field projection
        var projection = {};
        if (body['fields'] != undefined) {
            var fields = body['fields'];
            for (let i = 0; i < fields.length; ++i) {
                if (fields[i] == '_id') continue;
                if (fields[i] == '_etag') continue;
                projection[fields[i]] = 1;
            }
            projection['_id'] = 1;
    
            // add AIRR required fields to projection
            // NOTE: projection will not add a field if it is not already in the document
            // so below after the data has been retrieved, missing fields need to be
            // added with null values.
            if (all_fields.length > 0) {
                for (var r in all_fields) projection[all_fields[r]] = 1;
            }
    
            // add to field list so will be put in response if necessary
            for (let i = 0; i < fields.length; ++i) {
                if (fields[i] == '_id') continue;
                all_fields.push(fields[i]);
            }
        }

        // determine TSV headers
        var headers = [];
        if (format == 'tsv') {
            // if no projection
            if (Object.keys(projection).length == 0) {
                // then return all schema fields
                headers = schema_fields;
            } else {
                // else only return specified fields
                // schema fields first
                for (let p = 0; p < schema_fields.length; ++p) {
                    if (projection[schema_fields[p]]) headers.push(schema_fields[p]);
                }
                // add custom fields on end
                for (let p in projection) {
                    if (p == '_id') continue;
                    if (projection[p]) {
                        if (schema_fields.indexOf(p) >= 0) continue;
                        else headers.push(p);
                    }
                }
            }
        }

        // Open read/write streams
        config.log.info(context, 'writing to file: ' + filename);
        var writable = fs.createWriteStream(filename)
            .on('error', function(e) { let msg = config.log.error(context, 'caught error: ' + e); return reject(new Error(msg)); });

        writable.on('finish', function() {
            config.log.info(context, 'finish of write stream');
            //return resolve(filename);
        });

                // process the stream
//                readable.pipe(transform)
//                    .on('error', function(e) { let msg = config.log.error(context, 'caught error: ' + e); return reject(new Error(msg)); })
//                    .pipe(writable)
//                    .on('finish', function() {
//                        config.log.info(context, 'end of stream');
//                        writable.end();
//                    });

/*
                readable.pipe(zlib.createGunzip())
                    .pipe(transform)
                    .on('error', function(e) { console.log('caught error'); console.log(e); return reject(e); })
                    .pipe(zlib.createGzip())
                    .pipe(writable)
                    .on('finish', function() {
                        console.log('end of stream');
                        writable.end();
                    });
*/


        var first = true;
        var cnt = 0;
        var endpoint_process = endpoint_map[metadata['value']['endpoint']];
        return MongoClient.connect(mongoSettings.url, async function(err, db) {
            if (err) {
                var msg = "Could not connect to database: " + err;
                msg = config.log.error(context, msg);
                webhookIO.postToSlack(msg);
                return reject(new Error(msg))
            }

            var v1airr = db.db(mongoSettings.dbname);
            var collection = v1airr.collection(metadata['value']['collection']);
            config.log.info(context, JSON.stringify(parsed_query));

            // perform a normal query
            var cursor = collection.find(parsed_query);
            if (from) cursor.skip(from);
            if (size) cursor.limit(size);
            if (projection) cursor.project(projection);
            while (await cursor.hasNext()) {
                var entry = await cursor.next();
                cnt += 1;
    
                // data cleanup
                endpoint_process.cleanRecord(entry, airr_schema, projection, all_fields);
                //config.log.info(context, 'entry');
    
                // write data
                switch (format) {
                    case 'tsv':
                        if (first) {
                            writable.write(headers.join('\t'));
                            writable.write('\n');
                        } else {
                            let vals = [];
                            for (let i = 0; i < headers.length; ++i) {
                                let p = headers[i];
                                //if (config.debug) console.log(p, entry[p]);
                                if (entry[p] == undefined) vals.push('');
                                else vals.push(entry[p]);
                            }
                            writable.write(vals.join('\t'));
                            writable.write('\n');
                        }
                        break;
                    case 'jsonarray':
                        break;
                    case 'json':
                    default:
                        if (first) {
                            writable.write('{"Info":' + JSON.stringify(config.info) + ',"Rearrangement": [\n');
                        } else writable.write(',\n')
                        writable.write(JSON.stringify(entry));
                        break;
                }
                first = false;
            }
            config.log.info(context, 'records written: ' + cnt);

            db.close();
            if (format == 'json') writable.write(']}\n');
            else writable.write('\n');
            writable.end();
            return resolve(cnt);
        });
    });
}
