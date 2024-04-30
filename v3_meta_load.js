'use strict';

//
// v3_meta_load.js
// Load records into Tapis meta collection
//
// VDJServer
// http://vdjserver.org
//
// Copyright (C) 2023 The University of Texas Southwestern Medical Center
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

var tapisV3 = require('./tapisV3');
const events = require('events');
const fs = require('fs');
const readline = require('readline');

const processLineByLine = async function(filename) {
  try {
    const rl = readline.createInterface({
      input: fs.createReadStream(filename),
      crlfDelay: Infinity
    });

    rl.on('line', function(line) {
      console.log(`Line from file: ${line}`);
      let data = JSON.parse(line);
      delete data['_id'];
      rl.pause();

      tapisV3.createRecord('tapis_meta', data).then(function() {
          rl.resume();
        });
    });

    await events.once(rl, 'close');

    console.log('Reading file line by line with readline done.');
    const used = process.memoryUsage().heapUsed / 1024 / 1024;
    console.log(`The script uses approximately ${Math.round(used * 100) / 100} MB`);
  } catch (err) {
    console.error(err);
  }
};

if (process.argv.length != 4) {
    console.error('Usage: node v3_meta_load.js json_file collection');
    process.exit(1);
}

tapisV3.serviceAccount.getToken().then(function(t) {
    console.log(tapisV3.serviceAccount);
    console.log(process.argv[2]);

    processLineByLine(process.argv[2]).then(function() {
        console.log('File loaded');
    });
});
