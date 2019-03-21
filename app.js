let express = require('express');
let fs = require('fs');
let cassandra = require('cassandra-driver');

/* ====== Initialize objects ====== */
let app = express();
let client = new cassandra.Client({
    contactPoints: ['cass1.fit.fraunhofer.de', 'cass2.fit.fraunhofer.de', 'cass3.fit.fraunhofer.de'],
    keyspace: 'polycare'
});
let files = fs.readdirSync('./series-000001/');

/* ====== Deal with each file ====== */
for (let file in files) {
    let name = './series-000001/' + files[file];
    let buf = fs.readFileSync(name);

    /* ====== Save file data to db and then write to new file ====== */
    if (buf !== null) {
        const insertQuery = 'INSERT INTO "Media" (id, type, content) VALUES (?, ?, ?)';
        const insertParams = [String(parseInt(file) + 1), 'image', {data: buf}];
        client.execute(insertQuery, insertParams, {prepare: true})
            .then(result => {
                const selectQuery = 'SELECT * FROM "Media" WHERE id = ?';
                const selectParams = [String(parseInt(file) + 1)];
                client.execute(selectQuery, selectParams)
                    .then(result => {
                        fs.writeFile('./series-output/' + files[file], result.rows[0].content.data,
                            err => {
                                if (err) throw err;
                            });
                    })
                    .catch(err => console.log(err));
            })
            .catch(err => console.log(err));
    }
}
console.log('Done!');

module.exports = app;
