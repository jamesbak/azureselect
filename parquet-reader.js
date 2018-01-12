var parquet = require('parquetjs');

async function readParquet(filename) {
    // create new ParquetReader that reads from 'fruits.parquet`
    let reader = await parquet.ParquetReader.openFile(filename);

    // create a new cursor
    let cursor = reader.getCursor();

    // read all records from the file and print them
    let record = null;
    while (record = await cursor.next()) {
    console.log(record);
    }
}

readParquet(process.argv[2])
    .then(v => console.log('finished'))
    .catch(err => {
        console.error('Error: ' + err);
    });