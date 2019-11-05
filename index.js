const MongoClient = require("mongodb").MongoClient;
const elasticsearch = require('elasticsearch');
const chalk = require("chalk");
const delay = require("delay");

const options = {
  poolSize: 50,
  keepAlive: 15000,
  socketTimeoutMS: 6000000,
  connectTimeoutMS: 15000,
  useNewUrlParser: true,
  useUnifiedTopology: true
};

// Connection MongoDB URL
const url = "mongodb://localhost:27017/dbName";

// ElasticSearch Client
const esClient = new elasticsearch.Client({
    host: 'localhost:9200',
  });

const getAllCollections = db =>
  new Promise((resolve, reject) => {
    db.listCollections().toArray(function(err, collInfos) {
      if (err) {
        reject(err);
      }
      resolve(collInfos);
    });
  });

const getDocument = (db, collections) =>
  new Promise((resolve, reject) => {
    const allRecords = db
      .collection(collections)
      .find({})
      .toArray();
    if (!allRecords) {
      reject("Error Mongo", allRecords);
    }

    resolve(allRecords);
  });

(async () => {
  const client = await MongoClient.connect(url, options);

  if (!client) {
    return db;
  }

  const db = await client.db();

  const allCollection = await getAllCollections(db);
  console.log('Collection Total : ', allCollection.length + '\n')
  for (let index = 0; index < allCollection.length; index++) {
    const collection = allCollection[index];
    console.log(`Progress ${chalk.green(index+1)} from ${chalk.green(allCollection.length)}`)
    console.log(`Getting document from collection : `, collection.name);
    await delay(5000);
    const document = await getDocument(db, collection.name);
    console.log(`Total document from collection : `, document.length);
    console.log(`Try inserting document ${chalk.green(collection.name)} to elasticsearch.`)
    try{
        let insertToES;
        
        for (let indexx = 0; indexx < document.length; indexx++) {
            const doc = document[indexx];
            console.log(`Progress uploading document ${collection.name} ${chalk.green(indexx+1)} from ${chalk.green(document.length)}`)
            delete doc._id
            const body = [{ index: { _index: collection.name, _type:'doc',} }, doc]
            insertToES = await esClient.bulk({body: body});
        }
        console.log(JSON.stringify(insertToES));
    }catch(e){
        console.log('\n')
        console.log(`Failed in progress  ${chalk.red(index+1)} from ${chalk.green(allCollection.length)}`)
        console.log(`Error info : ${e}`)
        process.exit()
    }
  }
})();
