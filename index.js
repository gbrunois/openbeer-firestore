var csv = require("fast-csv");

const admin = require("firebase-admin");

var serviceAccount = require("./serviceAccountKey.json");

async function readCsv(csvPath) {
  const result = [];
  const p = new Promise((resolve, reject) => {
    csv
      .fromPath(csvPath, {
        headers: true
      })
      .on("data", function(data) {
        result.push(
          Object.keys(data)
            .filter(key => key !== "")
            .reduce(
              (newObject, key) =>
                Object.assign(newObject, { [key]: data[key] }),
              {}
            )
        );
      })
      .on("end", function() {
        resolve();
      });
  });
  return p.then(() => result);
}

function chunkArray(chunkSize, array) {
  return array.reduce(function(previous, current) {
    var chunk;
    if (
      previous.length === 0 ||
      previous[previous.length - 1].length === chunkSize
    ) {
      chunk = [];
      previous.push(chunk);
    } else {
      chunk = previous[previous.length - 1];
    }
    chunk.push(current);
    return previous;
  }, []);
}

function insertCollection(db, items, collectionName, baseRef) {
  const chuncks = chunkArray(100, items);

  chuncks.forEach(chunck => {
    // Begin a new batch
    const batch = db.batch();
    chunck.forEach(item => {
      const ref = baseRef.collection(collectionName).doc(item.id);
      batch.set(ref, item);
    });
    batch
      .commit()
      .then(res => {
        console.log("Batch successfully executed!");
      })
      .catch(error => {
        console.error("Error executing batch: ", error);
      });
  });
}

async function insertAsync() {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });

  const db = admin.firestore();

  const breweries = await readCsv("./openbeerdb_csv/breweries.csv");
  const beers = await readCsv("./openbeerdb_csv/beers.csv");
  const styles = await readCsv("./openbeerdb_csv/styles.csv");
  const categories = await readCsv("./openbeerdb_csv/categories.csv");

  insertCollection(db, styles, "styles", db);
  insertCollection(db, categories, "categories", db);
  insertCollection(db, breweries, "breweries", db);

  breweries.forEach(brewery => {
    const breweryRef = db.collection("breweries").doc(brewery.id);
    const allBeersInBreweries = beers.filter(
      beer => beer.brewery_id === brewery.id
    );

    console.log(
      `inserting ${allBeersInBreweries.length} beers in ${brewery.id}`
    );

    insertCollection(db, allBeersInBreweries, "beers", breweryRef);
  });
}

insertAsync();
