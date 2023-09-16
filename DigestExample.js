const sql = require('mssql');
const { TDigest } = require('tdigest');

async function main() {

    SQL_CONFIG.user = "SA";
    SQL_CONFIG.password = "Root#Pass";
    SQL_CONFIG.server = "localhost";
    SQL_CONFIG.port = 1433;
    SQL_CONFIG.database = 'master'

    const tableName = "TDigests";

    let pool = await getDBPool();

    // Initialize a T-Digest data structure
    const digest = new TDigest();

    // Add 100000 random records to the T-Digest
    for (let i = 0; i < 100000; i++) {
        digest.push(getRandomInt(10, 3000));
    }
    digest.compress();
    console.log("90th Percentile before saving: " + digest.percentile(0.9));
    const ps = new sql.PreparedStatement(pool);
    ps.input('digest', sql.NVarChar); // When we are storing the data we will compress it

    await ps.prepare(`INSERT INTO ${tableName} VALUES (COMPRESS(@digest))`);
    await ps.execute({ 'digest': JSON.stringify(digest.toArray()) });

    console.log("Ok we are done saving the digest, Now let's read and construct it back")

    const getDigestQuery = `SELECT CAST(DECOMPRESS(digest) AS NVARCHAR(MAX)) AS digest 
                            FROM ${tableName}`;

    const result = await pool.query(getDigestQuery);

    result.recordset.forEach(element => {
        let newDigest = new TDigest()
        newDigest.push_centroid(JSON.parse(element.digest));
        console.log("90th Percentile after constructing: " + newDigest.percentile(0.9));
    });

    ps.unprepare();
    pool.close();
}

// Function to generate a random integer between min and max (inclusive)
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

const SQL_CONFIG = {
    user: '',
    password: '',
    database: '',
    server: '',
    port: 1433,
    pool: {
      max: 10,
      min: 0,
      idleTimeoutMillis: 60000
    },
    options: {
      encrypt: false,
      trustServerCertificate: false,
      trustedConnection: false,
    }
  }
  
  function getDBPool() {
    const poolPromise = new sql.ConnectionPool(SQL_CONFIG)
    .connect()
    .then(pool => {
      console.log('Connected to MSSQL');
      return pool
    })
    return poolPromise;
  }

if (require.main === module) {
    main();
}
