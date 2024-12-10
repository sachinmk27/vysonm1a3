import sqlite3 from "sqlite3";
import { faker } from "@faker-js/faker";
import sql from "./sql.js";
import queries from "./queries.js";

const ONE_MILLION = 1_000_000;
const TOTAL_MEASUREMENTS = 10000 * ONE_MILLION;
const BATCH_SIZE = 10000;

sqlite3.verbose();
const db = new sqlite3.Database("my.db");

db.on("trace", (sql) => {
  console.log(`Executing SQL: ${sql.slice(0, 50)}`);
});

// Enable profiling: logs the SQL statement and the time it took to execute
db.on("profile", (sql, time) => {
  console.log(`SQL: ${sql.slice(0, 50)} took ${time} ms to execute.`);
});

function createMeasurement() {
  const firstName = faker.person.firstName();
  const lastName = faker.person.lastName();
  return {
    name: `${firstName} ${lastName}`,
    feet: Math.floor(Math.random() * 999) + 1,
    inches: Math.floor(Math.random() * 12),
  };
}

async function insertMeasurements(measurements) {
  const values = measurements.map(() => `(?, ?, ?)`);
  const insertStmt = db.prepare(
    `INSERT INTO measurements (name, feet, inches) VALUES ${values}`
  );
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run(`BEGIN TRANSACTION`, (err) => {
        if (err) reject(err);
      });
      insertStmt.run(
        measurements.map((m) => [m.name, m.feet, m.inches]).flat(),
        (err) => {
          if (err) reject(err);
        }
      );
      insertStmt.finalize((err) => {
        if (err) reject(err);
      });
      db.run(`COMMIT`, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  });
}

function runtotalInchesMigration() {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run("BEGIN TRANSACTION", (err) => {
        if (err) reject(err);
      });
      db.run(
        "ALTER TABLE measurements ADD COLUMN total_inches INTEGER;",
        (err) => {
          if (err) reject(err);
        }
      );
      db.run(
        "UPDATE measurements SET total_inches = (feet * 12) + inches;",
        (err) => {
          if (err) reject(err);
        }
      );
      db.run("ALTER TABLE measurements DROP COLUMN feet;", (err) => {
        if (err) reject(err);
      });
      db.run("ALTER TABLE measurements DROP COLUMN inches;", (err) => {
        if (err) reject(err);
      });
      db.run("COMMIT", (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  });
}

async function main() {
  await sql.run(db, "PRAGMA synchronous = off");
  await sql.run(db, queries.DROP_MEASUREMENTS_TABLE);
  await sql.run(db, queries.CREATE_MEASUREMENTS_TABLE);
  console.time("insert measurements");
  for (let i = 0; i < TOTAL_MEASUREMENTS / BATCH_SIZE; i++) {
    const measurements = faker.helpers.multiple(createMeasurement, {
      count: BATCH_SIZE,
    });
    await insertMeasurements(measurements);
    console.log(`Inserted ${i + 1}/ ${TOTAL_MEASUREMENTS / BATCH_SIZE}`);
  }
  console.timeEnd("insert measurements");
  console.time("migrations");
  await runtotalInchesMigration();
  console.timeEnd("migrations");
  await db.close();
}

main();
