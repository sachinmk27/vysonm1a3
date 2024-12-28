import sqlite3 from "sqlite3";
import { faker } from "@faker-js/faker";
import sql from "./sql.js";
import queries from "./queries.js";

const ONE_MILLION = 1_000_000;
const TOTAL_MEASUREMENTS = 1 * ONE_MILLION;
const BATCH_SIZE = 10000;

sqlite3.verbose();
const db = new sqlite3.Database("my.db");

db.on("trace", (sql) => {
  console.log(`Executing SQL: ${sql.slice(0, 100)}`);
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
        "UPDATE measurements SET total_inches = (feet * 12) + inches WHERE total_inches IS NULL",
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

async function runMeasurements() {
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
}

function createUser() {
  const firstName = faker.person.firstName();
  const lastName = faker.person.lastName();
  return {
    name: `${firstName} ${lastName}`,
    email: faker.internet.email({ firstName: firstName, lastName }),
  };
}

async function insertUsers(users) {
  const values = users.map(() => `(?, ?)`);
  const insertStmt = db.prepare(
    `INSERT INTO users (name, email) VALUES ${values}`
  );
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run(`BEGIN TRANSACTION`, (err) => {
        if (err) reject();
      });
      insertStmt.run(
        users.map((user) => [user.name, user.email]).flat(),
        (err) => {
          if (err) reject(err);
        }
      );
      insertStmt.finalize((err) => {
        if (err) reject();
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

function createPost() {
  return {
    content: faker.lorem.sentence({ min: 3, max: 8 }),
  };
}

async function insertPosts(posts) {
  const values = posts.map(() => `(?, ?)`);
  const insertStmt = db.prepare(
    `INSERT INTO posts (content, user_id) VALUES ${values}`
  );
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run(`BEGIN TRANSACTION`, (err) => {
        if (err) reject(err);
      });
      insertStmt.run(
        posts.map((post) => [post.content, post.user_id]).flat(),
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

async function insertLikes(likes) {
  const values = likes.map(() => `(?, ?)`);
  const insertStmt = db.prepare(
    `INSERT INTO likes (post_id, user_id) VALUES ${values}`
  );
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run(`BEGIN TRANSACTION`, (err) => {
        if (err) reject(err);
      });
      insertStmt.run(
        likes.map((like) => [like.post_id, like.user_id]).flat(),
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

async function insertReactionsV1(reactions) {
  const values = reactions.map(() => `(?, ?, ?)`);
  const insertStmt = db.prepare(
    `INSERT INTO reactions (post_id, user_id, reaction) VALUES ${values}`
  );
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run(`BEGIN TRANSACTION`, (err) => {
        if (err) reject(err);
      });
      insertStmt.run(
        reactions.map((r) => [r.post_id, r.user_id, r.reaction]).flat(),
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

function runAddReactionsMigrationsV1() {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run(queries.BEGIN_TRANSACTION, (err) => {
        if (err) reject(err);
      });
      db.run(queries.DROP_REACTIONS_TABLE, (err) => {
        if (err) reject(err);
      });
      db.run(queries.CREATE_REACTIONS_TABLE, (err) => {
        if (err) reject(err);
      });
      db.run(queries.INSERT_FROM_LIKES_TO_REACTIONS, (err) => {
        if (err) reject(err);
      });
      db.run(queries.COMMIT_TRANSACTION, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  });
}

function runAddSupportAndFunnyReactionsMigrationsV2() {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run(queries.BEGIN_TRANSACTION, (err) => {
        if (err) reject(err);
      });
      db.run(queries.DROP_REACTION_TYPES_TABLE, (err) => {
        if (err) reject(err);
      });
      db.run(queries.DROP_NEW_REACTIONS_TABLE, (err) => {
        if (err) reject(err);
      });
      db.run(queries.CREATE_REACTION_TYPES_TABLE, (err) => {
        if (err) reject(err);
      });
      db.run(queries.CREATE_NEW_REACTIONS_TABLE, (err) => {
        if (err) reject(err);
      });
      db.run(queries.INSERT_REACTION_TYPES, (err) => {
        if (err) reject(err);
      });
      db.run(queries.INSERT_FROM_REACTIONS_TO_NEW_REACTIONS, (err) => {
        if (err) reject(err);
      });
      db.run(queries.COMMIT_TRANSACTION, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  });
}

async function runRemoveCuriousReactionMigrationsV3() {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run(queries.BEGIN_TRANSACTION, (err) => {
        if (err) reject(err);
      });
      db.run(queries.ALTER_REACTIONS_TABLE_ADD_SOFT_DELETE_COLUMN, (err) => {
        if (err) reject(err);
      });
      db.run(queries.UPDATE_REMOVE_CURIOUS_REACTION, (err) => {
        if (err) reject(err);
      });
      db.run(queries.COMMIT_TRANSACTION, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  });
}

async function runReorderFunnyReactionToEndMigrationsV4() {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run(queries.BEGIN_TRANSACTION, (err) => {
        if (err) reject(err);
      });
      db.run(queries.UPDATE_MOVE_FUNNY_REACTION_TO_END, (err) => {
        if (err) reject(err);
      });
      db.run(queries.COMMIT_TRANSACTION, (err) => {
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
  //   await runMeasurements();

  await sql.run(db, queries.DROP_USERS_TABLE);
  await sql.run(db, queries.CREATE_USERS_TABLE);
  await sql.run(db, queries.DROP_POSTS_TABLE);
  await sql.run(db, queries.CREATE_POSTS_TABLE);
  await sql.run(db, queries.DROP_LIKES_TABLE);
  await sql.run(db, queries.CREATE_LIKES_TABLE);
  await sql.run(db, queries.DROP_REACTIONS_TABLE);
  const TOTAL_USERS = 10;
  const TOTAL_POSTS = 1000;
  const users = faker.helpers.multiple(createUser, {
    count: TOTAL_USERS,
  });

  await insertUsers(users);
  const posts = faker.helpers
    .multiple(createPost, {
      count: TOTAL_POSTS,
    })
    .map((post) => ({
      ...post,
      user_id: Math.floor(Math.random() * TOTAL_USERS) + 1,
    }));
  await insertPosts(posts);

  const likes = faker.helpers.multiple(
    () => ({
      post_id: Math.floor(Math.random() * TOTAL_POSTS) + 1,
      user_id: Math.floor(Math.random() * TOTAL_USERS) + 1,
    }),
    { count: 2 }
  );
  await insertLikes(likes);
  await runAddReactionsMigrationsV1();
  const reactions = faker.helpers.multiple(
    () => ({
      post_id: Math.floor(Math.random() * TOTAL_POSTS) + 1,
      user_id: Math.floor(Math.random() * TOTAL_USERS) + 1,
      reaction: faker.helpers.arrayElement([
        "like",
        "celebrate",
        "love",
        "insightful",
        "curious",
      ]),
    }),
    { count: 10000 }
  );
  await insertReactionsV1(reactions);
  await runAddSupportAndFunnyReactionsMigrationsV2();
  await runRemoveCuriousReactionMigrationsV3();
  await runReorderFunnyReactionToEndMigrationsV4();
  await db.close();
}

main();
