import { SynorError, MigrationRecord } from '@synor/core';
import { performance } from 'perf_hooks';
import { MongoClient, Db } from 'mongodb';
import { ConnectionString } from 'connection-string';

type DatabaseEngine = import('@synor/core').DatabaseEngine;
type DatabaseEngineFactory = import('@synor/core').DatabaseEngineFactory;
type MigrationSource = import('@synor/core').MigrationSource;

async function noOp(): Promise<null> {
  await Promise.resolve(null);
  return null;
}

function parseConnectionString(
  cn: string
): {
  protocol: string;
  host: string;
  port: number;
  user: string;
  password: string;
  path: string[];
  params: Record<string, any>;
  database: string;
  migrationRecordTable: string;
} {
  const {
    protocol,
    hostname: host,
    port,
    user,
    password,
    path,
    params
  } = new ConnectionString(cn, {
    params: {
      synor_migration_record_table: 'synor_migration_record'
    }
  });

  if (!host || !protocol || !port || !user || !password || !path || !params) {
    throw new SynorError('Invalid connection uri');
  }
  const database = path?.[0];
  const migrationRecordTable = params.synor_migration_record_table;

  return {
    database,
    host,
    protocol,
    port,
    user,
    password,
    path,
    params,
    migrationRecordTable
  };
}

async function ensureMigrationRecordTableExists(
  db: Db,
  mgCollName: string
): Promise<void> {
  await db.createCollection(mgCollName);
}

async function deleteDirtyRecords(db: Db, mgCollName: string): Promise<void> {
  await db.collection(mgCollName).deleteMany({
    dirty: true
  });
}

async function updateRecord(
  db: Db,
  mgCollName: string,
  { id, hash }: { id: number; hash: string }
): Promise<void> {
  await db.collection(mgCollName).updateOne(
    {
      id
    },
    {
      $set: {
        hash
      }
    }
  );
}

export const MongoDbEngine: DatabaseEngineFactory = (
  uri,
  { baseVersion, getAdvisoryLockId, getUserInfo }
): DatabaseEngine => {
  if (typeof getAdvisoryLockId !== 'function') {
    throw new SynorError(`Missing: getAdvisoryLockId`);
  }

  if (typeof getUserInfo !== 'function') {
    throw new SynorError(`Missing: getUserInfo`);
  }

  const { database, migrationRecordTable } = parseConnectionString(uri);

  let client: MongoClient | null = null;
  let db: Db | null = null;

  return {
    async open() {
      client = await MongoClient.connect(uri);
      db = await client.db(database);
      await ensureMigrationRecordTableExists(db, migrationRecordTable);
    },
    async close() {
      if (client) {
        await client.close();
      }
    },
    async lock() {
      await noOp();
    },
    async unlock() {
      await noOp();
    },
    async drop() {
      if (!db) {
        throw new SynorError('Database connection is null');
      }
      const collections = await (
        await db.listCollections(
          {},
          {
            nameOnly: true
          }
        )
      ).toArray();

      await Promise.all(
        collections.map(async c => {
          if (db) {
            await db.dropCollection(c);
          }
        })
      );
    },
    async run({ version, type, title, hash, run }: MigrationSource) {
      let dirty = false;

      const startTime = performance.now();
      try {
        if (run && db) {
          await run({
            db
          });
        } else {
          throw new SynorError('Run function is null');
        }
      } catch (err) {
        dirty = true;
        console.error('error trying to run migration');
        throw err;
      } finally {
        const endTime = performance.now();
        await db?.collection(migrationRecordTable).insert({
          version,
          type,
          title,
          hash,
          appliedAt: new Date(),
          appliedBy: '',
          executionTime: endTime - startTime,
          dirty
        });
      }
    },
    async repair(records) {
      if (!db) {
        throw new SynorError('Database connection is null');
      }
      await deleteDirtyRecords(db, migrationRecordTable);

      for (const { id, hash } of records) {
        await updateRecord(db, migrationRecordTable, { id, hash });
      }
    },
    async records(startid: number) {
      if (!db) {
        throw new SynorError('Database connection is null');
      }

      const records = (await (
        await db.collection(migrationRecordTable).find({
          id: {
            $gte: startid
          }
        })
      ).toArray()) as MigrationRecord[];
      return records;
    }
  };
};
