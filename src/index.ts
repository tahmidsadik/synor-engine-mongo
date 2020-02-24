import { SynorError, MigrationRecord } from '@synor/core';
// import { performance } from 'perf_hooks';
import { MongoClient, Db } from 'mongodb';
import { ConnectionString } from 'connection-string';

type DatabaseEngine = import('@synor/core').DatabaseEngine;
type DatabaseEngineFactory = import('@synor/core').DatabaseEngineFactory;
type MigrationSource = import('@synor/core').MigrationSource;
type EngineInput = import('@synor/core').EngineInput;

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

export const MongoDbEngine = async ({
  uri,
  helpers: { baseVersion, getAdvisoryLockId, getUserInfo }
}: EngineInput): Promise<DatabaseEngine> => {
  if (typeof getAdvisoryLockId !== 'function') {
    throw new SynorError(`Missing: getAdvisoryLockId`);
  }

  if (typeof getUserInfo !== 'function') {
    throw new SynorError(`Missing: getUserInfo`);
  }

  const { database, migrationRecordTable } = parseConnectionString(uri);

  const client = await MongoClient.connect(uri);
  const db = await client.db(database);

  return {
    async open() {
      await ensureMigrationRecordTableExists(db, migrationRecordTable);
    },
    async close() {
      await client.close();
    },
    async lock() {
      await noOp();
    },
    async unlock() {
      await noOp();
    },
    async drop() {
      const collections = await (
        await db.listCollections(
          {},
          {
            nameOnly: true
          }
        )
      ).toArray();

      await Promise.all(collections.map(async c => db.dropCollection(c)));
    },
    async run(migration: MigrationSource) {
      await noOp();
    },
    async repair(records) {
      await deleteDirtyRecords(db, migrationRecordTable);

      for (const { id, hash } of records) {
        await updateRecord(db, migrationRecordTable, { id, hash });
      }
    },
    async records(startid: number) {
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
