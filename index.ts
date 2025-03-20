/*!
 * Copyright © 2023 United States Government as represented by the
 * Administrator of the National Aeronautics and Space Administration.
 * All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

import functions from '@architect/functions';
import { DescribeTableCommand, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DescribeStreamCommand,
  DynamoDBStreamsClient,
  GetRecordsCommand,
  GetShardIteratorCommand,
  TrimmedDataAccessException,
} from '@aws-sdk/client-dynamodb-streams';

type ShardItem = {
  ShardIterator: string;
};

const shardMap: { [key: string]: ShardItem[] } = {};

export const credentials = {
  // Any credentials can be provided for local
  accessKeyId: 'localDb',
  secretAccessKey: 'randomAnyString',
};

// @ts-expect-error: The Architect plugins API has no type definitions.
function isEnabled(inv) {
  return Boolean(
    inv._project.preferences?.sandbox?.['external-db'] ||
      process.env.ARC_DB_EXTERNAL
  );
}

// @ts-expect-error: The Architect plugins API has no type definitions.
function getPort(inv) {
  return (
    inv._project.preferences?.sandbox?.ports?.tables ||
    Number(process.env.ARC_TABLES_PORT)
  );
}
export const sandbox = {
  // @ts-expect-error: The Architect plugins API has no type definitions.
  async start({ inventory: { inv }, invoke }) {
    if (!isEnabled(inv)) {
      console.log(
        'ARC_DB_EXTERNAL is not set. To use the local dynamodb table streams, set ARC_DB_EXTERNAL to `true` and defined a port with ARC_TABLES_PORT in your .env file.'
      );
      return;
    }
    const tableStreams = inv['tables-streams'];
    const dynamodbClient = new DynamoDBClient({
      region: inv.aws.region,
      endpoint: `http://localhost:${getPort(inv)}`,
      credentials,
    });
    const ddbStreamsClient = new DynamoDBStreamsClient({
      region: inv.aws.region,
      endpoint: `http://localhost:${getPort(inv)}`,
      credentials,
    });

    for (const arcStream of tableStreams) {
      shardMap[arcStream.table] = [];
      await resetTableStreams(
        dynamodbClient,
        ddbStreamsClient,
        arcStream.table
      );
    }

    while (true) {
      await sleep(2000);
      for (const key of Object.keys(shardMap)) {
        if (shardMap[key].length) {
          const shardItem = shardMap[key].pop();
          if (!shardItem) continue;
          try {
            const event = await ddbStreamsClient.send(
              new GetRecordsCommand({
                ShardIterator: shardItem.ShardIterator,
              })
            );
            if (event.Records?.length) {
              invoke({
                pragma: 'tables-streams',
                name: key,
                payload: event,
              });
            }

            if (event.NextShardIterator) {
              shardMap[key].push({
                ShardIterator: event.NextShardIterator,
              });
            }
          } catch (error) {
            if (error instanceof TrimmedDataAccessException) {
              console.log(error.name);
            }

            await resetTableStreams(dynamodbClient, ddbStreamsClient, key);
          }
        }
      }
    }
  },
};

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function resetTableStreams(
  ddbClient: DynamoDBClient,
  ddbStreamsClient: DynamoDBStreamsClient,
  arcTableName: string
) {
  const db = await functions.tables();
  const tableName = db.name(arcTableName);
  const table = await ddbClient.send(
    new DescribeTableCommand({
      TableName: tableName,
    })
  );
  const stream = await ddbStreamsClient.send(
    new DescribeStreamCommand({
      StreamArn: table.Table?.LatestStreamArn,
    })
  );
  if (stream.StreamDescription?.Shards && table.Table?.LatestStreamArn) {
    for (const shard of stream.StreamDescription?.Shards) {
      if (shard.ShardId) {
        const ShardIterator = (
          await ddbStreamsClient.send(
            new GetShardIteratorCommand({
              StreamArn: table.Table.LatestStreamArn,
              ShardIteratorType: 'LATEST',
              ShardId: shard.ShardId,
            })
          )
        ).ShardIterator;

        if (ShardIterator) {
          shardMap[arcTableName].push({
            ShardIterator: ShardIterator,
          });
        }
      }
    }
  }
}
