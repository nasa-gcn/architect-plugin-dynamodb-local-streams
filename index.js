// index.ts
import functions from "@architect/functions";
import { DescribeTableCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DescribeStreamCommand,
  DynamoDBStreamsClient,
  GetRecordsCommand,
  GetShardIteratorCommand,
  TrimmedDataAccessException
} from "@aws-sdk/client-dynamodb-streams";
var shardMap = {};
var sandbox = {
  // @ts-expect-error: The Architect plugins API has no type definitions.
  async start({ inventory: { inv }, invoke }) {
    const tableStreams = inv["tables-streams"];
    const db = await functions.tables();
    const dynamodbClient = new DynamoDBClient({
      region: inv.aws.region,
      endpoint: `http://localhost:${process.env.ARC_TABLES_PORT}`
    });
    const ddbStreamsClient = new DynamoDBStreamsClient({
      region: inv.aws.region,
      endpoint: `http://localhost:${process.env.ARC_TABLES_PORT}`
    });
    for (const arcStream of tableStreams) {
      shardMap[arcStream.table] = [];
      const tableName = db.name(arcStream.table);
      const table = await dynamodbClient.send(
        new DescribeTableCommand({
          TableName: tableName
        })
      );
      const stream = await ddbStreamsClient.send(
        new DescribeStreamCommand({
          StreamArn: table.Table?.LatestStreamArn
        })
      );
      if (stream.StreamDescription?.Shards && table.Table?.LatestStreamArn) {
        for (const shard of stream.StreamDescription?.Shards) {
          if (shard.ShardId) {
            const ShardIterator = (await ddbStreamsClient.send(
              new GetShardIteratorCommand({
                StreamArn: table.Table.LatestStreamArn,
                ShardIteratorType: "LATEST",
                ShardId: shard.ShardId
              })
            )).ShardIterator;
            if (ShardIterator) {
              shardMap[arcStream.table].push({
                ShardIterator
              });
            }
          }
        }
      }
    }
    while (true) {
      await sleep(2e3);
      for (const key of Object.keys(shardMap)) {
        console.log("Polling: ", key);
        if (shardMap[key].length) {
          const shardItem = shardMap[key].pop();
          if (!shardItem) continue;
          try {
            const records = await ddbStreamsClient.send(
              new GetRecordsCommand({
                ShardIterator: shardItem.ShardIterator
              })
            );
            if (records.Records?.length) {
              console.log("Invoking: ");
              invoke({
                pragma: "tables-streams",
                name: key,
                payload: records
              });
            }
            if (records.NextShardIterator) {
              shardMap[key].push({
                ShardIterator: records.NextShardIterator
              });
            }
          } catch (error) {
            if (error instanceof TrimmedDataAccessException) {
              console.log(error.name);
            }
            const table = await dynamodbClient.send(
              new DescribeTableCommand({
                TableName: db.name(key)
              })
            );
            const stream = await ddbStreamsClient.send(
              new DescribeStreamCommand({
                StreamArn: table.Table?.LatestStreamArn
              })
            );
            if (stream.StreamDescription?.Shards && table.Table?.LatestStreamArn) {
              for (const shard of stream.StreamDescription?.Shards) {
                if (shard.ShardId) {
                  const ShardIterator = (await ddbStreamsClient.send(
                    new GetShardIteratorCommand({
                      StreamArn: table.Table.LatestStreamArn,
                      ShardIteratorType: "LATEST",
                      ShardId: shard.ShardId
                    })
                  )).ShardIterator;
                  if (ShardIterator) {
                    shardMap[key].push({
                      ShardIterator
                    });
                  }
                }
              }
            }
          }
        }
      }
    }
  }
};
async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
export {
  sandbox
};
