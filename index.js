const AWS = require("aws-sdk");

const arrayChunks = (items, chunkSize = 25) => {
  const chunks = [];

  for (let i = 0; i < items.length; i += chunkSize) {
    const tmp = items.slice(i, i + chunkSize);
    chunks.push(tmp);
  }

  return chunks;
};

module.exports = {
  execute: async ({ srcTableName, dstTableName, region }) => {
    const client = new AWS.DynamoDB.DocumentClient({
      region
    });

    const recursiveScan = async exclusiveStartKey => {
      const scanResult = await client
        .scan({
          TableName: srcTableName,
          ExclusiveStartKey: exclusiveStartKey
        })
        .promise();

      const items = scanResult.Items;

      if (scanResult.LastEvaluatedKey) {
        console.log(
          "Scan paginated result found. Next scan exclusive start key: ",
          scanResult.LastEvaluatedKey
        );
        return items.concat(await recursiveScan(scanResult.LastEvaluatedKey));
      }

      return items;
    };

    const recursiveBatchWrites = async putItems => {
      const chunks = arrayChunks(putItems);
      let processedItems = 0;

      for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        const batchWriteResult = await client
          .batchWrite({
            RequestItems: {
              [dstTableName]: chunk
            }
          })
          .promise();

        processedItems += chunk.length;

        const unprocessedItems = batchWriteResult.UnprocessedItems;

        if (unprocessedItems && unprocessedItems[dstTableName]) {
          console.log(
            `${
              unprocessedItems[dstTableName].length
            } unprocessed item(s) found. Will retry batch writing them.`
          );
          await recursiveBatchWrites(unprocessedItems[dstTableName]);
        }
      }

      return processedItems;
    };

    const scannedItems = await recursiveScan();

    const putItems = scannedItems.map(item => {
      return {
        PutRequest: { Item: item }
      };
    });

    return {
      ProcessedItems: await recursiveBatchWrites(putItems)
    };
  }
};
