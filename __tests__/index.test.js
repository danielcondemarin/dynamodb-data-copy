const { execute } = require('../dynamo-db-import-export-tool');

const awsPromisify = (mockFunction, resolvedValue = undefined) =>
  mockFunction.mockReturnValue({
    promise: () => Promise.resolve(resolvedValue)
  });

const awsPromisifyOnce = (mockFunction, resolvedValue = undefined) =>
  mockFunction.mockReturnValueOnce({
    promise: () => Promise.resolve(resolvedValue)
  });

function MockDynamoDBConstructor () { }
function MockDocumentClientConstructor () { }
MockDynamoDBConstructor.DocumentClient = MockDocumentClientConstructor;
const mockScan = jest.fn();
const mockBatchWrite = jest.fn();
MockDynamoDBConstructor.DocumentClient.prototype.scan = awsPromisify(mockScan);
MockDynamoDBConstructor.DocumentClient.prototype.batchWrite = awsPromisify(mockBatchWrite);

jest.mock('aws-sdk', () => ({
  DynamoDB: MockDynamoDBConstructor
}));

describe('dynamodb import / export tool', () => {
  afterEach(() => {
    mockScan.mockClear();
    mockBatchWrite.mockClear();
  });

  it('copies one item from source table to destination table', async () => {
    const item = {
      id: '123',
      x: 'y'
    };

    awsPromisify(mockScan, {
      Count: 1,
      Items: [item]
    });

    const batchWriteResult = {
      UnprocessedItems: {}
    };

    awsPromisify(mockBatchWrite, batchWriteResult);

    const result = await execute({
      srcTableName: 'SourceTable',
      dstTableName: 'DestinationTable'
    });

    expect(result).toEqual({ ProcessedItems: 1 });
    expect(mockScan).toBeCalledWith({
      TableName: 'SourceTable'
    });
    expect(mockBatchWrite).toBeCalledWith({
      RequestItems: {
        'DestinationTable': [{
          PutRequest: {
            Item: item
          }
        }]
      }
    });
  });

  it('copies multiple items from source table to destination table', async () => {
    const itemOne = {
      id: '123',
      x: 'y'
    };

    const itemTwo = {
      id: '456',
      b: 'c'
    };

    awsPromisify(mockScan, {
      Count: 1,
      Items: [itemOne, itemTwo]
    });

    const batchWriteResult = {
      UnprocessedItems: {}
    };

    awsPromisify(mockBatchWrite, batchWriteResult);

    const result = await execute({
      srcTableName: 'SourceTable',
      dstTableName: 'DestinationTable'
    });

    expect(result).toEqual({ ProcessedItems: 2 });
    expect(mockScan).toBeCalledWith({
      TableName: 'SourceTable'
    });
    expect(mockBatchWrite).toBeCalledWith({
      RequestItems: {
        'DestinationTable': [{
          PutRequest: {
            Item: itemOne
          }
        }, {
          PutRequest: {
            Item: itemTwo
          }
        }]
      }
    });
  });

  it('handles scan paginated results', async () => {
    const firstPageItem = {
      id: '123',
      x: 'y'
    };

    const secondPageItem = {
      id: '456',
      a: 'b'
    };

    awsPromisifyOnce(mockScan, {
      Count: 1,
      Items: [firstPageItem],
      LastEvaluatedKey: firstPageItem
    });

    awsPromisifyOnce(mockScan, {
      Count: 1,
      Items: [secondPageItem]
    });

    const batchWriteResult = {
      UnprocessedItems: {}
    };

    awsPromisify(mockBatchWrite, batchWriteResult);

    const result = await execute({
      srcTableName: 'SourceTable',
      dstTableName: 'DestinationTable'
    });

    expect(result).toEqual({ ProcessedItems: 2 });
    expect(mockScan).toBeCalledWith({
      TableName: 'SourceTable'
    });
    expect(mockScan).toBeCalledWith({
      TableName: 'SourceTable',
      ExclusiveStartKey: firstPageItem
    });
    expect(mockBatchWrite).toBeCalledWith({
      RequestItems: {
        'DestinationTable': [{
          PutRequest: {
            Item: firstPageItem
          }
        }, {
          PutRequest: {
            Item: secondPageItem
          }
        }]
      }
    });
  });

  it('handles unprocessed items from batch write', async () => {
    const itemOne = {
      id: '123',
      x: 'y'
    };

    const itemTwo = {
      id: '456',
      x: 'y'
    };

    awsPromisify(mockScan, {
      Count: 1,
      Items: [itemOne, itemTwo]
    });

    const firstBatchWriteResult = {
      UnprocessedItems: {
        'DestinationTable': [{
          PutRequest: {
            Item: itemTwo
          }
        }]
      }
    };

    const secondBatchWriteResult = {
      UnprocessedItems: {}
    };

    awsPromisifyOnce(mockBatchWrite, firstBatchWriteResult);
    awsPromisifyOnce(mockBatchWrite, secondBatchWriteResult);

    const result = await execute({
      srcTableName: 'SourceTable',
      dstTableName: 'DestinationTable'
    });

    expect(result).toEqual({ ProcessedItems: 2 });
    expect(mockBatchWrite).toBeCalledWith({
      RequestItems: {
        'DestinationTable': [{
          PutRequest: {
            Item: itemOne
          }
        }, {
          PutRequest: {
            Item: itemTwo
          }
        }]
      }
    });
    expect(mockBatchWrite).toBeCalledWith({
      RequestItems: {
        'DestinationTable': [{
          PutRequest: {
            Item: itemTwo
          }
        }]
      }
    });
  });

  it('creates separate batch writes more than 25 items', async () => {
    const items = [];

    for (let i = 0; i < 50; i++) {
      const item = {
        id: `123-${i}`,
        x: 'y'
      };
      items.push(item);
    }

    awsPromisify(mockScan, {
      Count: 1,
      Items: items
    });

    const batchWriteResult = {
      UnprocessedItems: {}
    };

    awsPromisify(mockBatchWrite, batchWriteResult);

    const result = await execute({
      srcTableName: 'SourceTable',
      dstTableName: 'DestinationTable'
    });

    expect(result).toEqual({ ProcessedItems: 50 });
    expect(mockScan).toBeCalledWith({
      TableName: 'SourceTable'
    });
    // calls twice in batches of 25 items
    expect(mockBatchWrite).toBeCalledTimes(2);
  });
});
