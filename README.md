## Simple tool to copy data between DynamoDB tables

# Usage

```js
const { execute } = require("dynamodb-data-copy");

const result = await execute({
  srcTableName: "SRC",
  dstTableName: "TGT",
  region: "eu-west-1"
});
// { ProcessedItems : 100 }
```

This has only been tested for relatively small number of records. Probably is not good enough as is for larger volumes of data, hence the roadmap below.

# Roadmap

- Error retries and exponential back off
- Parallel Writes
- User configured read throughput
- User configured write throughput
- User configured read consistency for scan

# Tests

`npm test`
