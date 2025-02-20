# Architect plugin for Local DynamoDB Streams

This is a plugin for invoking stream functions defined in the `@tables-streams` pragma alongside a local DynamoDB instance.

When using Architect's sandbox mode, the plugin will consistently poll the local database for events with records. If records are found, it will invoke the function associated with the table.

## Prerequisites

- [@nasa-gcn/architect-plugin-dynamodb-local](https://github.com/nasa-gcn/architect-plugin-dynamodb-local) or another means of running a local dynamodb instance
- Node.js (if this project is built as a Node.js tool).

## Usage

1. Install this package using npm:

```
npm i @nasa-gcn/architect-plugin-dynamodb-local-streams
```

2. Add the following to your project's `app.arc` configuration file:

```
@plugins
nasa-gcn/architect-plugin-dynamodb-local-streams
```

## Known issues

There is an issue with the Docker image of DynamoDB local that causes a TrimmedDataAccessException error to be thrown on the first read. This problem lies deeper within DynamoDB and is out of the scope of this plugin. To handle this issue, there is a reset function that will trigger automatically. In practice, this means that the 1st invocation will fail (you will see a logged `TrimmedDataAccessException` message in your console), but the following invocations will work successfully.
