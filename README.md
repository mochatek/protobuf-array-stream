# protobuf-array-stream

[![npm version](https://img.shields.io/npm/v/protobuf-array-stream.svg)](https://www.npmjs.com/package/protobuf-array-stream)
[![npm downloads](https://img.shields.io/npm/dm/protobuf-array-stream.svg)](https://www.npmjs.com/package/protobuf-array-stream)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Stream and decode Protocol Buffer arrays without memory issues. While other libraries need the entire message in memory, this one processes array elements directly from the source stream - making it perfect for large datasets.

## Why Use This?

- 💾 **Memory Efficient**: No need to load entire message in memory
- 🌊 **Streaming**: Process array elements directly from source (file, network, etc.)
- 🎯 **Specialized**: Built for messages with a repeated field (array)
- ⚡ **Scalable**: Works with arrays of any size, memory usage stays constant

## Installation

```bash
npm install protobuf-array-stream
```

## Usage

### Loading Protocol Definitions

First, you need to load your Protocol Buffer definitions. The library exposes these methods directly from protobufjs for convenience:

```typescript
import RepeatedFieldStream from "protobuf-array-stream";
import { createReadStream } from "fs";

// 1. Load from a .proto file (async)
const root1 = await RepeatedFieldStream.loadRootFromFile(
  "path/to/messages.proto",
);

// 2. Load from a .proto file (sync)
const root2 = RepeatedFieldStream.loadRootFromFileSync(
  "path/to/messages.proto",
);

// 3. Load from a proto definition string
const root3 = RepeatedFieldStream.loadRootFromSource(`
  syntax = "proto3";
  
  message NumberList {
    repeated int32 numbers = 1;
  }
`);

// 4. Load from JSON schema
const root4 = RepeatedFieldStream.loadRootFromJSON({
  nested: {
    NumberList: {
      fields: {
        numbers: {
          id: 1,
          rule: "repeated",
          type: "int32",
        },
      },
    },
  },
});
```

### Real-World Example

You're building an anti-cheat system for your massively multiplayer game. Players are generating TONS of data:

```protobuf
syntax = "proto3";

message GameLog {
  repeated PlayerAction actions = 1;
}

message PlayerAction {
  int64 timestamp = 1;
  string player_id = 2;
  string action = 3;
  Position position = 4;
}

message Position {
  float x = 1;
  float y = 2;
  float z = 3;
}
```

Process large game session logs efficiently:

```typescript
const sessionStream = createReadStream("game_session.bin");

const decodeStream = new RepeatedFieldStream({
  root,
  messageName: "GameLog",
});

const analyzeStream = new Transform({
  objectMode: true,
  transform(action, _, callback) {
    const { timestamp, playerId, action: move, position } = action;

    if (move === "shoot" && position.y > 500) {
      console.log(
        `Detected: ${playerId} at height ${position.y} on ${new Date(timestamp.toNumber())}`,
      );
    }

    callback(null, action);
  },
});

sessionStream.pipe(decodeStream).pipe(analyzeStream);
```

### Error Handling

The stream emits errors in these cases:

- Message type not found in proto definitions
- Message has no fields or multiple fields
- Field is not a repeated field
- Invalid protobuf encoding in input

```typescript
stream.on("error", (error) => {
  console.error("Stream error:", error.message);
});
```

## API

### `new RepeatedFieldStream(options)`

Creates a new transform stream for processing Protocol Buffer repeated fields.

#### Options

- `root`: Root namespace from protobufjs containing message definitions
- `messageName`: Name of the message type to decode

### Static Methods

#### `loadRootFromFile(path: string): Promise<Root>`

Asynchronously loads Protocol Buffer definitions from a .proto file.

- `path`: Path to the .proto file
- Returns: Promise that resolves to the root namespace

#### `loadRootFromFileSync(path: string): Root`

Synchronously loads Protocol Buffer definitions from a .proto file.

- `path`: Path to the .proto file
- Returns: Root namespace

#### `loadRootFromSource(source: string): Root`

Loads Protocol Buffer definitions from a string containing proto definitions.

- `source`: String containing the proto definitions
- Returns: Root namespace

#### `loadRootFromJSON(json: object): Root`

Loads Protocol Buffer definitions from a JSON object.

- `json`: JSON object containing the proto definitions
- Returns: Root namespace

## Requirements

- Node.js >= 16.0.0
- Protocol Buffer messages must contain exactly one repeated field

## License

MIT License - see the [LICENSE](https://github.com/mochatek/protobuf-array-stream/blob/main/LICENSE) file for details
