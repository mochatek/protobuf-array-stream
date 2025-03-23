import test from "node:test";
import assert from "node:assert/strict";
import { Writable } from "stream";
import { RepeatedFieldStream } from "../src";

// Test message definitions
const proto = `
syntax = "proto3";

// Native types - packed by default
message Int32Message {
  repeated int32 numbers = 1;
}

message Int64Message {
  repeated int64 numbers = 1;
}

message Uint32Message {
  repeated uint32 numbers = 1;
}

message Uint64Message {
  repeated uint64 numbers = 1;
}

message Sint32Message {
  repeated sint32 numbers = 1;
}

message Sint64Message {
  repeated sint64 numbers = 1;
}

message BoolMessage {
  repeated bool flags = 1;
}

message Fixed32Message {
  repeated fixed32 numbers = 1;
}

message Fixed64Message {
  repeated fixed64 numbers = 1;
}

message Sfixed32Message {
  repeated sfixed32 numbers = 1;
}

message Sfixed64Message {
  repeated sfixed64 numbers = 1;
}

message FloatMessage {
  repeated float numbers = 1;
}

message DoubleMessage {
  repeated double numbers = 1;
}

// String and bytes - not packed
message StringMessage {
  repeated string texts = 1;
}

message BytesMessage {
  repeated bytes data = 1;
}

// Explicitly non-packed numeric
message NonPackedInt32Message {
  repeated int32 numbers = 1 [packed = false];
}

// Enum type
enum Color {
  RED = 0;
  GREEN = 1;
  BLUE = 2;
}

message EnumMessage {
  repeated Color colors = 1;
}

// Embedded message type
message Point {
  int32 x = 1;
  int32 y = 2;
}

message PointsMessage {
  repeated Point points = 1;
}
`;

// Helper function to collect stream chunks
async function collectChunks<T>(
  stream: RepeatedFieldStream,
  message: Uint8Array,
): Promise<T[]> {
  return new Promise((resolve, reject) => {
    const chunks: T[] = [];
    stream.on("data", (chunk) => chunks.push(chunk as T));
    stream.on("error", reject);
    stream.on("end", () => resolve(chunks));
    stream.write(message);
    stream.end();
  });
}

// Test native packed numeric types
test("should decode packed int32", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("Int32Message");
  const stream = new RepeatedFieldStream({ root, messageName: "Int32Message" });
  const message = Message.encode({ numbers: [1, -2, 3] }).finish();
  const chunks = await collectChunks<number>(stream, message);
  assert.deepEqual(chunks, [1, -2, 3]);
});

test("should decode packed int64", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("Int64Message");
  const stream = new RepeatedFieldStream({ root, messageName: "Int64Message" });
  const message = Message.encode({ numbers: [1, -2, 3] }).finish();
  const chunks = await collectChunks<any>(stream, message);
  assert.deepEqual(
    chunks.map((c) => c.toNumber()),
    [1, -2, 3],
  );
});

test("should decode packed uint32", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("Uint32Message");
  const stream = new RepeatedFieldStream({
    root,
    messageName: "Uint32Message",
  });
  const message = Message.encode({ numbers: [1, 2, 3] }).finish();
  const chunks = await collectChunks<number>(stream, message);
  assert.deepEqual(chunks, [1, 2, 3]);
});

test("should decode packed bool", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("BoolMessage");
  const stream = new RepeatedFieldStream({ root, messageName: "BoolMessage" });
  const message = Message.encode({ flags: [true, false, true] }).finish();
  const chunks = await collectChunks<boolean>(stream, message);
  assert.deepEqual(chunks, [true, false, true]);
});

test("should decode packed float", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("FloatMessage");
  const stream = new RepeatedFieldStream({ root, messageName: "FloatMessage" });
  const message = Message.encode({ numbers: [1.5, -2.5, 3.14] }).finish();
  const chunks = await collectChunks<number>(stream, message);
  // Float has less precision than double, so we need to compare with some tolerance
  assert.ok(Math.abs(chunks[0] - 1.5) < 0.000001);
  assert.ok(Math.abs(chunks[1] - -2.5) < 0.000001);
  assert.ok(Math.abs(chunks[2] - 3.14) < 0.000001);
});

test("should decode packed double", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("DoubleMessage");
  const stream = new RepeatedFieldStream({
    root,
    messageName: "DoubleMessage",
  });
  const message = Message.encode({ numbers: [1.5, -2.5, Math.PI] }).finish();
  const chunks = await collectChunks<number>(stream, message);
  assert.deepEqual(chunks, [1.5, -2.5, Math.PI]);
});

test("should decode packed fixed32", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("Fixed32Message");
  const stream = new RepeatedFieldStream({
    root,
    messageName: "Fixed32Message",
  });
  const message = Message.encode({ numbers: [1, 2, 3] }).finish();
  const chunks = await collectChunks<number>(stream, message);
  assert.deepEqual(chunks, [1, 2, 3]);
});

test("should decode packed fixed64", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("Fixed64Message");
  const stream = new RepeatedFieldStream({
    root,
    messageName: "Fixed64Message",
  });
  const message = Message.encode({ numbers: [1, 2, 3] }).finish();
  const chunks = await collectChunks<any>(stream, message);
  assert.deepEqual(
    chunks.map((c) => c.toNumber()),
    [1, 2, 3],
  );
});

test("should decode packed sfixed32", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("Sfixed32Message");
  const stream = new RepeatedFieldStream({
    root,
    messageName: "Sfixed32Message",
  });
  const message = Message.encode({ numbers: [1, -2, 3] }).finish();
  const chunks = await collectChunks<number>(stream, message);
  assert.deepEqual(chunks, [1, -2, 3]);
});

test("should decode packed sfixed64", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("Sfixed64Message");
  const stream = new RepeatedFieldStream({
    root,
    messageName: "Sfixed64Message",
  });
  const message = Message.encode({ numbers: [1, -2, 3] }).finish();
  const chunks = await collectChunks<any>(stream, message);
  assert.deepEqual(
    chunks.map((c) => c.toNumber()),
    [1, -2, 3],
  );
});

// Test non-packed types
test("should decode string field", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("StringMessage");
  const stream = new RepeatedFieldStream({
    root,
    messageName: "StringMessage",
  });
  const message = Message.encode({ texts: ["a", "b"] }).finish();
  const chunks = await collectChunks<string>(stream, message);
  assert.deepEqual(chunks, ["a", "b"]);
});

test("should decode bytes field", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("BytesMessage");
  const stream = new RepeatedFieldStream({ root, messageName: "BytesMessage" });
  const data = [Buffer.from([1, 2, 3]), Buffer.from([4, 5, 6])];
  const message = Message.encode({ data }).finish();
  const chunks = await collectChunks<Uint8Array>(stream, message);
  assert.deepEqual(chunks, data);
});

// Test explicitly non-packed numeric
test("should decode non-packed int32", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("NonPackedInt32Message");
  const stream = new RepeatedFieldStream({
    root,
    messageName: "NonPackedInt32Message",
  });
  const message = Message.encode({ numbers: [1, 2, 3] }).finish();
  const chunks = await collectChunks<number>(stream, message);
  assert.deepEqual(chunks, [1, 2, 3]);
});

// Test enum type
test("should decode enum field", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("EnumMessage");
  const stream = new RepeatedFieldStream({ root, messageName: "EnumMessage" });
  const message = Message.encode({ colors: [0, 1, 2] }).finish();
  const chunks = await collectChunks<number>(stream, message);
  assert.deepEqual(chunks, [0, 1, 2]);
});

// Test embedded message type
test("should decode embedded message field", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("PointsMessage");
  const stream = new RepeatedFieldStream({
    root,
    messageName: "PointsMessage",
  });
  const points = [
    { x: 1, y: 2 },
    { x: 3, y: 4 },
  ];
  const message = Message.encode({ points }).finish();
  const chunks = await collectChunks<{ x: number; y: number }>(stream, message);
  assert.deepEqual(chunks, points);
});

// Test edge cases
test("should handle empty arrays", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("Int32Message");
  const stream = new RepeatedFieldStream({ root, messageName: "Int32Message" });
  const message = Message.encode({ numbers: [] }).finish();
  const chunks = await collectChunks<number>(stream, message);
  assert.deepEqual(chunks, []);
});

test("should handle unicode strings", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("StringMessage");
  const stream = new RepeatedFieldStream({
    root,
    messageName: "StringMessage",
  });
  const texts = ["Hello 世界", "🌟 Star", "\u0000 Null char"];
  const message = Message.encode({ texts }).finish();
  const chunks = await collectChunks<string>(stream, message);
  assert.deepEqual(chunks, texts);
});

test("should handle large numbers", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("Int64Message");
  const stream = new RepeatedFieldStream({ root, messageName: "Int64Message" });
  const numbers = [Number.MAX_SAFE_INTEGER, Number.MIN_SAFE_INTEGER];
  const message = Message.encode({ numbers }).finish();
  const chunks = await collectChunks<any>(stream, message);
  assert.deepEqual(
    chunks.map((c) => c.toNumber()),
    numbers,
  );
});

test("should handle large arrays of embedded messages", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("PointsMessage");
  const stream = new RepeatedFieldStream({
    root,
    messageName: "PointsMessage",
  });
  const points = Array.from({ length: 10000 }, (_, i) => ({ x: i, y: i * 2 }));
  const message = Message.encode({ points }).finish();
  const chunks = await collectChunks<{ x: number; y: number }>(stream, message);
  assert.deepEqual(chunks, points);
});

test("should handle sint32 zigzag encoding", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("Sint32Message");
  const stream = new RepeatedFieldStream({
    root,
    messageName: "Sint32Message",
  });
  const numbers = [-1, -64, -1024, -12345];
  const message = Message.encode({ numbers }).finish();
  const chunks = await collectChunks<number>(stream, message);
  assert.deepEqual(chunks, numbers);
});

test("should handle sint64 zigzag encoding", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("Sint64Message");
  const stream = new RepeatedFieldStream({
    root,
    messageName: "Sint64Message",
  });
  const numbers = [-1, -64, -1024, -12345, -(2 ** 32)];
  const message = Message.encode({ numbers }).finish();
  const chunks = await collectChunks<any>(stream, message);
  assert.deepEqual(
    chunks.map((c) => c.toNumber()),
    numbers,
  );
});

test("should handle chunked input", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("Int32Message");
  const stream = new RepeatedFieldStream({ root, messageName: "Int32Message" });
  const message = Message.encode({ numbers: [1, 2, 3] }).finish();

  return new Promise<void>((resolve, reject) => {
    const chunks: number[] = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("error", reject);
    stream.on("end", () => {
      assert.deepEqual(chunks, [1, 2, 3]);
      resolve();
    });

    // Write message in chunks
    const chunkSize = 2;
    for (let i = 0; i < message.length; i += chunkSize) {
      stream.write(message.subarray(i, i + chunkSize));
    }
    stream.end();
  });
});

// Test error cases
test("should throw on non-existent message", () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  assert.throws(
    () => new RepeatedFieldStream({ root, messageName: "NonExistentMessage" }),
    /does not exist in the proto file/,
  );
});

test("should throw on empty message", () => {
  const emptyProto = `
    syntax = "proto3";
    message EmptyMessage {}
  `;
  const root = RepeatedFieldStream.loadRootFromSource(emptyProto);
  assert.throws(
    () => new RepeatedFieldStream({ root, messageName: "EmptyMessage" }),
    /does not have any fields/,
  );
});

test("should throw on message with multiple fields", () => {
  const multiFieldProto = `
    syntax = "proto3";
    message MultiFieldMessage {
      repeated int32 numbers = 1;
      string name = 2;
    }
  `;
  const root = RepeatedFieldStream.loadRootFromSource(multiFieldProto);
  assert.throws(
    () => new RepeatedFieldStream({ root, messageName: "MultiFieldMessage" }),
    /should only have one field/,
  );
});

test("should throw on non-repeated field", () => {
  const nonRepeatedProto = `
    syntax = "proto3";
    message NonRepeatedMessage {
      int32 number = 1;
    }
  `;
  const root = RepeatedFieldStream.loadRootFromSource(nonRepeatedProto);
  assert.throws(
    () => new RepeatedFieldStream({ root, messageName: "NonRepeatedMessage" }),
    /must be 'repeated'/,
  );
});

test("should handle backpressure with slow consumer", async () => {
  const root = RepeatedFieldStream.loadRootFromSource(proto);
  const Message = root.lookupType("Int32Message");
  const stream = new RepeatedFieldStream({ root, messageName: "Int32Message" });

  const numbers = Array.from({ length: 10 }, (_, i) => i);
  const message = Message.encode({ numbers }).finish();

  return new Promise<void>((resolve, reject) => {
    const chunks: number[] = [];

    // Create a writable stream with artificial delay
    const slowWriter = new Writable({
      objectMode: true,
      highWaterMark: 2,
      write(chunk: number, _encoding: string, callback: Function) {
        chunks.push(chunk);
        setTimeout(callback, 10);
      },
    });

    slowWriter.on("finish", () => {
      assert.deepEqual(chunks, numbers);
      resolve();
    });

    slowWriter.on("error", reject);
    stream.on("error", reject);

    stream.pipe(slowWriter);
    stream.end(message);
  });
});
