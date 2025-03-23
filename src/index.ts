import { Transform, TransformCallback } from "stream";
import * as protobuf from "protobufjs";
import { DATA_TYPES, WIRE_TYPES, MASKS, TOKENS, ERRORS } from "./constants";
import RepeatedFieldStreamError from "./errors";

/**
 * A transform stream for processing Protocol Buffer repeated fields one element at a time.
 * This stream accepts a Protocol Buffer message containing exactly one repeated field
 * and emits each element of that field as a separate data event.
 *
 * @example
 * ```typescript
 * // Load proto definitions
 * const root = RepeatedFieldStream.loadRootFromFileSync('path/to/proto');
 
 * // Create stream
 * const stream = new RepeatedFieldStream({
 *   root,
 *   messageName: "YourMessageName"
 * });
 *
 * stream.on('data', (datum) => {
 *   console.log('Received:', datum);
 * });
 *
 * stream.end(message);
 * ```
 */
class RepeatedFieldStream<T = unknown> extends Transform {
  private repeatedField: ReturnType<
    typeof RepeatedFieldStream.getRepeatedFieldDetails
  >;
  private currentPayloadSize: number;
  private buffer: Buffer;
  private tag: number | null;
  private readOffset: number;
  private nextToken: number;

  /**
   * Creates a new RepeatedFieldStream.
   *
   * @param {Object} options - Configuration options
   * @param {protobuf.Root} options.root - Root namespace from protobufjs containing message definitions
   * @param {string} options.messageName - Name of the message type to decode. Must be a message with exactly one repeated field.
   * @throws {RepeatedFieldStreamError} If the message type doesn't exist
   * @throws {RepeatedFieldStreamError} If the message has no fields
   * @throws {RepeatedFieldStreamError} If the message has more than one field
   * @throws {RepeatedFieldStreamError} If the field is not marked as repeated
   */
  constructor(options: { root: protobuf.Root; messageName: string }) {
    super({ readableObjectMode: true, writableObjectMode: false });
    const { root, messageName } = options;

    RepeatedFieldStream.validateMessage(root, messageName);

    this.repeatedField = RepeatedFieldStream.getRepeatedFieldDetails(
      root,
      messageName,
    );
    this.currentPayloadSize = RepeatedFieldStream.getPayloadSizeInBytes(
      this.repeatedField.wireType,
    );
    this.buffer = Buffer.alloc(0);
    this.tag = null;
    this.readOffset = 0;
    this.nextToken = TOKENS.TAG;
  }

  /**
   * Exposes protobuf.loadSync from protobufjs to load Protocol Buffer definitions from a file synchronously.
   * @returns {protobuf.Root} Root namespace
   */
  static loadRootFromFileSync = protobuf.loadSync;

  /**
   * Exposes protobuf.load from protobufjs to load Protocol Buffer definitions from a file asynchronously.
   * @returns {Promise<protobuf.Root>} Root namespace
   */
  static loadRootFromFile = protobuf.load;

  /**
   * Exposes protobuf.Root.fromJSON from protobufjs to load Protocol Buffer definitions from a JSON object.
   * @returns {protobuf.Root} Root namespace
   */
  static loadRootFromJSON = protobuf.Root.fromJSON;

  /**
   * Exposes protobuf.parse from protobufjs to load Protocol Buffer definitions from a string.
   * @returns {protobuf.Root} Root namespace
   */
  static loadRootFromSource = (
    source: string,
    options?: protobuf.IParseOptions,
  ): protobuf.Root => protobuf.parse(source, options).root;

  private static validateMessage = (
    root: protobuf.Root,
    messageName: string,
  ) => {
    let MessageType: protobuf.Type | undefined;
    try {
      MessageType = root.lookupType(messageName);
    } catch {
      throw new RepeatedFieldStreamError(
        ERRORS.MESSAGE.NOT_EXISTS(messageName),
      );
    }

    const fieldCount = MessageType?.fieldsArray?.length || 0;
    const repeatedField = MessageType?.fieldsArray?.at(0);

    if (!repeatedField)
      throw new RepeatedFieldStreamError(
        ERRORS.MESSAGE.EMPTY_FIELD(messageName),
      );
    if (fieldCount > 1)
      throw new RepeatedFieldStreamError(
        ERRORS.MESSAGE.MORE_FIELDS(messageName, fieldCount),
      );
    if (!repeatedField?.repeated)
      throw new RepeatedFieldStreamError(
        ERRORS.MESSAGE.NOT_REPEATED(messageName, repeatedField.name),
      );
  };

  private static getRepeatedFieldDetails = (
    root: protobuf.Root,
    messageName: string,
  ) => {
    const getWriteType = (type: string) => {
      switch (type) {
        case DATA_TYPES.INT_32:
        case DATA_TYPES.INT_64:
        case DATA_TYPES.UINT_32:
        case DATA_TYPES.UINT_64:
        case DATA_TYPES.SINT_32:
        case DATA_TYPES.SINT_64:
        case DATA_TYPES.BOOL:
        case DATA_TYPES.ENUM:
          return WIRE_TYPES.VARINT;
        case DATA_TYPES.FIXED_64:
        case DATA_TYPES.SFIXED_64:
        case DATA_TYPES.DOUBLE:
          return WIRE_TYPES.I64;
        case DATA_TYPES.FIXED_32:
        case DATA_TYPES.SFIXED_32:
        case DATA_TYPES.FLOAT:
          return WIRE_TYPES.I32;
        case DATA_TYPES.STRING:
        case DATA_TYPES.BYTES:
        default:
          return WIRE_TYPES.LEN;
      }
    };

    const getResolvedType = (type: string, root: protobuf.Root) => {
      let resolvedType = type;

      if (!Object.values(DATA_TYPES).includes(type)) {
        const EmbeddedMessageType = root.lookupTypeOrEnum(type);
        if (EmbeddedMessageType instanceof protobuf.Enum) {
          resolvedType = DATA_TYPES.INT_32;
        }
      }

      return resolvedType;
    };

    const getDecoder = (type: string, root: protobuf.Root) => {
      if (!Object.values(DATA_TYPES).includes(type)) {
        const EmbeddedMessageType = root.lookupType(type);
        return (payload: Buffer) => {
          const message = EmbeddedMessageType.decode(payload);
          const entry = EmbeddedMessageType.toObject(message);
          return entry;
        };
      }

      return (payload: Buffer) => {
        const reader = protobuf.Reader.create(payload);

        switch (resolvedType) {
          case DATA_TYPES.INT_32:
            return reader.int32();
          case DATA_TYPES.INT_64:
            return reader.int64();
          case DATA_TYPES.UINT_32:
            return reader.uint32();
          case DATA_TYPES.UINT_64:
            return reader.uint64();
          case DATA_TYPES.SINT_32:
            return reader.sint32();
          case DATA_TYPES.SINT_64:
            return reader.sint64();
          case DATA_TYPES.BOOL:
            return reader.bool();
          case DATA_TYPES.FIXED_64:
            return reader.fixed64();
          case DATA_TYPES.SFIXED_64:
            return reader.sfixed64();
          case DATA_TYPES.DOUBLE:
            return reader.double();
          case DATA_TYPES.FIXED_32:
            return reader.fixed32();
          case DATA_TYPES.SFIXED_32:
            return reader.sfixed32();
          case DATA_TYPES.FLOAT:
            return reader.float();
          case DATA_TYPES.STRING:
            return payload.toString("utf8");
          case DATA_TYPES.BYTES:
          default:
            return payload;
        }
      };
    };

    const MessageType = root.lookupType(messageName);
    const repeatedField = MessageType.fieldsArray.at(0)!;
    const { name, packed } = repeatedField;
    const resolvedType = getResolvedType(repeatedField.type, root);

    const wireType = getWriteType(resolvedType);
    const isNumeric = [
      WIRE_TYPES.VARINT,
      WIRE_TYPES.I64,
      WIRE_TYPES.I32,
    ].includes(wireType);
    const isPacked = isNumeric && packed !== false;
    const decode = getDecoder(resolvedType, root);

    return {
      name,
      wireType,
      isPacked,
      decode,
    };
  };

  private static getPayloadSizeInBytes = (wireType: number) => {
    switch (wireType) {
      case WIRE_TYPES.I32:
        return 4;
      case WIRE_TYPES.I64:
        return 8;
      default:
        return 0;
    }
  };

  private static decodeVarint = (varint: Buffer) => {
    let number = 0;

    for (let bytePosition = 0; bytePosition < varint.length; bytePosition++) {
      const currentByte = varint[bytePosition];
      const sevenBits = currentByte & MASKS.MSB;
      const shift = bytePosition * 7;
      number = number | (sevenBits << shift);
    }

    return number;
  };

  private decodePayload(payload: Buffer) {
    try {
      return this.repeatedField.decode(payload) as T;
    } catch (error: unknown) {
      throw new RepeatedFieldStreamError(ERRORS.PAYLOAD(error));
    }
  }

  private cleanup() {
    this.repeatedField = {
      name: "",
      wireType: 0,
      isPacked: false,
      decode: () => ({}),
    };
    this.buffer = Buffer.alloc(0);
  }

  private detectVarint() {
    let bytesReamining = true;
    while (bytesReamining && this.readOffset < this.buffer.length) {
      const byte = this.buffer.at(this.readOffset)!;
      this.readOffset += 1;
      const continuationBit = byte >> 7;

      if (continuationBit === 0) {
        bytesReamining = false;
      }
    }

    return !bytesReamining;
  }

  private detectPayload() {
    while (
      this.readOffset < this.currentPayloadSize &&
      this.readOffset < this.buffer.length
    ) {
      this.readOffset += 1;
    }

    return this.readOffset === this.currentPayloadSize;
  }

  private decode() {
    if (this.nextToken === TOKENS.TAG) {
      if (this.detectVarint()) {
        const varint = this.buffer.subarray(0, this.readOffset);
        const tag = RepeatedFieldStream.decodeVarint(varint);

        if (!this.tag) {
          this.tag = tag;
        } else if (tag !== this.tag) {
          throw new RepeatedFieldStreamError(ERRORS.TAG(this.tag, tag));
        }
        this.buffer = this.buffer.subarray(this.readOffset);
        this.readOffset = 0;
        this.nextToken =
          this.repeatedField.wireType === WIRE_TYPES.LEN ||
          this.repeatedField.isPacked
            ? TOKENS.LENGTH
            : TOKENS.PAYLOAD;
      }
    }

    if (this.nextToken === TOKENS.LENGTH) {
      if (this.detectVarint()) {
        const varint = this.buffer.subarray(0, this.readOffset);

        if (!this.repeatedField.isPacked) {
          this.currentPayloadSize = RepeatedFieldStream.decodeVarint(varint);
        }
        this.buffer = this.buffer.subarray(this.readOffset);
        this.readOffset = 0;
        this.nextToken = TOKENS.PAYLOAD;
      }
    }

    if (this.nextToken === TOKENS.PAYLOAD) {
      if (
        this.repeatedField.wireType === WIRE_TYPES.VARINT
          ? this.detectVarint()
          : this.detectPayload()
      ) {
        const payload = this.buffer.subarray(0, this.readOffset);
        const entry = this.decodePayload(payload);

        this.push(entry);

        this.buffer = this.buffer.subarray(this.readOffset);
        this.readOffset = 0;
        if (!this.repeatedField.isPacked) {
          this.currentPayloadSize = 0;
          this.nextToken = TOKENS.TAG;
        } else {
          this.nextToken = TOKENS.PAYLOAD;
        }
      }
    }
  }

  _transform(
    chunk: Buffer,
    _encoding: BufferEncoding,
    callback: TransformCallback,
  ) {
    try {
      this.buffer = Buffer.concat([this.buffer, chunk]);

      while (this.readOffset < this.buffer.length) {
        this.decode();
      }

      callback();
    } catch (error: unknown) {
      callback(error as Error);
    }
  }

  _final(callback: TransformCallback) {
    this.cleanup();
    callback();
  }

  _flush(callback: TransformCallback) {
    try {
      while (this.readOffset < this.buffer.length) {
        this.decode();
      }
      callback();
    } catch (error: unknown) {
      callback(error as Error);
    }
  }
}

export default RepeatedFieldStream;
