const DATA_TYPES = {
  INT_32: "int32",
  INT_64: "int64",
  UINT_32: "uint32",
  UINT_64: "uint64",
  SINT_32: "sint32",
  SINT_64: "sint64",
  BOOL: "bool",
  ENUM: "enum",
  FIXED_64: "fixed64",
  SFIXED_64: "sfixed64",
  DOUBLE: "double",
  STRING: "string",
  BYTES: "bytes",
  FIXED_32: "fixed32",
  SFIXED_32: "sfixed32",
  FLOAT: "float",
};

const WIRE_TYPES = {
  VARINT: 0,
  I64: 1,
  LEN: 2,
  I32: 5,
};

const TOKENS = {
  TAG: 0,
  LENGTH: 1,
  PAYLOAD: 3,
};

const MASKS = {
  MSB: 0b01111111,
  WIRE_TYPE: 0b00000111,
};

const ERRORS = {
  MESSAGE: {
    NOT_EXISTS: (messageName: string) =>
      `Invalid message: '${messageName}' does not exist in the proto file.`,
    EMPTY_FIELD: (messageName: string) =>
      `Invalid message: '${messageName}' does not have any fields.`,
    MORE_FIELDS: (messageName: string, fieldCount: number) =>
      `Invalid message: '${messageName}' should only have one field, but has ${fieldCount} fields.`,
    NOT_REPEATED: (messageName: string, fieldName: string) =>
      `Invalid message: The field '${fieldName}' in '${messageName}' must be 'repeated'.`,
  },
  TAG: (expected: number, actual: number) =>
    `Expected array-like structure: Tag should repeat as ${expected}, but encountered a different tag: ${actual}.`,
  PAYLOAD: (error: unknown) =>
    `Failed to decode protobuf payload: ${error instanceof Error ? error.message : String(error)}`,
};

export { DATA_TYPES, WIRE_TYPES, TOKENS, MASKS, ERRORS };
