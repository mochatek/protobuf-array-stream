class RepeatedFieldStreamError extends Error {
  constructor(
    message: string,
    public readonly cause?: unknown,
  ) {
    super(message);
    this.name = "RepeatedFieldStreamError";
  }
}

export default RepeatedFieldStreamError;
