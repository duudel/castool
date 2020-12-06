
export type Result<Success, Failed> = makeAdt<{
  success: Success,
  failed: Failed,
}>;


export type Value = string | number | boolean | Date | null;
export type DataType = "null" | "string" | "number" | "boolean" | "date";
export type DataTypeFrom<T extends Value> =
   T extends null ? "null"
  : T extends string ? "string"
  : T extends number ? "number"
  : T extends boolean ? "boolean"
  : T extends Date ? "date"
  : never;

export type InputRow = { [key: string]: Value };
export type TableDef = {
  columns: [string, DataType][];
};

export interface LexError {
  error: "Lexical error";
  message: string;
  sourcePos: [number, number];
}

export interface ParseError {
  error: "Syntax error";
  message: string;
  sourcePos: [number, number];
}

export interface SemanticError {
  error: "Semantic error";
  message: string;
  sourcePos: [number, number];
}

export type RqlError = LexError | ParseError | SemanticError;

export function lexError(message: string, input: string, pos: number): LexError { return { error: "Lexical error", message, sourcePos: getInputPosition(input, pos) }; }
export function parseError(message: string, input: string, pos: number): ParseError { return { error: "Syntax error", message, sourcePos: getInputPosition(input, pos) }; }
export function semError(message: string, input: string, pos: number): SemanticError { return { error: "Semantic error", message, sourcePos: getInputPosition(input, pos) }; }

export function getInputPosition(input: string, pos: number): [number, number] {
  let line = 0;
  let col = 0;
  while (pos > 0) {
    if (input.charAt(pos) === '\n') {
      line++;
    } else if (line === 0) {
      col++;
    }
    pos--;
  }
  return [line, col];
}

export type makeAdt<T extends Record<string, {}>> = {
  [K in keyof T]: K extends "_" ? never : { _type: K } & T[K];
}[keyof T];

