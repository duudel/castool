
export enum TokenKind {
  Ident = "Ident",
  StringLit = "StringLit",
  NumberLit = "NumberLit",
  DateLit = "DateLit",
  // special
  Bar = "|",
  LParen = "(",
  RParen = ")",
  Comma = ",",
  // operators
  OpNot = "!",
  OpPlus = "+",
  OpMinus = "-",
  OpTimes = "*",
  OpDivide = "/",
  OpAssign = "=",
  OpEqual = "==",
  OpNotEqual = "!=",
  OpContains = "contains",
  OpNotContains = "!contains",
}

export interface Token {
  kind: TokenKind;
  value: string;
  pos: number;
}

export enum UnaryOperator {
  Not, Plus, Minus
}

export enum BinaryOperator {
  Plus, Minus, Multiply, Divide, Contains, NotContains
}

export type makeAdt<T extends Record<string, {}>> = {
  [K in keyof T]: K extends "_" ? never : { _type: K } & T[K];
}[keyof T];

export type Ast = makeAdt<{
  table: { name: Token },
  cont: { source: Ast, op: Ast }
  where: { expr: Ast },
  project: { names: Token[] },
  extend: { name: Token, expr: Ast },
  
  column: { name: Token },
  stringLit: { value: Token },
  numberLit: { value: Token },
  dateLit: { value: Token },
  unaryOp: { op: UnaryOperator, expr: Ast },
  binaryOp: { op: BinaryOperator, exprA: Ast, exprB: Ast },
}>;

export interface Lexer {
  input: string;
  pos: number;

  tokens: Token[];
  error: string | null;
};

export interface Parser {
  input: string;
  pos: number;
  tokens: Token[];

  error: string | null;
}

