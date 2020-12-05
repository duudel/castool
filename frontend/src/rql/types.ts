
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
  where: { expr: AstExpr },
  project: { names: Token[] },
  extend: { name: Token, expr: AstExpr },
  
  column: { name: Token },
  stringLit: { value: Token },
  numberLit: { value: Token },
  dateLit: { value: Token },
  unaryOp: { op: UnaryOperator, expr: AstExpr },
  binaryOp: { op: BinaryOperator, exprA: AstExpr, exprB: AstExpr },
}>;

export type AstTable = Ast & { _type: "table" };
export type AstCont = Ast & { _type: "cont" };
export type AstWhere = Ast & { _type: "where" };
export type AstProject = Ast & { _type: "project" };
export type AstExtend = Ast & { _type: "extend" };
export type AstExpr = Ast & { _type: "column" | "stringLit" | "numberLit" | "dateLit" | "unaryOp" | "binaryOp" };

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

