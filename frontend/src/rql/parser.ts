import { UnaryOperator, BinaryOperator } from './types';
import { TokenKind, Token, tokenize, LexResult } from './lexer';
import { makeAdt, ParseError, parseError, Result } from './common';

export type Aggregation = {
  name: Token;
  expr: AstExpr & { _type: "functionCall" };
};

export type Ast = makeAdt<{
  table: { name: Token, pos: number },
  cont: { source: Ast, op: Ast, pos: number }
  where: { expr: AstExpr, pos: number },
  project: { names: Token[], pos: number },
  extend: { name: Token, expr: AstExpr, pos: number },
  summarize: { aggregations: Aggregation[], groupBy: Token[], pos: number },
  orderBy: { names: Token[], pos: number, order: "asc" | "desc" },
  
  column: { name: Token, pos: number },
  nullLit: { pos: number },
  trueLit: { pos: number },
  falseLit: { pos: number },
  stringLit: { value: Token, pos: number },
  numberLit: { value: Token, pos: number },
  dateLit: { value: Token, pos: number },
  unaryOp: { op: UnaryOperator, expr: AstExpr, pos: number },
  binaryOp: { op: BinaryOperator, exprA: AstExpr, exprB: AstExpr, pos: number },
  functionCall: { functionName: Token, args: AstExpr[], pos: number },
}>;

export type AstTable = Ast & { _type: "table" };
export type AstCont = Ast & { _type: "cont" };
export type AstWhere = Ast & { _type: "where" };
export type AstProject = Ast & { _type: "project" };
export type AstExtend = Ast & { _type: "extend" };
export type AstSummarize = Ast & { _type: "summarize" };
export type AstOrderBy = Ast & { _type: "orderBy" };
export type AstExpr = Ast & { _type: "column" | "nullLit" | "trueLit" | "falseLit" | "stringLit" | "numberLit" | "dateLit" | "unaryOp" | "binaryOp" | "functionCall" };

export interface Parser {
  input: string;
  pos: number;
  tokens: Token[];

  error: ParseError | null;
}

export type ParseResult = Result<
  { ast: Ast },
  { error: ParseError }
>;

function setParserError(parser: Parser, error: string) {
  console.log("Error", parser);
  if (hasTokens(parser)) {
    const token = currentToken(parser);
    parser.error = parseError(error, parser.input, token.pos);
  } else {
    parser.error = parseError(error, parser.input, -1);
  }
}
function hasParserError(parser: Parser): boolean { return parser.error !== null; }

function hasTokens(parser: Parser): boolean { return parser.pos < parser.tokens.length; }
function currentToken(parser: Parser): Token { return parser.tokens[parser.pos]; } // TODO: range check
function prevToken(parser: Parser): Token { return parser.tokens[parser.pos-1]; } // TODO: range check

function advanceParser(parser: Parser) { parser.pos++; }

function acceptToken(parser: Parser, kind: TokenKind): Token | null {
  if (hasTokens(parser) && currentToken(parser).kind === kind) {
    const token = currentToken(parser);
    advanceParser(parser);
    return token;
  }
  return null;
}

function acceptAnyToken<K extends TokenKind, T extends Token & { kind: K }>(parser: Parser, kinds: K[]): T | null {
  if (hasTokens(parser) && (kinds as TokenKind[]).includes(currentToken(parser).kind)) {
    const token = currentToken(parser);
    advanceParser(parser);
    return token as T;
  }
  return null;
}

function expectToken(parser: Parser, kind: TokenKind): Token | null {
  const token = acceptToken(parser, kind);
  if (!token) {
    setParserError(parser, "Expected " + kind);
    return null;
  }
  return token;
}

function acceptIdent(parser: Parser, value: string): Token | null {
  if (hasTokens(parser)) {
    const token = currentToken(parser);
    if (token.kind === TokenKind.Ident && token.value === value) {
      advanceParser(parser);
      return token;
    }
  }
  return null;
}

function expectIdent(parser: Parser, value: string): Token | null {
  const token = acceptIdent(parser, value);
  if (!token) {
    setParserError(parser, "Expected " + value)
    return null;
  }
  return token;
}

function parseArguments(parser: Parser): AstExpr[] | null {
  const result: AstExpr[] = [];
  while (true) {
    const expr = parseExpression(parser);
    if (expr === null) {
      if (!hasParserError(parser)) {
        if (result.length === 0) {
          return [];
        } else {
          setParserError(parser, "Expected function argument");
          return null;
        }
      } else {
        return null;
      }
    }

    result.push(expr);
    if (!acceptToken(parser, TokenKind.Comma)) break;
  }
  return result;
}

// Unary ops, table column, literal, parentheses, function call
function parseOperand(parser: Parser): AstExpr | null {
  if (acceptToken(parser, TokenKind.OpNot)) {
    const expr = parseOperand(parser);
    if (!expr) return null;
    return { _type: "unaryOp", op: UnaryOperator.Not, expr, pos: expr.pos };
  } else if (acceptToken(parser, TokenKind.OpPlus)) {
    const expr = parseOperand(parser);
    if (!expr) return null;
    return { _type: "unaryOp", op: UnaryOperator.Plus, expr, pos: expr.pos };
  } else if (acceptToken(parser, TokenKind.OpMinus)) {
    const expr = parseOperand(parser);
    if (!expr) return null;
    return { _type: "unaryOp", op: UnaryOperator.Minus, expr, pos: expr.pos };
  } else if (acceptToken(parser, TokenKind.LParen)) {
    const expr = parseExpression(parser);
    if (!expr) return null;
    if (!expectToken(parser, TokenKind.RParen)) return null;
    return expr;
  } else {
    const nullLit = acceptToken(parser, TokenKind.NullLit);
    if (nullLit) {
      return { _type: "nullLit", pos: nullLit.pos };
    }
    const trueLit = acceptToken(parser, TokenKind.TrueLit);
    if (trueLit) {
      return { _type: "trueLit", pos: trueLit.pos };
    }
    const falseLit = acceptToken(parser, TokenKind.FalseLit);
    if (falseLit) {
      return { _type: "falseLit", pos: falseLit.pos };
    }
    const stringLit = acceptToken(parser, TokenKind.StringLit);
    if (stringLit) {
      return { _type: "stringLit", value: stringLit, pos: stringLit.pos };
    }
    const numberLit = acceptToken(parser, TokenKind.NumberLit);
    if (numberLit) {
      return { _type: "numberLit", value: numberLit, pos: numberLit.pos };
    }
    const dateLit = acceptToken(parser, TokenKind.DateLit);
    if (dateLit) {
      return { _type: "dateLit", value: dateLit, pos: dateLit.pos };
    }
    const ident = acceptToken(parser, TokenKind.Ident);
    if (ident && acceptToken(parser, TokenKind.LParen)) {
      const args = parseArguments(parser);
      if (args === null) return null;
      if (!expectToken(parser, TokenKind.RParen)) return null;
      return { _type: "functionCall", functionName: ident, args, pos: ident.pos };
    } else if (ident) {
      return { _type: "column", name: ident, pos: ident.pos };
    }
    return null;
  }
}

function binaryOpFromToken<T extends {
  kind:
    TokenKind.OpPlus | TokenKind.OpMinus | TokenKind.OpTimes | TokenKind.OpDivide
    | TokenKind.OpEqual | TokenKind.OpNotEqual
    | TokenKind.OpLess | TokenKind.OpLessEq
    | TokenKind.OpGreater | TokenKind.OpGreaterEq
    | TokenKind.OpContains | TokenKind.OpNotContains
    | TokenKind.OpAnd | TokenKind.OpOr
}>(token: T): BinaryOperator {
  switch (token.kind) {
    case TokenKind.OpPlus: return BinaryOperator.Plus;
    case TokenKind.OpMinus: return BinaryOperator.Minus;
    case TokenKind.OpTimes: return BinaryOperator.Multiply;
    case TokenKind.OpDivide: return BinaryOperator.Divide;
    case TokenKind.OpEqual: return BinaryOperator.Equal;
    case TokenKind.OpNotEqual: return BinaryOperator.NotEqual;
    case TokenKind.OpLess: return BinaryOperator.Less;
    case TokenKind.OpLessEq: return BinaryOperator.LessEq;
    case TokenKind.OpGreater: return BinaryOperator.Greater;
    case TokenKind.OpGreaterEq: return BinaryOperator.GreaterEq;
    case TokenKind.OpContains: return BinaryOperator.Contains;
    case TokenKind.OpNotContains: return BinaryOperator.NotContains;
    case TokenKind.OpAnd: return BinaryOperator.And;
    case TokenKind.OpOr: return BinaryOperator.Or;
  }
}

// Multiply, Divide
function parseProductExpression(parser: Parser): AstExpr | null {
  let left = parseOperand(parser);
  if (!left) return null;
  do {
    let opToken = acceptAnyToken(parser, [TokenKind.OpTimes, TokenKind.OpDivide]);
    if (!opToken) break;
    const op = binaryOpFromToken(opToken);
    const right = parseProductExpression(parser);
    if (!right) return null;
    left = { _type: "binaryOp", op, exprA: left, exprB: right, pos: opToken.pos };
  } while (true);
  return left;
}

// Plus, Minus
function parseSumExpression(parser: Parser): AstExpr | null {
  let left = parseProductExpression(parser)
  if (!left) return null;
  do {
    let opToken = acceptAnyToken(parser, [TokenKind.OpPlus, TokenKind.OpMinus]);
    if (!opToken) break;
    const op = binaryOpFromToken(opToken);
    const right = parseSumExpression(parser);
    if (!right) return null;
    left = { _type: "binaryOp", op, exprA: left, exprB: right, pos: opToken.pos };
  } while (true);
  return left;
}

// Contains, NotContains
function parseContainsExpression(parser: Parser): AstExpr | null {
  let left = parseSumExpression(parser);
  if (!left) return null;
  do {
    let opToken = acceptAnyToken(parser, [TokenKind.OpContains, TokenKind.OpNotContains]);
    if (!opToken) break;
    const op = binaryOpFromToken(opToken);
    const right = parseContainsExpression(parser);
    if (!right) return null;
    left = { _type: "binaryOp", op, exprA: left, exprB: right, pos: opToken.pos };
  } while (true);
  return left;
}

// Comparisons: == != < > <= >=
function parseComparisonExpression(parser: Parser): AstExpr | null {
  let left = parseContainsExpression(parser);
  if (!left) return null;
  do {
    let opToken = acceptAnyToken(parser, [
      TokenKind.OpEqual, TokenKind.OpNotEqual,
      TokenKind.OpLess, TokenKind.OpLessEq,
      TokenKind.OpGreater, TokenKind.OpGreaterEq
    ]);
    if (!opToken) break;
    const op = binaryOpFromToken(opToken);
    const right = parseComparisonExpression(parser);
    if (!right) return null;
    left = { _type: "binaryOp", op, exprA: left, exprB: right, pos: opToken.pos };
  } while (true);
  return left;
}

// Logical and, or
function parseLogicalExpression(parser: Parser): AstExpr | null {
  let left = parseComparisonExpression(parser);
  if (!left) return null;
  do {
    let opToken = acceptAnyToken(parser, [TokenKind.OpAnd, TokenKind.OpOr]);
    if (!opToken) break;
    const op = binaryOpFromToken(opToken);
    const right = parseLogicalExpression(parser);
    if (!right) return null;
    left = { _type: "binaryOp", op, exprA: left, exprB: right, pos: opToken.pos };
  } while (true);
  return left;
}

function parseExpression(parser: Parser): AstExpr | null {
  return parseLogicalExpression(parser)
}

function parseWhere(parser: Parser, pos: number): AstWhere | null {
  const expr = parseExpression(parser);
  if (!expr) return null;
  return { _type: "where", expr, pos: expr.pos };
}

function parseExtend(parser: Parser, pos: number): AstExtend | null {
  const name = expectToken(parser, TokenKind.Ident);
  if (!name) return null;
  if (!expectToken(parser, TokenKind.OpAssign)) return null;
  const expr = parseExpression(parser);
  if (!expr) return null;
  return { _type: "extend", name, expr, pos: pos };
}

function parseProject(parser: Parser, pos: number): AstProject | null {
  function parseNames(result: Token[]): Token[] {
    const ident = expectToken(parser, TokenKind.Ident);
    if (ident) {
      result.push(ident);
      if (acceptToken(parser, TokenKind.Comma)) {
        return parseNames(result);
      } else {
        return result;
      }
    } else {
      return result; // There was an error
    }
  }
  const names = parseNames([]);
  return { _type: "project", names, pos };
}

// | summarize <aggregation>, ... [by <column>, ...]
function parseSummarize(parser: Parser, pos: number): AstSummarize | null {
  const aggregations: Aggregation[] = [];
  do {
    const aggrName = expectToken(parser, TokenKind.Ident);
    if (!aggrName) return null;
    if (!expectToken(parser, TokenKind.OpAssign)) return null;
    const expr = parseExpression(parser);
    if (!expr) return null;
    if (expr._type !== "functionCall") return null;
    aggregations.push({ name: aggrName, expr });
  } while (acceptToken(parser, TokenKind.Comma))

  const groupBy: Token[] = [];
  if (acceptIdent(parser, "by")) {
    do {
      const columnName = expectToken(parser, TokenKind.Ident);
      if (!columnName) return null;
      groupBy.push(columnName);
    } while (acceptToken(parser, TokenKind.Comma))
  }
  return { _type: "summarize", aggregations, groupBy, pos };
}

function parserOrderBy(parser: Parser, pos: number): AstOrderBy | null {
  if (!expectIdent(parser, "by")) return null;

  const names: Token[] = [];
  do {
    const columnName = expectToken(parser, TokenKind.Ident);
    if (!columnName) return null;
    names.push(columnName);
  } while (acceptToken(parser, TokenKind.Comma))

  const orderToken = acceptIdent(parser, "asc") || acceptIdent(parser, "desc");
  if (!orderToken) {
    setParserError(parser, "Expected 'asc' or 'desc'");
    return null;
  }
  const order = orderToken.value === "asc" ? "asc" : "desc";

  return { _type: "orderBy", names, order, pos };
}

function parseSourceOp(parser: Parser): Ast | null {
  const ident = expectToken(parser, TokenKind.Ident);
  if (ident) {
    switch (ident.value) {
      case "where":
        return parseWhere(parser, ident.pos);
      case "extend":
        return parseExtend(parser, ident.pos);
      case "project":
        return parseProject(parser, ident.pos);
      case "summarize":
        return parseSummarize(parser, ident.pos);
      case "order":
        return parserOrderBy(parser, ident.pos);
    }
  }

  setParserError(parser, "Expected source operator 'where', 'extend', 'project' or 'summarize' 'order by'");
  return null;
}

function parseTopLevel(parser: Parser): Ast | null {
  const ident = acceptToken(parser, TokenKind.Ident);
  if (!ident) return null;
    
  let topLevel: Ast = { _type: "table", name: ident, pos: ident.pos };

  while (acceptToken(parser, TokenKind.Bar)) {
    const barPos = prevToken(parser).pos;
    const op = parseSourceOp(parser);
    if (!op) return null;
    topLevel = { _type: "cont", source: topLevel, op, pos: barPos };
  }
  return topLevel;
}

// TODO: parse multiple top levels into a root ast node
export function parseTokens(input: string, tokens: Token[]): ParseResult {
  const parser: Parser = { input, pos: 0, tokens, error: null };
  const result = parseTopLevel(parser);
  if (result === null && hasTokens(parser) && !hasParserError(parser)) {
    setParserError(parser, "Expected top level");
  }
  if (parser.error !== null) {
    return { _type: "failed", error: parser.error };
  } else if (result === null) {
    return { _type: "failed", error: parseError("No result", input, -1) };
  } else {
    return { _type: "success", ast: result };
  }
}

export function parse(input: string): ParseResult | LexResult {
  const lexResult = tokenize(input);
  if (lexResult._type === "failed") {
    return lexResult;
  } else {
    return parseTokens(input, lexResult.tokens);
  }
}

