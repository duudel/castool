import { UnaryOperator, BinaryOperator } from './types';
import { TokenKind, Token, tokenize, LexResult } from './lexer';
import { makeAdt, ParseError, parseError, Result } from './common';

export type Ast = makeAdt<{
  table: { name: Token, pos: number },
  cont: { source: Ast, op: Ast, pos: number }
  where: { expr: AstExpr, pos: number },
  project: { names: Token[], pos: number },
  extend: { name: Token, expr: AstExpr, pos: number },
  
  column: { name: Token, pos: number },
  nullLit: { pos: number },
  stringLit: { value: Token, pos: number },
  numberLit: { value: Token, pos: number },
  dateLit: { value: Token, pos: number },
  unaryOp: { op: UnaryOperator, expr: AstExpr, pos: number },
  binaryOp: { op: BinaryOperator, exprA: AstExpr, exprB: AstExpr, pos: number },
}>;

export type AstTable = Ast & { _type: "table" };
export type AstCont = Ast & { _type: "cont" };
export type AstWhere = Ast & { _type: "where" };
export type AstProject = Ast & { _type: "project" };
export type AstExtend = Ast & { _type: "extend" };
export type AstExpr = Ast & { _type: "column" | "nullLit" | "stringLit" | "numberLit" | "dateLit" | "unaryOp" | "binaryOp" };

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

// Unary ops, table column, literal, parentheses
function parseOperand(parser: Parser): AstExpr | null {
  if (acceptToken(parser, TokenKind.OpNot)) {
    const expr = parseExpression(parser);
    if (!expr) return null;
    return { _type: "unaryOp", op: UnaryOperator.Not, expr, pos: expr.pos };
  } else if (acceptToken(parser, TokenKind.OpPlus)) {
    const expr = parseExpression(parser);
    if (!expr) return null;
    return { _type: "unaryOp", op: UnaryOperator.Plus, expr, pos: expr.pos };
  } else if (acceptToken(parser, TokenKind.OpMinus)) {
    const expr = parseExpression(parser);
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
    const ident = expectToken(parser, TokenKind.Ident);
    if (ident) {
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
function parseLevel1Expression(parser: Parser): AstExpr | null {
  let left = parseSumExpression(parser);
  if (!left) return null;
  do {
    let opToken = acceptAnyToken(parser, [TokenKind.OpContains, TokenKind.OpNotContains]);
    if (!opToken) break;
    const op = binaryOpFromToken(opToken);
    const right = parseLevel1Expression(parser);
    if (!right) return null;
    left = { _type: "binaryOp", op, exprA: left, exprB: right, pos: opToken.pos };
  } while (true);
  return left;
}

// Comparisons: == != < > <= >=
function parseLevel0Expression(parser: Parser): AstExpr | null {
  let left = parseLevel1Expression(parser);
  if (!left) return null;
  do {
    let opToken = acceptAnyToken(parser, [
      TokenKind.OpEqual, TokenKind.OpNotEqual,
      TokenKind.OpLess, TokenKind.OpLessEq,
      TokenKind.OpGreater, TokenKind.OpGreaterEq
    ]);
    if (!opToken) break;
    const op = binaryOpFromToken(opToken);
    const right = parseLevel0Expression(parser);
    if (!right) return null;
    left = { _type: "binaryOp", op, exprA: left, exprB: right, pos: opToken.pos };
  } while (true);
  return left;
}

//// Comparisons: and or
//function parseLogcalExpression(parser: Parser): AstExpr | null {
//  let left = parseLevel1Expression(parser);
//  if (!left) return null;
//  do {
//    let opToken = acceptAnyToken(parser, [TokenKind.OpEqual, TokenKind.OpNotEqual]);
//    if (!opToken) break;
//    const op = binaryOpFromToken(opToken);
//    const right = parseLogcalExpression(parser);
//    if (!right) return null;
//    left = { _type: "binaryOp", op, exprA: left, exprB: right, pos: opToken.pos };
//  } while (true);
//  return left;
//}

function parseExpression(parser: Parser): AstExpr | null {
  return parseLevel0Expression(parser)
}

function parseWhere(parser: Parser, pos: number): Ast | null {
  const expr = parseExpression(parser);
  if (!expr) return null;
  return { _type: "where", expr, pos: expr.pos };
}

function parseExtend(parser: Parser, pos: number): Ast | null {
  const name = expectToken(parser, TokenKind.Ident);
  if (!name) return null;
  if (!expectToken(parser, TokenKind.OpAssign)) return null;
  const expr = parseExpression(parser);
  if (!expr) return null;
  return { _type: "extend", name, expr, pos: pos };
}

function parseProject(parser: Parser, pos: number): Ast | null {
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

function parseSourceOp(parser: Parser): Ast | null {
  const ident = expectToken(parser, TokenKind.Ident);
  if (!ident || (ident.value !== "where" && ident.value !== "extend" && ident.value !== "project")) {
    setParserError(parser, "Expected source operator 'where', 'extend', or 'project'");
    return null;
  }

  switch (ident.value) {
    case "where":
      return parseWhere(parser, ident.pos);
    case "extend":
      return parseExtend(parser, ident.pos);
    case "project":
      return parseProject(parser, ident.pos);
  }
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

