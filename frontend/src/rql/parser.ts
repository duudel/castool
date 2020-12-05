import { TokenKind, Token, UnaryOperator, BinaryOperator, Ast, AstExpr, Parser } from './types';
import { tokenize } from './lexer';

function getInputPosition(input: string, pos: number): [number, number] {
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

function setParserError(parser: Parser, error: string) {
  if (hasTokens(parser)) {
    const token = currentToken(parser);
    const [line, col] = getInputPosition(parser.input, token.pos);
    parser.error = "At " + line + ":" + col + ": " + error;
  } else {
    parser.error = error;
  }
}
function hasParserError(parser: Parser): boolean { return parser.error !== null; }

function hasTokens(parser: Parser): boolean { return parser.pos < parser.tokens.length; }
function currentToken(parser: Parser): Token { return parser.tokens[parser.pos]; } // TODO: range check
function prevToken(parser: Parser): Token { return parser.tokens[parser.pos - 1]; } // TODO: range check

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

//  column: { name: Token },
//  literal: { name: Token },
//  unaryOp: { op: UnaryOperator, expr: Ast },
//  binaryOp: { op: BinaryOperator, exprA: Ast, exprB: Ast },
//  // operators
//  OpNot = "!",
//  OpPlus = "+",
//  OpMinus = "-",
//  OpTimes = "*",
//  OpDivide = "/",
//  OpAssign = "=",
//  OpEqual = "==",
//  OpNotEqual = "!=",
//
//export enum UnaryOperator {
//  Not, Plus, Minus
//}
//
//export enum BinaryOperator {
//  Plus, Minus, Multiply, Divide, Contains, NotContains
//}

// Unary ops, table column, literal, parentheses
function parseOperand(parser: Parser): AstExpr | null {
  if (acceptToken(parser, TokenKind.OpNot)) {
    const expr = parseExpression(parser);
    if (!expr) return null;
    return { _type: "unaryOp", op: UnaryOperator.Not, expr };
  } else if (acceptToken(parser, TokenKind.OpPlus)) {
    const expr = parseExpression(parser);
    if (!expr) return null;
    return { _type: "unaryOp", op: UnaryOperator.Plus, expr };
  } else if (acceptToken(parser, TokenKind.OpMinus)) {
    const expr = parseExpression(parser);
    if (!expr) return null;
    return { _type: "unaryOp", op: UnaryOperator.Minus, expr };
  } else if (acceptToken(parser, TokenKind.LParen)) {
    const expr = parseExpression(parser);
    if (!expr) return null;
    if (!expectToken(parser, TokenKind.RParen)) return null;
    return expr;
  } else {
    const stringLit = acceptToken(parser, TokenKind.StringLit);
    if (stringLit) {
      return { _type: "stringLit", value: stringLit };
    }
    const numberLit = acceptToken(parser, TokenKind.NumberLit);
    if (numberLit) {
      return { _type: "numberLit", value: numberLit };
    }
    const dateLit = acceptToken(parser, TokenKind.DateLit);
    if (dateLit) {
      return { _type: "dateLit", value: dateLit };
    }
    const ident = expectToken(parser, TokenKind.Ident);
    if (ident) {
      return { _type: "column", name: ident };
    }
    return null;
  }
}

function binaryOpFromToken<T extends {
  kind: TokenKind.OpPlus | TokenKind.OpMinus | TokenKind.OpTimes | TokenKind.OpDivide | TokenKind.OpContains | TokenKind.OpNotContains
}>(token: T): BinaryOperator {
  switch (token.kind) {
    case TokenKind.OpPlus: return BinaryOperator.Plus;
    case TokenKind.OpMinus: return BinaryOperator.Minus;
    case TokenKind.OpTimes: return BinaryOperator.Multiply;
    case TokenKind.OpDivide: return BinaryOperator.Divide;
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
    left = { _type: "binaryOp", op, exprA: left, exprB: right };
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
    left = { _type: "binaryOp", op, exprA: left, exprB: right };
  } while (true);
  return left;
}

// Contains, NotContains
function parseLevel0Expression(parser: Parser): AstExpr | null {
  let left = parseSumExpression(parser);
  if (!left) return null;
  do {
    let opToken = acceptAnyToken(parser, [TokenKind.OpContains, TokenKind.OpNotContains]);
    if (!opToken) break;
    const op = binaryOpFromToken(opToken);
    const right = parseProductExpression(parser);
    if (!right) return null;
    left = { _type: "binaryOp", op, exprA: left, exprB: right };
  } while (true);
  return left;
}

function parseExpression(parser: Parser): AstExpr | null {
  return parseLevel0Expression(parser)
}

function parseWhere(parser: Parser): Ast | null {
  const expr = parseExpression(parser);
  if (!expr) return null;
  return { _type: "where", expr };
}

function parseExtend(parser: Parser): Ast | null {
  const name = expectToken(parser, TokenKind.Ident);
  if (!name) return null;
  if (!expectToken(parser, TokenKind.OpAssign)) return null;
  const expr = parseExpression(parser);
  if (!expr) return null;
  return { _type: "extend", name, expr };
}

function parseProject(parser: Parser): Ast | null {
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
  return { _type: "project", names };
}

function parseSourceOp(parser: Parser): Ast | null {
  const ident = expectToken(parser, TokenKind.Ident);
  if (!ident || (ident.value !== "where" && ident.value !== "extend" && ident.value !== "project")) {
    setParserError(parser, "Expected source operator 'where', 'extend', or 'project'");
    return null;
  }

  switch (ident.value) {
    case "where":
      return parseWhere(parser);
    case "extend":
      return parseExtend(parser);
    case "project":
      return parseProject(parser);
  }
}

function parseTopLevel(parser: Parser): Ast | null {
  const ident = acceptToken(parser, TokenKind.Ident);
  if (!ident) return null;
    
  let topLevel: Ast = { _type: "table", name: ident };

  while (acceptToken(parser, TokenKind.Bar)) {
    const op = parseSourceOp(parser);
    if (!op) return null;
    topLevel = { _type: "cont", source: topLevel, op };
  }
  return topLevel;
}

function parseTokens(input: string, tokens: Token[]): Ast | string {
  const parser: Parser = { input, pos: 0, tokens, error: null };
  const result = parseTopLevel(parser);
  if (result === null && hasTokens(parser) && !hasParserError(parser)) {
    return "Expected top level";
  }
  return (parser.error !== null) ? parser.error : (result === null ? "No result" : result);
}

export function parse(input: string): Ast | string {
  const lexResult = tokenize(input);
  if (typeof lexResult === "string") {
    return lexResult;
  } else {
    console.log(lexResult);
    return parseTokens(input, lexResult);
  }
}

