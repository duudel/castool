import { TokenKind, Token, Lexer, UnaryOperator, BinaryOperator, Ast, Parser } from './types';

function makeToken(kind: TokenKind, value: string, pos?: number) {
  return { kind, value, pos: pos ? pos : -1 };
}

function isWhitespace(c: string): boolean { return (c === ' ') || (c === '\t') || (c === '\n') || (c === '\r'); }
function isAlphabetical(c: string): boolean { return (('A' <= c) && (c <= 'Z')) || (('a' <= c) && (c <= 'z')); }
function isDigit(c: string): boolean { return ('0' <= c) && (c <= '9'); }
function isAlphaNumeric(c: string): boolean { return isAlphabetical(c) || isDigit(c); }
function isIdent(c: string): boolean { return isAlphaNumeric(c) || (c === '_'); }


function inputLeft(lexer: Lexer): boolean { return (lexer.pos < lexer.input.length); }
function hasError(lexer: Lexer): boolean { return (lexer.error !== null); }
function currentChar(lexer: Lexer): string { return lexer.input[lexer.pos]; }

function lexicalError(lexer: Lexer, error: string) {
  lexer.error = error;
}

function addToken(lexer: Lexer, token: Token) {
  lexer.tokens.push({ ...token, pos: token.pos === -1 ? lexer.pos : token.pos } );
}

function addTokenSubstring(lexer: Lexer, startPos: number, kind: TokenKind) {
  const token = makeToken(kind, lexer.input.substring(startPos, lexer.pos), startPos);
  addToken(lexer, token);
}

function advance(lexer: Lexer) { lexer.pos++; }
function advanceWithToken(lexer: Lexer, token: Token) {
  advance(lexer);
  addToken(lexer, token);
}
function accept(lexer: Lexer, c: string, token?: Token): boolean {
  if (inputLeft(lexer) && currentChar(lexer) === c) {
    if (token !== undefined) {
      advanceWithToken(lexer, token);
    } else {
      advance(lexer);
    }
    return true;
  }
  return false;
}

function acceptString(lexer: Lexer, s: string, token?: Token): boolean {
  const start = lexer.pos;
  let i = 0;
  while (inputLeft(lexer) && currentChar(lexer) === s.charAt(i)) {
    advance(lexer);
    i++;
  }
  if (i === s.length && (!inputLeft(lexer) || !isIdent(currentChar(lexer)))) {
    if (token) addToken(lexer, token);
    return true;
  } else {
    lexer.pos = start;
    return false;
  }
}

function skipWhitespace(lexer: Lexer) {
  while (inputLeft(lexer)) {
    if (isWhitespace(currentChar(lexer))) {
      advance(lexer);
      continue;
    }
    break;
  }
}

function lex(lexer: Lexer) {
  switch (currentChar(lexer)) {
    case ' ': case '\t': case '\n': case '\r':
      skipWhitespace(lexer);
      break;
    case '|':
      advanceWithToken(lexer, makeToken(TokenKind.Bar, "|"));
      break;
    case '(':
      advanceWithToken(lexer, makeToken(TokenKind.LParen, "("));
      break;
    case ')':
      advanceWithToken(lexer, makeToken(TokenKind.RParen, ")"));
      break;
    case ',':
      advanceWithToken(lexer, makeToken(TokenKind.Comma, ","));
      break;

    case '!':
      advance(lexer);
      if (inputLeft(lexer) && currentChar(lexer) === '=') {
        advanceWithToken(lexer, makeToken(TokenKind.OpNotEqual, "!="));
      } else if (acceptString(lexer, "contains", makeToken(TokenKind.OpNotContains, "!contains"))) {
        break;
      } else {
        addToken(lexer, makeToken(TokenKind.OpNot, "!"));
      }
      break;
    case '+':
      advanceWithToken(lexer, makeToken(TokenKind.OpPlus, "+"));
      break;
    case '-':
      advanceWithToken(lexer, makeToken(TokenKind.OpPlus, "-"));
      break;
    case '*':
      advanceWithToken(lexer, makeToken(TokenKind.OpPlus, "*"));
      break;
    case '/':
      advanceWithToken(lexer, makeToken(TokenKind.OpPlus, "/"));
      break;
    case '=': {
      advance(lexer);
      if (inputLeft(lexer) && currentChar(lexer) === '=') {
        advanceWithToken(lexer, makeToken(TokenKind.OpEqual, "=="));
      } else {
        addToken(lexer, makeToken(TokenKind.OpAssign, "="));
      }
      break;
    }

  //StringLit = "StringLit",
    case '"': {
      advance(lexer);
      const start = lexer.pos;
      while (inputLeft(lexer)) {
        if (currentChar(lexer) === '\\') {
          advance(lexer);
          if (inputLeft(lexer)) {
            advance(lexer);
          }
          continue;
        } else if (currentChar(lexer) === '"') {
          break;
        }
        advance(lexer);
      }

      addTokenSubstring(lexer, start, TokenKind.StringLit);
      advance(lexer); // skip the ending "
      break;
    }
    case "'":
      advance(lexer);
      const start = lexer.pos;
      while (inputLeft(lexer)) {
        if (currentChar(lexer) === '\\') {
          advance(lexer);
          if (inputLeft(lexer)) {
            advance(lexer);
          }
          continue;
        } else if (currentChar(lexer) === "'") {
          break;
        }
        advance(lexer);
      }

      addTokenSubstring(lexer, start, TokenKind.StringLit);
      advance(lexer); // skip the ending '
      break;

  //NumberLit = "NumberLit",
  //DateLit = "DateLit",
    case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9': 
      advance(lexer);
      break;

  //Ident = "Ident",
    default: {
      if (acceptString(lexer, "contains", makeToken(TokenKind.OpContains, "contains"))) {
        break;
      }
      if (!isAlphabetical(currentChar(lexer))) {
        lexicalError(lexer, "Invalid character '" + currentChar(lexer) + "'");
        advance(lexer);
        break;
      }
      const start = lexer.pos;
      while (inputLeft(lexer) && isIdent(currentChar(lexer))) {
        advance(lexer);
      }
      addTokenSubstring(lexer, start, TokenKind.Ident);
    }
  }
}

function tokenize(input: string): Token[] | string {
  const lexer: Lexer = { input, pos: 0, tokens: [], error: null };
  while (inputLeft(lexer) && !hasError(lexer)) {
    lex(lexer);
  }

  return (lexer.error === null) ? lexer.tokens : lexer.error;
}

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
function parseOperand(parser: Parser): Ast | null {
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
function parseProductExpression(parser: Parser): Ast | null {
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
function parseSumExpression(parser: Parser): Ast | null {
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
function parseLevel0Expression(parser: Parser): Ast | null {
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

function parseExpression(parser: Parser): Ast | null {
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

