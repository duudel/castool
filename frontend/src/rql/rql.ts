
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
}

export interface Token {
  kind: TokenKind;
  value: string;
}

function makeToken(kind: TokenKind, value: string) {
  return { kind, value };
}

export enum UnaryOperator {
  Not, Plus, Minus
}

export enum BinaryOperator {
  Not, Plus, Minus, Multiply, Divide, Contains, NotContains
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
  literal: { name: Token },
  unaryOp: { op: UnaryOperator, expr: Ast },
  binaryOp: { op: BinaryOperator, exprA: Ast, exprB: Ast },
}>;

interface Lexer {
  input: string;
  pos: number;

  tokens: Token[];
  error: string | null;
};

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
  lexer.tokens.push(token);
}

function addTokenSubstring(lexer: Lexer, startPos: number, kind: TokenKind) {
  const token = makeToken(kind, lexer.input.substring(startPos, lexer.pos));
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

interface Parser {
  pos: number;
  tokens: Token[];

  error: string | null;
}

function setParserError(parser: Parser, error: string) { parser.error = error; }
function hasParserError(parser: Parser): boolean { return parser.error !== null; }

function hasTokens(parser: Parser): boolean { return parser.pos < parser.tokens.length; }
function currentToken(parser: Parser): Token { return parser.tokens[parser.pos]; }

function advanceParser(parser: Parser) { parser.pos++; }

function acceptToken(parser: Parser, kind: TokenKind): Token | null {
  if (hasTokens(parser) && currentToken(parser).kind === kind) {
    const token = currentToken(parser);
    advanceParser(parser);
    return token;
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

function parseSourceOp(parser: Parser): Ast | null {
  const ident = expectToken(parser, TokenKind.Ident);
  if (!ident || (ident.value !== "where" && ident.value !== "extend" && ident.value !== "project")) {
    setParserError(parser, "Expected source operator 'where', 'extend', or 'project'");
    return null;
  }

  switch (ident.value) {
    case "where":
    case "extend":
    case "project":
      return { _type: "project", names: [] };
  }
}

function parseTopLevel(parser: Parser): Ast | null {
  const ident = acceptToken(parser, TokenKind.Ident);
  if (!ident) return null;
    
  const table: Ast = { _type: "table", name: ident };

  if (acceptToken(parser, TokenKind.Bar)) {
    const op = parseSourceOp(parser);
    if (!op) {
      return null;
    }
    return { _type: "cont", source: table, op };
  } else {
    return table;
  }
}

function parseTokens(tokens: Token[]): Ast | string {
  const parser: Parser = { pos: 0, tokens, error: null };
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
    return parseTokens(lexResult);
  }
  //return { _type: 'table', name: { kind: TokenKind.Ident, value: "Rows" } };
}

