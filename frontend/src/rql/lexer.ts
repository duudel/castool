import { lexError, LexError, Result } from "./common";

export enum TokenKind {
  Ident = "Ident",
  StringLit = "StringLit",
  NumberLit = "NumberLit",
  DateLit = "DateLit",
  NullLit = "Null",
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
  OpLess = "<",
  OpGreater = ">",
  OpLessEq = "<=",
  OpGreaterEq = ">=",
  OpContains = "contains",
  OpNotContains = "!contains",
}

export interface Token {
  kind: TokenKind;
  value: string;
  pos: number;
}

export interface Lexer {
  input: string;
  pos: number;

  tokens: Token[];
  error: LexError | null;
};

export type LexResult = Result<
  { tokens: Token[] },
  { error: LexError }
>;


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
function peekChar(lexer: Lexer): string | undefined { return (lexer.input.length > lexer.pos + 1) ? lexer.input[lexer.pos + 1] : undefined; }

function lexicalError(lexer: Lexer, error: string) {
  lexer.error = lexError(error, lexer.input, lexer.pos);
}

function skipMultilineComment(lexer: Lexer) {
  const startPos = lexer.pos;
  advance(lexer); // skip /
  advance(lexer); // skip *
  while (inputLeft(lexer)) {
    if (currentChar(lexer) === '*' && peekChar(lexer) === '/') {
      break;
    }
    advance(lexer);
  }
  if (!inputLeft(lexer)) {
    lexer.error = lexError("Un treminated multiline comment", lexer.input, startPos);
  } else {
    advance(lexer); // skip *
    advance(lexer); // skip /
  }
}

function skipSingleLineComment(lexer: Lexer) {
  advance(lexer); // skip /
  advance(lexer); // skip /
  while (inputLeft(lexer) && currentChar(lexer) !== '\n') {
    advance(lexer);
  }
  if (inputLeft(lexer)) advance(lexer); // skip newline
}

function skipWhitespace(lexer: Lexer): boolean {
  let skipped = false;
  while (inputLeft(lexer)) {
    if (isWhitespace(currentChar(lexer))) {
      skipped = true;
      advance(lexer);
      continue;
    }
    break;
  }
  return skipped;
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
      advanceWithToken(lexer, makeToken(TokenKind.OpTimes, "*"));
      break;
    case '/':
      if (peekChar(lexer) === '/') {
        skipSingleLineComment(lexer);
        break;
      } else if (peekChar(lexer) === '*') {
        skipMultilineComment(lexer);
        break;
      }
      advanceWithToken(lexer, makeToken(TokenKind.OpDivide, "/"));
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
    case '<': {
      advance(lexer);
      if (inputLeft(lexer) && currentChar(lexer) === '=') {
        advanceWithToken(lexer, makeToken(TokenKind.OpLessEq, "<="))
      } else {
        addToken(lexer, makeToken(TokenKind.OpLess, "<"));
      }
      break;
    }
    case '>': {
      advance(lexer);
      if (inputLeft(lexer) && currentChar(lexer) === '=') {
        advanceWithToken(lexer, makeToken(TokenKind.OpGreaterEq, ">="))
      } else {
        addToken(lexer, makeToken(TokenKind.OpGreater, ">"));
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

    default: {
      if (acceptString(lexer, "null", makeToken(TokenKind.NullLit, "null"))) {
        break;
      } else if (acceptString(lexer, "contains", makeToken(TokenKind.OpContains, "contains"))) {
        break;
      } else if (!isAlphabetical(currentChar(lexer))) {
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

export function tokenize(input: string): LexResult {
  const lexer: Lexer = { input, pos: 0, tokens: [], error: null };
  while (inputLeft(lexer) && !hasError(lexer)) {
    lex(lexer);
  }

  if (lexer.error !== null) {
    return { _type: "failed", error: lexer.error };
  } else {
    return { _type: "success", tokens: lexer.tokens };
  }
}

