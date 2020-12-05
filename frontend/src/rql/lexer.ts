import { TokenKind, Token, Lexer } from './types';

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

export function tokenize(input: string): Token[] | string {
  const lexer: Lexer = { input, pos: 0, tokens: [], error: null };
  while (inputLeft(lexer) && !hasError(lexer)) {
    lex(lexer);
  }

  return (lexer.error === null) ? lexer.tokens : lexer.error;
}

