
enum TokenType {
  Keyword,
  Ident,
  Number,
  String,
  Special,
  Comment,
  Whitespace,
}

interface Token {
  s: string;
  offs: number;
}

interface TokenLine {
  tokens: Token[];
}

export interface LexResult {
  success: boolean;
  lines: TokenLine[] | null;
}

type TokenMatcher = RegExp | string[] | null;

export interface TokenMatchers {
  keyword: TokenMatcher;
  ident: TokenMatcher;
  number: TokenMatcher;
  string: TokenMatcher;
  special: TokenMatcher;
  comment: TokenMatcher;
}

export interface LexOptions {
  tokenMatchers: TokenMatchers;
}

export interface Lexer {
  options: LexOptions;
  input: string[];

}


function lex(lexer: Lexer, input: string): LexResult {
  return ({success: false, lines: null});
}

function reLex(lexer: Lexer, input: string[], offset: number, line: number): LexResult {
  return ({success: false, lines: null});
}

export function createLexer(options: LexOptions): Lexer {
  return {
    options,
    input: []
  };
}


//import { createLexer, Lexer } from './lexer';

const le: Lexer = createLexer({
  tokenMatchers: {
    keyword: ["if", "else", "function"],
    ident: /[A-Za-z]+[A-Za-z0-9]*/,
    number: /[+-]?[0-9]+(\.[0-9]+([eE]?[0-9]+)?)?/,
    string: /"[^"]*"/,
    special:  null,
    comment: /\/\*.*\/\*/,
  }
});

