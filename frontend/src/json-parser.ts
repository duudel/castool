
type ParserResultValue = string;
type ParserResultClass = string;
export type ParserResultToken = [ParserResultValue, ParserResultClass];
export type ParserResults = ParserResultToken[];

export interface Parser {
  value: string;
  pos: number;
  line: number;
  lineLimit: number;
  results: ParserResults;
}

export const createParser = (value: string, lineLimit?: number): Parser => ({
  value,
  pos: 0,
  line: 0,
  lineLimit: (lineLimit === undefined) ? Infinity : lineLimit,
  results: []
});

const errorLocation = (parser: Parser) => {
  let linesBefore = 3;
  let linesAfter = 4;
  let start = parser.pos;
  while (start > 0) {
    if (parser.value.charAt(start) === '\n') {
      linesBefore--;
      if (linesBefore === 0) break;
    }
    start--;
  }
  start++;
  let end = parser.pos + 1;
  while (end < parser.value.length) {
    if (parser.value.charAt(end) === '\n') {
      linesAfter--;
      if (linesAfter === 0) break;
    }
    end++; 
  }
  const line = parser.value.substr(start, end - start);
  const before = parser.value.substr(start, parser.pos - start);
  const after = parser.value.substr(parser.pos + 1, end - parser.pos - 1);
  const c = parser.value.charAt(parser.pos);
  return before + "<" + c + "(" + c.charCodeAt(0) + ")>" + after + "\nL: " + line;
};

const syntaxError = (parser: Parser, error: string) => {
  const message = error + " at\n" + errorLocation(parser);
  throw Error(message);
}

const isNumeric = (c: string): boolean => {
  return '0' <= c && c <= '9';
};
const isWhitespace = (c: string): boolean => {
  switch (c) {
    case ' ': case '\t': case '\f': case '\n': case '\r':
      return true;
    default:
      return false;
  }
};

const current = (parser: Parser): string => parser.value.charAt(parser.pos);
const inputLeft = (parser: Parser): boolean => (parser.pos < parser.value.length) && (parser.line < parser.lineLimit);
const lineLimitReached = (parser: Parser): boolean => !(parser.line < parser.lineLimit);

const current_is = (parser: Parser, c: string): boolean => inputLeft(parser) && (current(parser) === c);

const add = (parser: Parser, token: ParserResultToken) => {
  parser.results.push(token);
};

const addSubstring = (parser: Parser, start: number, cls: ParserResultClass) => {
  const value = parser.value.substr(start, parser.pos - start);
  parser.results.push([value, cls]);
};

const accept = (parser: Parser, str: string, token?: ParserResultToken): boolean => {
  if ((parser.pos < parser.value.length) && (parser.value.substr(parser.pos, str.length) === str))
  {
    parser.pos += str.length;
    if (token !== undefined) {
      add(parser, token)
    }
    return true;
  }
  return false;
};

const acceptWhile = (parser: Parser, predicate: (v: string) => boolean): boolean => {
  var result = false;
  while (inputLeft(parser) && predicate(current(parser))) {
    parser.pos++;
    result = true;
  }
  return result;
};

const eatWhitespace = (parser: Parser): void => {
  const start = parser.pos;
  while (inputLeft(parser) && isWhitespace(current(parser))) {
    if (current(parser) === '\n') {
      parser.line++;
    }
    parser.pos++;
  }
  if (parser.pos > start) {
    add(parser, [parser.value.substr(start, parser.pos - start), ""]);
  }
};

export const parseField = (parser: Parser): boolean => {
  if (!accept(parser, '"')) return false;

  const fieldStart = parser.pos - 1;
  while ((parser.pos < parser.value.length) && !current_is(parser, '"')) parser.pos++;
  if (!accept(parser, '"')) {
    syntaxError(parser, "No ending quote for propery");
  }

  const field = parser.value.substr(fieldStart, parser.pos - fieldStart);
  add(parser, [field, "json-field"]);
  return true;
}

export const parseJson = (parser: Parser): ParserResults => {
  eatWhitespace(parser);

  if (!inputLeft(parser)) return parser.results;

  switch (current(parser)) {
    case '{': {
      accept(parser, '{', ['{', "json-brace"]);
      eatWhitespace(parser);
      if (accept(parser, '}', ['}', "json-brace"])) {
        // Empty object
        break;
      }
      while (true) {
        eatWhitespace(parser);
        if (lineLimitReached(parser)) return parser.results;
        if (!parseField(parser)) {
          syntaxError(parser, "Expected property name");
        }
        eatWhitespace(parser);
        if (lineLimitReached(parser)) return parser.results;
        if (!accept(parser, ':', [':', ""])) {
          syntaxError(parser, "Expected ':'");
        }

        parseJson(parser);
        eatWhitespace(parser);
        if (lineLimitReached(parser)) return parser.results;

        if (accept(parser, '}', ['}', "json-brace"])) {
          break;
        }
        if (!accept(parser, ',', [',', "json-comma"])) {
          syntaxError(parser, "Expected end of object '}' or ',' and a new property");
        }
      }
    } break;
    case '[': {
      accept(parser, '[', ['[', "json-bracket"]);
      eatWhitespace(parser);
      if (lineLimitReached(parser)) return parser.results;

      if (accept(parser, ']', [']', "json-bracket"])) {
        // Empty array
        break;
      }

      while (true) {
        parseJson(parser);
        eatWhitespace(parser);
        if (lineLimitReached(parser)) return parser.results;

        if (!accept(parser, ',', [',', "json-comma"]))
          break;
      }
      if (!accept(parser, ']', [']', "json-bracket"])) {
        syntaxError(parser, "Expected array end ']'");
      }
    } break;
    case '"': {
      // TODO: parse escape sequences, at least for \".
      const strStart = parser.pos;
      parser.pos++;
      while ((parser.pos < parser.value.length) && !current_is(parser, '"')) parser.pos++;
      accept(parser, '"');

      addSubstring(parser, strStart, "json-str");
    } break;
    case 'n': {
      accept(parser, "null", ["null", "json-null"]);
    } break;
    case 't': {
      accept(parser, "true", ["true", "json-bool"]);
    } break;
    case 'f': {
      accept(parser, "false", ["false", "json-bool"]);
    } break;
    case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9': {
      const numStart = parser.pos;
      accept(parser, '+') || accept(parser, '-');
      acceptWhile(parser, isNumeric);
      if (accept(parser, '.')) {
        acceptWhile(parser, isNumeric);
      }
      if (accept(parser, 'e') || accept(parser, 'E')) {
        accept(parser, '+') || accept(parser, '-');
        acceptWhile(parser, isNumeric);
      }

      addSubstring(parser, numStart, "json-num");
    } break;
    default:
      syntaxError(parser, "Invalid character");
  }

  return parser.results;
}

