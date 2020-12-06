
//export type { UnaryOperator, BinaryOperator } from './types';

import { Token, tokenize } from './lexer';
import { Ast, parseTokens } from './parser';
import { semCheck, SemCheckEnv } from './semcheck';
import { CompilationEnv, CompilationResult, compileChecked, TableSource } from './compilation';
import {RqlError} from './common';

export interface CompileResult {
  tokens: Token[] | null;
  ast: Ast | null;
  program: CompilationResult | null;
  error: RqlError | null;
}

export function compile(input: string, env: CompilationEnv): CompileResult {
  const lexResult = tokenize(input);
  if (lexResult._type === "failed") return {
    tokens: null, ast: null, program: null, error: lexResult.error
  };

  const tokens = lexResult.tokens;
  const parserResult = parseTokens(input, tokens);
  if (parserResult._type === "failed") return {
    tokens, ast: null, program: null, error: parserResult.error
  };

  function mapObject<V1, V2, O extends { [key: string]: V1} >(obj: O, f: (v: V1) => V2): { [key: string]: V2 } {
    const result: { [key: string]: V2 } = {};
    Object.keys(obj).forEach(key => {
      result[key] = f(obj[key]);
    });
    return result;
  }

  const semCheckEnv: SemCheckEnv = {
    tables: mapObject(env.tables, (source: TableSource) => source.tableDef )
  };

  const ast = parserResult.ast;
  const semCheckResult = semCheck(input, ast, semCheckEnv);
  if (!semCheckResult.success) return {
    tokens, ast, program: null, error: semCheckResult.error
  };

  const program = compileChecked(semCheckResult.result);

  return {
    tokens, ast, program, error: null
  };
}

export type{ DataType, Value } from './common';
export { parse } from './parser';
export type{ CompilationEnv, RowsObs } from'./compilation';

