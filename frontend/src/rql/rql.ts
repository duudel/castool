
//export type { UnaryOperator, BinaryOperator } from './types';

import { Token, tokenize } from './lexer';
import { Ast, parseTokens } from './parser';
import { CheckedQuery, semCheckEnvFrom, semCheck, SemCheckEnv } from './semcheck';
import { CompilationResult, compileChecked } from './compilation';
import { BuiltinFunctions, CompilationEnv, ExecutionEnv, RqlError } from './common';

export interface CompileResult {
  tokens: Token[] | null;
  ast: Ast | null;
  checked: CheckedQuery | null;
  program: CompilationResult | null;
  error: RqlError | null;
}

const builtinFunctions: BuiltinFunctions = {
  parse_int: {
    parameters: [{str: "string"}],
    returnType: "number",
    func: (str: string): number => parseInt(str)
  },
  parse_float: {
    parameters: [{str: "string"}],
    returnType: "number",
    func: (str: string): number => parseFloat(str)
  },
  starts_with: {
    parameters: [{str: "string"}, {search: "string"}],
    returnType: "boolean",
    func: (str: string, search: string): boolean => str.startsWith(search)
  },
  ends_with: {
    parameters: [{str: "string"}, {search: "string"}],
    returnType: "boolean",
    func: (str: string, search: string): boolean => str.endsWith(search)
  },
  avg: {
    parameters: [{acc: "number"}, {x: "number"}],
    returnType: "number",
    func: (acc: number, x: number): number => acc + x
  }
};

export function compile(input: string, env: CompilationEnv): CompileResult {
  const lexResult = tokenize(input);
  if (lexResult._type === "failed") return {
    tokens: null, ast: null, checked: null, program: null, error: lexResult.error
  };

  const tokens = lexResult.tokens;
  if (tokens.length === 0) return {
    tokens, ast: null, checked: null, program: null, error: null
  };

  const parserResult = parseTokens(input, tokens);
  if (parserResult._type === "failed") return {
    tokens, ast: null, checked: null, program: null, error: parserResult.error
  };

  const execEnv: ExecutionEnv = { ...env, builtinFunctions };
  const semCheckEnv: SemCheckEnv = semCheckEnvFrom(execEnv);

  const ast = parserResult.ast;
  const semCheckResult = semCheck(input, ast, semCheckEnv);
  if (!semCheckResult.success) return {
    tokens, ast, checked: null, program: null, error: semCheckResult.error
  };

  const checked = semCheckResult.result;
  const program = compileChecked(checked);

  return {
    tokens, ast, checked, program, error: null
  };
}

export type{ DataType, Value, TableDef, CompilationEnv, UserFunctions, RowsObs } from './common';
export { parse } from './parser';

