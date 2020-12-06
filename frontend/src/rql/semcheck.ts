import { BinaryOperator, UnaryOperator } from './types';
import { Ast, AstCont, AstExpr, AstExtend, AstProject, AstWhere, AstTable } from './parser';
import { makeAdt, Value, DataType, InputRow, TableDef, SemanticError, semError, DataTypeFrom } from './common';

export type SemCheckEnv = {
  tables: {
    [name: string]: TableDef;
  }
};

export type SemCheckContext = {
  input: string;
  env: SemCheckEnv;
};

export interface CheckedExpr<T extends Value> {
  evaluate: (row: InputRow) => T;
  dataType: DataTypeFrom<T>;
}

export type CheckedQuery = makeAdt<{
  table: { name: string, tableDef: TableDef },
  cont: { source: CheckedQuery, op: CheckedOpQuery, tableDef: TableDef }
  where: { expr: CheckedExpr<boolean>, tableDef: TableDef },
  project: { names: string[], tableDef: TableDef },
  extend: { name: string, expr: CheckedExpr<Value>, tableDef: TableDef },
}>;

export type CheckedTable = CheckedQuery & { _type: "table" };
export type CheckedCont = CheckedQuery & { _type: "cont" };
export type CheckedWhere = CheckedQuery & { _type: "where" };
export type CheckedProject = CheckedQuery & { _type: "project" };
export type CheckedExtend = CheckedQuery & { _type: "extend" };
export type CheckedOpQuery = CheckedWhere | CheckedProject | CheckedExtend;

interface SemCheckSuccess<R> {
  success: true;
  result: R;
}

interface SemCheckFailure {
  success: false;
  error: SemanticError;
}

type SemCheckResult<R> = SemCheckSuccess<R> | SemCheckFailure;
type SemCheckExprResult<T extends Value> = SemCheckResult<CheckedExpr<T>>
type SemCheckQueryResult = SemCheckResult<CheckedQuery>

function semSuccess<R>(result: R): SemCheckSuccess<R> { return { success: true, result }; }
function semSuccessExpr<T extends Value, D extends DataTypeFrom<T>>(dataType: D, evaluate: (row: InputRow) => T): SemCheckExprResult<T> { return semSuccess({ dataType, evaluate }); }
function semSuccessQuery<Q extends CheckedQuery>(query: Q): SemCheckResult<Q> { return semSuccess(query); }
function semFailure(ctx: SemCheckContext, ast: Ast, error: string): SemCheckFailure { return { success: false, error: semError(error, ctx.input, ast.pos) }; }

function evalUnary<T extends Value>(evaluate: (row: InputRow) => Value, then: (x: T) => (T | null)): (row: InputRow) => (T | null) {
  return (row: InputRow) => {
    const x = evaluate(row) as T;
    if (x === null) return null;
    return then(x);
  }
}

function evalBinary<A extends Value, B extends Value, R extends Value>(
  evalA: (row: InputRow) => Value,
  evalB: (row: InputRow) => Value,
  then: (a: A, b: B) => (R | null)
): (row: InputRow) => (R | null) {
  return (row: InputRow) => {
    const a = evalA(row) as A;
    const b = evalB(row) as B;
    if (a === null) return null;
    if (b === null) return null;
    return then(a, b);
  }
}

function semCheckUnaryOp(ctx: SemCheckContext, source: TableDef, expr: AstExpr & { _type: "unaryOp" }): SemCheckExprResult<Value> {
  const check = semCheckExpr(ctx, source, expr.expr);
  if (!check.success) return check;
  const { dataType: operandDataType, evaluate: evaluateExpr } = check.result;
  switch (expr.op) {
    case UnaryOperator.Not: {
      if (operandDataType !== "boolean")
        return semFailure(ctx, expr, "Cannot use unary ! on " + operandDataType);
      const evaluate = (row: InputRow) => !evaluateExpr(row);
      return semSuccessExpr("boolean", evaluate);
    }
    case UnaryOperator.Plus: {
      if (operandDataType === "number") {
        const evaluate = evalUnary<number>(evaluateExpr, x => +x);
        return semSuccessExpr("number", evaluate);
      }
      return semFailure(ctx, expr, "Cannot use unary + on " + operandDataType);
    }
    case UnaryOperator.Minus: {
      if (operandDataType !== "number") {
        const evaluate = evalUnary<number>(evaluateExpr, x => -x);
        return semSuccessExpr("number", evaluate);
      }
      return semFailure(ctx, expr, "Cannot use unary - on " + operandDataType);
    }
  }
}

function semCheckBinaryOp(ctx: SemCheckContext, source: TableDef, expr: AstExpr & { _type: "binaryOp" }): SemCheckExprResult<Value> {
  const a = semCheckExpr(ctx, source, expr.exprA);
  if (!a.success) return a;

  const b = semCheckExpr(ctx, source, expr.exprB);
  if (!b.success) return b;

  const { dataType: dataTypeA, evaluate: evalA } = a.result;
  const { dataType: dataTypeB, evaluate: evalB } = a.result;
  switch (expr.op) {
    case BinaryOperator.Minus:
      if (dataTypeA !== "number" || dataTypeB !== "number") {
        return semFailure(ctx, expr, `Cannot subtract '${dataTypeB}' from '${dataTypeA}'`);
      } else {
        const evaluate = evalBinary<number, number, number>(evalA, evalB, (a, b) => a - b);
        return semSuccessExpr("number", evaluate);
      }
    case BinaryOperator.Plus:
      if (dataTypeA === "number") {
        if (dataTypeB === "number") {
          const evaluate = evalBinary<number, number, number>(evalA, evalB, (a, b) => a + b);
          return semSuccessExpr("number", evaluate);
        } else if (dataTypeB === "string") {
          const evaluate = evalBinary<number, string, string>(evalA, evalB, (a, b) => a + b);
          return semSuccessExpr("string", evaluate);
        }
      } else if (dataTypeA === "string") {
        if (dataTypeB === "number") {
          const evaluate = evalBinary<string, number, string>(evalA, evalB, (a, b) => a + b);
          return semSuccessExpr("string", evaluate);
        } else if (dataTypeB === "string") {
          const evaluate = evalBinary<string, string, string>(evalA, evalB, (a, b) => a + b);
          return semSuccessExpr("string", evaluate);
        }
      }
      return semFailure(ctx, expr, `Cannot subtract '${dataTypeB}' from '${dataTypeA}'`);
    case BinaryOperator.Multiply:
      if (dataTypeA !== "number" || dataTypeB !== "number") {
        return semFailure(ctx, expr, `Cannot multiply '${dataTypeA}' and '${dataTypeB}'`);
      } else {
        const evaluate = evalBinary<number, number, number>(evalA, evalB, (a, b) => a * b);
        return semSuccessExpr("number", evaluate);
      }
    case BinaryOperator.Divide:
      if (dataTypeA !== "number" || dataTypeB !== "number") {
        return semFailure(ctx, expr, `Cannot divide '${dataTypeA}' with '${dataTypeB}'`);
      } else {
        const evaluate = evalBinary<number, number, number>(evalA, evalB, (a, b) => a / b);
        return semSuccessExpr("number", evaluate);
      }
    case BinaryOperator.Equal:
    case BinaryOperator.NotEqual:
      if (dataTypeA !== dataTypeB) {
        return semFailure(ctx, expr, `Cannot compare '${dataTypeA}' with '${dataTypeB}'`);
      } else {
        switch (expr.op) {
          case BinaryOperator.Equal: {
            const evaluate = (row: InputRow) => a.result.evaluate(row) === b.result.evaluate(row);
            //const ev = evalBinary(evalA, evalB, (a, b) => a === b)
            return semSuccessExpr("boolean", evaluate);
          }
          case BinaryOperator.NotEqual: {
            const evaluate = (row: InputRow) => a.result.evaluate(row) !== b.result.evaluate(row);
            return semSuccessExpr("boolean", evaluate);
          }
        }
      }
    case BinaryOperator.Contains:
      if (dataTypeA !== "string" || dataTypeB !== "string") {
        return semFailure(ctx, expr, `Cannot use contains with '${dataTypeA}' and '${dataTypeB}'`);
      } else {
        const evaluate = evalBinary<string, string, boolean>(evalA, evalB, (a, b) => a.includes(b)); //(row: InputRow) => (a.result.evaluate(row) as string).includes(b.result.evaluate(row) as string);
        return semSuccessExpr("boolean", evaluate);
      }
    case BinaryOperator.NotContains:
      if (dataTypeA !== "string" || dataTypeB !== "string") {
        return semFailure(ctx, expr, `Cannot use !contains with '${dataTypeA}' and '${dataTypeB}'`);
      } else {
        const evaluate = evalBinary<string, string, boolean>(evalA, evalB, (a, b) => !a.includes(b)); //(row: InputRow) => !(a.result.evaluate(row) as string).includes(b.result.evaluate(row) as string);
        return semSuccessExpr("boolean", evaluate);
      }
  }
}

function semCheckExpr(ctx: SemCheckContext, source: TableDef, expr: AstExpr): SemCheckExprResult<Value> {
  switch (expr._type) {
    case "column": {
      const columnDef = source.columns.find(([name]) => name === expr.name.value);
      if (!columnDef) {
        return semFailure(ctx, expr, "No such column as '" + expr.name.value + "' found");
      } else {
        const [name, dataType] = columnDef;
        const evaluate = (row: InputRow) => row[name];
        return semSuccessExpr(dataType, evaluate);
      }
    }
    case "nullLit": {
      const evaluate = (row: InputRow) => null;
      return semSuccessExpr("null", evaluate);
    }
    case "stringLit": {
      const evaluate = (row: InputRow) => expr.value.value;
      return semSuccessExpr("string", evaluate);
    }
    case "numberLit": {
      const evaluate = (row: InputRow) => parseFloat(expr.value.value);
      return semSuccessExpr("number", evaluate);
    }
    case "dateLit": {
      const evaluate = (row: InputRow) => new Date(expr.value.value);
      return semSuccessExpr("date", evaluate);
    }
    case "unaryOp":
      return semCheckUnaryOp(ctx, source, expr);
    case "binaryOp":
      return semCheckBinaryOp(ctx, source, expr);
  }
}

function semCheckProject(ctx: SemCheckContext, source: TableDef, op: AstProject): SemCheckResult<CheckedProject> {
  const names = op.names.map(n => n.value);
  const sourceColumnNames = source.columns.map(([name]) => name);

  const missingColumns = names.filter(name => !sourceColumnNames.includes(name));
  if (missingColumns.length > 1) return semFailure(ctx, op, `Columns ${missingColumns.map(n => "'" + n +"'").join(", ")} not found from the source table`);
  else if (missingColumns.length > 0) return semFailure(ctx, op, `Column '${missingColumns[0]}' not found from the source table`);
  
  const columns = names.map(column => {
    const columnDef = source.columns.find(([name]) => name === column);
    if (columnDef === undefined) {
      throw Error("Unreachable");
    } else {
      return columnDef;
    }
  });

  const tableDef: TableDef = { columns }
  const project: CheckedProject = { _type: "project", tableDef, names };
  return semSuccessQuery(project);
}

function semCheckExtend(ctx: SemCheckContext, source: TableDef, op: AstExtend): SemCheckResult<CheckedExtend> {
  const exprResult = semCheckExpr(ctx, source, op.expr);
  if (!exprResult.success) return exprResult;
  const name = op.name.value;
  const tableDef: TableDef = {
    columns: source.columns.concat([[name, exprResult.result.dataType]])
  };
  const extend: CheckedExtend = { _type: "extend", tableDef, name, expr: exprResult.result };
  return semSuccessQuery(extend);
}

function semCheckWhere(ctx: SemCheckContext, source: TableDef, op: AstWhere): SemCheckResult<CheckedWhere> {
  const exprResult = semCheckExpr(ctx, source, op.expr);
  if (!exprResult.success) return exprResult;
  if (exprResult.result.dataType !== "boolean") return semFailure(ctx, op.expr, "Where clause expression must be boolean");
  const where: CheckedWhere = { _type: "where", tableDef: source, expr: exprResult.result as CheckedExpr<boolean> };
  return semSuccessQuery(where);
}

function semCheckTopLevelOp(ctx: SemCheckContext, source: TableDef, op: Ast): SemCheckResult<CheckedOpQuery> {
  switch (op._type) {
    case "project":
      return semCheckProject(ctx, source, op);
    case "extend":
      return semCheckExtend(ctx, source, op);
    case "where":
      return semCheckWhere(ctx, source, op);
  }
  throw Error("Unreachable");
}

function semCheckCont(ctx: SemCheckContext, ast: AstCont): SemCheckQueryResult {
  const sourceResult = semCheckTopLevel(ctx, ast.source);
  if (!sourceResult.success) return sourceResult;
  const source = sourceResult.result;

  const opResult = semCheckTopLevelOp(ctx, source.tableDef, ast.op);
  if (!opResult.success) return opResult;
  const op = opResult.result;

  const query: CheckedQuery = { _type: "cont", source, op: op, tableDef: op.tableDef };
  return semSuccessQuery(query);
}

function semCheckTable(ctx: SemCheckContext, ast: AstTable): SemCheckQueryResult {
  const tableName = ast.name.value;
  const tableDef = ctx.env.tables[tableName];
  if (tableDef === undefined) return semFailure(ctx, ast, `No such table as '${tableName}' found`);

  const table: CheckedTable = { _type: "table", tableDef, name: tableName };
  return semSuccessQuery(table);
}

export function semCheckTopLevel(ctx: SemCheckContext, ast: Ast): SemCheckQueryResult {
  switch (ast._type) {
    case "table":
      return semCheckTable(ctx, ast);
    case "cont":
      return semCheckCont(ctx, ast);
  }
  throw Error("Unreachable");
}

export function semCheck(input: string, ast: Ast, env: SemCheckEnv): SemCheckQueryResult {
  const ctx: SemCheckContext = { input, env };
  return semCheckTopLevel(ctx, ast);
}


