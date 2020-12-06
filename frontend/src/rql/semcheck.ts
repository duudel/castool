import { BinaryOperator, UnaryOperator } from './types';
import { Ast, AstCont, AstExpr, AstExtend, AstProject, AstWhere, AstTable } from './parser';
import { makeAdt, Value, DataType, InputRow, TableDef, SemanticError, semError } from './common';

export type SemCheckEnv = {
  tables: {
    [name: string]: TableDef;
  }
};

export type SemCheckContext = {
  input: string;
  env: SemCheckEnv;
};

export interface CheckedExpr {
  evaluate: (row: InputRow) => Value;
  dataType: DataType;
}

export type CheckedQuery = makeAdt<{
  table: { name: string, tableDef: TableDef },
  cont: { source: CheckedQuery, op: CheckedOpQuery, tableDef: TableDef }
  where: { expr: CheckedExpr, tableDef: TableDef },
  project: { names: string[], tableDef: TableDef },
  extend: { name: string, expr: CheckedExpr, tableDef: TableDef },
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
type SemCheckExprResult = SemCheckResult<CheckedExpr>
type SemCheckQueryResult = SemCheckResult<CheckedQuery>

function semSuccess<R>(result: R): SemCheckSuccess<R> { return { success: true, result }; }
function semSuccessExpr(dataType: DataType, evaluate: (row: InputRow) => Value): SemCheckExprResult { return semSuccess({ dataType, evaluate }); }
function semSuccessQuery<Q extends CheckedQuery>(query: Q): SemCheckResult<Q> { return semSuccess(query); }
function semFailure(ctx: SemCheckContext, ast: Ast, error: string): SemCheckFailure { return { success: false, error: semError(error, ctx.input, ast.pos) }; }

function semCheckUnaryOp(ctx: SemCheckContext, source: TableDef, expr: AstExpr & { _type: "unaryOp" }): SemCheckExprResult {
  const check = semCheckExpr(ctx, source, expr.expr);
  if (!check.success) return check;
  const operandDataType = check.result.dataType;
  switch (expr.op) {
    case UnaryOperator.Not: {
      if (operandDataType !== "boolean")
        return semFailure(ctx, expr, "Cannot use unary ! on " + operandDataType);
      const evaluate = (row: InputRow) => !check.result.evaluate(row);
      return semSuccessExpr("boolean", evaluate);
    }
    case UnaryOperator.Plus: {
      if (operandDataType !== "number")
        return semFailure(ctx, expr, "Cannot use unary + on " + operandDataType);
      const evaluate = (row: InputRow) => +check.result.evaluate(row);
      return semSuccessExpr("number", evaluate);
    }
    case UnaryOperator.Minus: {
      if (operandDataType !== "number")
        return semFailure(ctx, expr, "Cannot use unary - on " + operandDataType);
      const evaluate = (row: InputRow) => -check.result.evaluate(row);
      return semSuccessExpr("number", evaluate);
    }
  }
}

function semCheckBinaryOp(ctx: SemCheckContext, source: TableDef, expr: AstExpr & { _type: "binaryOp" }): SemCheckExprResult {
  const a = semCheckExpr(ctx, source, expr.exprA);
  if (!a.success) return a;

  const b = semCheckExpr(ctx, source, expr.exprB);
  if (!b.success) return b;

  const aDataType = a.result.dataType;
  const bDataType = b.result.dataType;
  switch (expr.op) {
    case BinaryOperator.Minus:
      if (aDataType !== "number" || bDataType !== "number") {
        return semFailure(ctx, expr, `Cannot subtract '${bDataType}' from '${aDataType}'`);
      } else {
        const evaluate = (row: InputRow) => (a.result.evaluate(row) as number) - (b.result.evaluate(row) as number);
        return semSuccessExpr("number", evaluate);
      }
    case BinaryOperator.Plus:
      if (aDataType === "number") {
        if (bDataType === "number") {
          const evaluate = (row: InputRow) => (a.result.evaluate(row) as number) + (b.result.evaluate(row) as number);
          return semSuccessExpr("number", evaluate);
        } else if (bDataType === "string") {
          const evaluate = (row: InputRow) => (a.result.evaluate(row) as string) + (b.result.evaluate(row) as string);
          return semSuccessExpr("string", evaluate);
        }
      } else if (aDataType === "string") {
        if (bDataType === "number") {
          const evaluate = (row: InputRow) => (a.result.evaluate(row) as string) + (b.result.evaluate(row) as string);
          return semSuccessExpr("string", evaluate);
        } else if (bDataType === "string") {
          const evaluate = (row: InputRow) => (a.result.evaluate(row) as string) + (b.result.evaluate(row) as string);
          return semSuccessExpr("string", evaluate);
        }
      }
      return semFailure(ctx, expr, `Cannot subtract '${bDataType}' from '${aDataType}'`);
    case BinaryOperator.Multiply:
      if (aDataType !== "number" || bDataType !== "number") {
        return semFailure(ctx, expr, `Cannot multiply '${aDataType}' and '${bDataType}'`);
      } else {
        const evaluate = (row: InputRow) => (a.result.evaluate(row) as number) * (b.result.evaluate(row) as number);
        return semSuccessExpr("number", evaluate);
      }
    case BinaryOperator.Divide:
      if (aDataType !== "number" || bDataType !== "number") {
        return semFailure(ctx, expr, `Cannot divide '${aDataType}' with '${bDataType}'`);
      } else {
        const evaluate = (row: InputRow) => (a.result.evaluate(row) as number) / (b.result.evaluate(row) as number);
        return semSuccessExpr("number", evaluate);
      }
    case BinaryOperator.Contains:
      if (aDataType !== "string" || bDataType !== "string") {
        return semFailure(ctx, expr, `Cannot use contains with '${aDataType}' and '${bDataType}'`);
      } else {
        const evaluate = (row: InputRow) => (a.result.evaluate(row) as string).includes(b.result.evaluate(row) as string);
        return semSuccessExpr("boolean", evaluate);
      }
    case BinaryOperator.NotContains:
      if (aDataType !== "string" || bDataType !== "string") {
        return semFailure(ctx, expr, `Cannot use !contains with '${aDataType}' and '${bDataType}'`);
      } else {
        const evaluate = (row: InputRow) => !(a.result.evaluate(row) as string).includes(b.result.evaluate(row) as string);
        return semSuccessExpr("boolean", evaluate);
      }
  }
}

function semCheckExpr(ctx: SemCheckContext, source: TableDef, expr: AstExpr): SemCheckExprResult {
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
    case "stringLit": {
      const evaluate = (row: InputRow) => expr.value.value;
      return semSuccessExpr("string", evaluate);
    }
    case "numberLit": {
      const evaluate = (row: InputRow) => parseFloat(expr.value.value);
      return semSuccessExpr("number", evaluate);
    }
    case "dateLit": {
      const evaluate = (row: InputRow) => expr.value.value;
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
  const where: CheckedWhere = { _type: "where", tableDef: source, expr: exprResult.result };
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


