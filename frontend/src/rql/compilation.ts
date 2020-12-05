import * as rxjs from 'rxjs';
import * as rxop from 'rxjs/operators';

import { Ast, AstTable, AstCont, AstWhere, AstExtend, AstProject, AstExpr, BinaryOperator, UnaryOperator } from './types';

export type Value = string | number | boolean;
export type DataType = "string" | "number" | "boolean" | "date";
export type InputRow = { [key: string]: Value };
export type RowsObs = rxjs.Observable<InputRow>;
export type ResultObs = rxjs.Observable<InputRow>;
export type TableDef = {
  rows: RowsObs;
  columns: [string, DataType][];
};
export type Env = {
  tables: {
    [name: string]: TableDef;
  }
};

interface CompilationFailure {
  success: false;
  error: string;
}
interface CompilationSuccess {
  success: true;
  result: TableDef;
}
type CompilationResult = (env: Env) => (CompilationSuccess | CompilationFailure);

function success(result: TableDef): CompilationSuccess {
  return { success: true, result };
}

function failure(error: string): CompilationFailure {
  return { success: false, error };
}

//function assertAstType<T extends string, A extends Ast & { _type: T }>(ast: A, type: T): boolean { //: ast extends Ast & { _type: type } {
//function assertAstType<K extends string, A extends Ast & { _type: K }>(ast: A, type: K): A {
//  if (ast._type !== type) throw Error("Assert failure: expected " + type);
//  return ast as Ast & { _type: type };
//}

function compileTable(ast: AstTable): CompilationResult {
  const tableName = ast.name.value;
  return (env: Env) => {
    const table = env.tables[tableName];
    if (table === undefined) {
      return failure("No such table as '" + tableName + "' found");
    } else {
      return success(table);
    }
  };
}


interface SemCheckSuccess {
  success: true;
  dataType: DataType;
  evaluate: (row: InputRow) => Value;
}

interface SemCheckFailure {
  success: false;
  error: string;
}

type SemCheckResult = SemCheckSuccess | SemCheckFailure;

function semSuccess(dataType: DataType, evaluate: (row: InputRow) => Value): SemCheckSuccess { return { success: true, dataType, evaluate }; }
function semFailure(error: string): SemCheckFailure { return { success: false, error }; }

function semCheckUnaryOp(source: TableDef, expr: AstExpr & { _type: "unaryOp" }): SemCheckResult {
  const semCheckResult = semCheckExpr(source, expr.expr);
  if (!semCheckResult.success) return semCheckResult;
  const operandDataType = semCheckResult.dataType;
  switch (expr.op) {
    case UnaryOperator.Not: {
      if (operandDataType !== "boolean")
        return semFailure("Cannot use unary ! on " + operandDataType);
      const evaluate = (row: InputRow) => !semCheckResult.evaluate(row);
      return semSuccess("boolean", evaluate);
    }
    case UnaryOperator.Plus: {
      if (operandDataType !== "number")
        return semFailure("Cannot use unary + on " + operandDataType);
      const evaluate = (row: InputRow) => +semCheckResult.evaluate(row);
      return semSuccess("number", evaluate);
    }
    case UnaryOperator.Minus: {
      if (operandDataType !== "number")
        return semFailure("Cannot use unary - on " + operandDataType);
      const evaluate = (row: InputRow) => -semCheckResult.evaluate(row);
      return semSuccess("number", evaluate);
    }
  }
}

function semCheckBinaryOp(source: TableDef, expr: AstExpr & { _type: "binaryOp" }): SemCheckResult {
  const aSemCheck = semCheckExpr(source, expr.exprA);
  if (!aSemCheck.success) return aSemCheck;

  const bSemCheck = semCheckExpr(source, expr.exprB);
  if (!bSemCheck.success) return bSemCheck;

  const aDataType = aSemCheck.dataType;
  const bDataType = bSemCheck.dataType;
  switch (expr.op) {
    case BinaryOperator.Minus:
      if (aDataType !== "number" || bDataType !== "number") {
        return semFailure(`Cannot subtract '${bDataType}' from '${aDataType}'`);
      } else {
        const evaluate = (row: InputRow) => (aSemCheck.evaluate(row) as number) - (bSemCheck.evaluate(row) as number);
        return semSuccess("number", evaluate);
      }
    case BinaryOperator.Plus:
      if (aDataType === "number") {
        if (bDataType === "number") {
          const evaluate = (row: InputRow) => (aSemCheck.evaluate(row) as number) + (bSemCheck.evaluate(row) as number);
          return semSuccess("number", evaluate);
        } else if (bDataType === "string") {
          const evaluate = (row: InputRow) => (aSemCheck.evaluate(row) as string) + (bSemCheck.evaluate(row) as string);
          return semSuccess("string", evaluate);
        }
      } else if (aDataType === "string") {
        if (bDataType === "number") {
          const evaluate = (row: InputRow) => (aSemCheck.evaluate(row) as string) + (bSemCheck.evaluate(row) as string);
          return semSuccess("string", evaluate);
        } else if (bDataType === "string") {
          const evaluate = (row: InputRow) => (aSemCheck.evaluate(row) as string) + (bSemCheck.evaluate(row) as string);
          return semSuccess("string", evaluate);
        }
      }
      return semFailure(`Cannot subtract '${bDataType}' from '${aDataType}'`);
    case BinaryOperator.Multiply:
      if (aDataType !== "number" || bDataType !== "number") {
        return semFailure(`Cannot multiply '${aDataType}' and '${bDataType}'`);
      } else {
        const evaluate = (row: InputRow) => (aSemCheck.evaluate(row) as number) * (bSemCheck.evaluate(row) as number);
        return semSuccess("number", evaluate);
      }
    case BinaryOperator.Divide:
      if (aDataType !== "number" || bDataType !== "number") {
        return semFailure(`Cannot divide '${aDataType}' with '${bDataType}'`);
      } else {
        const evaluate = (row: InputRow) => (aSemCheck.evaluate(row) as number) / (bSemCheck.evaluate(row) as number);
        return semSuccess("number", evaluate);
      }
    case BinaryOperator.Contains:
      if (aDataType !== "string" || bDataType !== "string") {
        return semFailure(`Cannot use contains with '${aDataType}' and '${bDataType}'`);
      } else {
        const evaluate = (row: InputRow) => (aSemCheck.evaluate(row) as string).includes(bSemCheck.evaluate(row) as string);
        return semSuccess("boolean", evaluate);
      }
    case BinaryOperator.NotContains:
      if (aDataType !== "string" || bDataType !== "string") {
        return semFailure(`Cannot use !contains with '${aDataType}' and '${bDataType}'`);
      } else {
        const evaluate = (row: InputRow) => !(aSemCheck.evaluate(row) as string).includes(bSemCheck.evaluate(row) as string);
        return semSuccess("boolean", evaluate);
      }
  }
  //return semFailure("Not implemented binary operator: " + expr.op);
}

function semCheckExpr(source: TableDef, expr: AstExpr): SemCheckResult {
  switch (expr._type) {
    case "column": {
      const columnDef = source.columns.find(([name]) => name === expr.name.value);
      if (!columnDef) {
        return semFailure("No such column as '" + expr.name.value + "' found");
      } else {
        const [name, dataType] = columnDef;
        const evaluate = (row: InputRow) => row[name];
        return semSuccess(dataType, evaluate);
      }
    }
    case "stringLit": {
      const evaluate = (row: InputRow) => expr.value.value;
      return semSuccess("string", evaluate);
    }
    case "numberLit": {
      const evaluate = (row: InputRow) => parseFloat(expr.value.value);
      return semSuccess("number", evaluate);
    }
    case "dateLit": {
      const evaluate = (row: InputRow) => expr.value.value;
      return semSuccess("date", evaluate);
    }
    case "unaryOp":
      return semCheckUnaryOp(source, expr);
    case "binaryOp":
      return semCheckBinaryOp(source, expr);
  }
}

interface ExprCompilationSuccess {
  success: true;
  evaluate: (row: InputRow) => Value;
  dataType: DataType;
}

interface ExprCompilationFailure {
  success: false;
  error: string;
}

type ExprCompilationResult = ExprCompilationSuccess | ExprCompilationFailure;

function exprSuccess(evaluate: (row: InputRow) => Value, dataType: DataType): ExprCompilationSuccess { return { success: true, evaluate, dataType }; }
function exprFailure(error: string): ExprCompilationFailure { return { success: false, error }; }

function compileExpr(source: TableDef, expr: AstExpr): ExprCompilationResult {
  const semCheckResult = semCheckExpr(source, expr);
  if (!semCheckResult.success) {
    return exprFailure(semCheckResult.error);
  }
  const evaluate = semCheckResult.evaluate;
  return exprSuccess(evaluate, semCheckResult.dataType);
}

function compileProject(source: TableDef, ast: AstProject): CompilationResult {
  const selectedColumns = ast.names.map(n => n.value);
  const errors = selectedColumns.reduce((acc, name) => {
    console.log("Source", source);
    if (source.columns.find(([columnName]) => name === columnName)) {
      return acc;
    } else {
      const error = "Column '" + name + "' not found";
      return acc.concat([error]);
    }
  }, [] as string[]);
  if (errors.length > 0) return (env: Env) => failure(errors.join("; "));

  const columns = selectedColumns.map(column => {
    const columnDef = source.columns.find(([name]) => name === column);
    if (columnDef === undefined) {
      throw Error("Unreachable");
    } else {
      return columnDef;
    }
  });

  return (env: Env) => {
    const rows = source.rows.pipe(
      rxop.map(row => {
        return selectedColumns.reduce((acc, column) => {
          acc[column] = row[column];
          return acc;
        }, {} as InputRow);
      })
    );
    return success({ columns, rows });
  };
}

function compileExtend(source: TableDef, op: AstExtend): CompilationResult {
  const exprResult = compileExpr(source, op.expr);
  if (!exprResult.success) return (env: Env) => failure(exprResult.error);
  const name = op.name.value;
  return (env) => {
    const rows = source.rows.pipe(
      rxop.map(row => {
        const value = exprResult.evaluate(row);
        const mapped = { ...row, [name]: value };
        return mapped;
      })
    );
    const columns = source.columns.concat([name, exprResult.dataType]);
    return success({ columns, rows });
  };
}

function compileWhere(source: TableDef, op: AstWhere): CompilationResult {
  const exprResult = compileExpr(source, op.expr);
  if (!exprResult.success) return (env: Env) => failure(exprResult.error);
  if (exprResult.dataType !== "boolean") return (env: Env) => failure("Where clause expression must be boolean");
  return (env: Env) => {
    const rows = source.rows.pipe(
      rxop.filter(row => {
        const value = exprResult.evaluate(row);
        return value === true;
      })
    );
    return success({ columns: source.columns, rows });
  };
}

function compileTopLevelOp(source: TableDef, op: Ast): CompilationResult {
  switch (op._type) {
    case "project":
      return compileProject(source, op);
    case "extend":
      return compileExtend(source, op);
    case "where":
      return compileWhere(source, op);
  }
  throw Error("Unreachable");
}

function compileCont(ast: AstCont): CompilationResult {
  const sourceFn = compileTopLevel(ast.source);
  return (env: Env) => {
    const sourceResult = sourceFn(env);
    if (!sourceResult.success) return sourceResult;
    const opFn = compileTopLevelOp(sourceResult.result, ast.op);
    return opFn(env);
  };
}

function compileTopLevel(ast: Ast): CompilationResult {
  switch (ast._type) {
    case "table":
      return compileTable(ast);
    case "cont":
      return compileCont(ast);
  }
  throw Error("Unreachable");
}

export function compile(ast: Ast): CompilationResult {
  return compileTopLevel(ast);
}

