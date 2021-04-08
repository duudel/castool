import { BinaryOperator, UnaryOperator } from './types';
import { Ast, AstCont, AstExpr, AstExtend, AstProject, AstWhere, AstSummarize, AstTable, Aggregation, AstOrderBy } from './parser';
import {
  makeAdt,
  Value,
  DataType,
  FunctionDef,
  InputRow,
  TableDef,
  TableSource,
  SemanticError,
  semError,
  DataTypeFrom,
  ExecutionEnv,
  BuiltinFunctions,
  UserFunctions,
  deconstructParam
} from './common';

export type SemCheckEnv = {
  tables: {
    [name: string]: TableDef;
  },
  builtinFunctions: BuiltinFunctions;
  userFunctions?: UserFunctions;
};

function mapObject<V1, V2, O extends { [key: string]: V1} >(obj: O, f: (v: V1) => V2): { [key: string]: V2 } {
  const result: { [key: string]: V2 } = {};
  Object.keys(obj).forEach(key => {
    result[key] = f(obj[key]);
  });
  return result;
}

export function semCheckEnvFrom(env: ExecutionEnv): SemCheckEnv {
  return {
    tables: mapObject(env.tables, (source: TableSource) => source.tableDef ),
    builtinFunctions: env.builtinFunctions,
    userFunctions: env.userFunctions,
  };
}

export type SemCheckContext = {
  input: string;
  env: SemCheckEnv;
};

export interface CheckedExpr<T extends Value> {
  evaluate: (row: InputRow) => T;
  dataType: DataTypeFrom<T>;
}

export interface CheckedAggr<T extends Value> {
  name: string;
  aggregate: (accum: T, row: InputRow) => T;
  initialValue: T;
  finalPass?: (accum: T, N: number) => Value;
  dataType: DataTypeFrom<T>;
}

export type CheckedQuery = makeAdt<{
  table: { name: string, tableDef: TableDef },
  cont: { source: CheckedQuery, op: CheckedOpQuery, tableDef: TableDef }
  where: { expr: CheckedExpr<boolean>, tableDef: TableDef },
  project: { names: string[], tableDef: TableDef },
  extend: { name: string, expr: CheckedExpr<Value>, tableDef: TableDef },
  summarize: { aggregations: CheckedAggr<Value>[], groupBy: string[], tableDef: TableDef },
  orderBy: { names: string[], order: "asc" | "desc", tableDef: TableDef },
}>;

export type CheckedTable = CheckedQuery & { _type: "table" };
export type CheckedCont = CheckedQuery & { _type: "cont" };
export type CheckedWhere = CheckedQuery & { _type: "where" };
export type CheckedProject = CheckedQuery & { _type: "project" };
export type CheckedExtend = CheckedQuery & { _type: "extend" };
export type CheckedSummarize = CheckedQuery & { _type: "summarize" };
export type CheckedOrderBy = CheckedQuery & { _type: "orderBy" };
export type CheckedOpQuery = CheckedWhere | CheckedProject | CheckedExtend | CheckedSummarize | CheckedOrderBy;

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

function evalUnarySafe<T extends Value>(evaluate: (row: InputRow) => Value, then: (x: T) => (T | null)): (row: InputRow) => (T | null) {
  return (row: InputRow) => {
    const x = evaluate(row) as T;
    if (x === null) return null;
    return then(x);
  }
}

function evalBinarySafe<A extends Value, B extends Value, R extends Value>(
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

function evalBinary<A extends Value, B extends Value, R extends Value>(
  evalA: (row: InputRow) => Value,
  evalB: (row: InputRow) => Value,
  then: (a: A | null, b: B | null) => (R | null)
): (row: InputRow) => (R | null) {
  return (row: InputRow) => {
    const a = evalA(row) as A;
    const b = evalB(row) as B;
    return then(a, b);
  }
}

// Here we prefer user defined function over builtins, so user defined one can override the builtin. No real reason why, can be changed later.
function findFunctionDef(ctx: SemCheckContext, funcName: string): FunctionDef | null {
  const userFunc = ctx.env.userFunctions ?
    ctx.env.userFunctions[funcName] : null;
  return userFunc ? userFunc : ctx.env.builtinFunctions[funcName];
}

function semCheckFunctionCall(ctx: SemCheckContext, source: TableDef, expr: AstExpr & { _type: "functionCall" }): SemCheckExprResult<Value> {
  const funcName = expr.functionName.value;
  const funcDef = findFunctionDef(ctx, funcName);
  if (!funcDef) {
    return semFailure(ctx, expr, `No such function as '${funcName}' found`);
  }
  const params = funcDef.parameters;
  if (expr.args.length !== params.length) {
    return semFailure(ctx, expr, `Function '${funcName}' takes ${params.length} arguments, ${expr.args.length} were given`);
  }
  const evals: ((row: InputRow) => Value)[] = [];
  for (const argI in expr.args) {
    const arg = expr.args[argI];
    const argResult = semCheckExpr(ctx, source, arg);
    if (!argResult.success) return argResult;

    const [paramName, paramDataType] = deconstructParam(params[argI]);
    if (argResult.result.dataType !== paramDataType) {
      return semFailure(ctx, arg, `Function '${funcName}' parameter '${paramName}' has type ${paramDataType}, cannot pass argument of type ${argResult.result.dataType}`);
    }
    evals.push(argResult.result.evaluate);
  }

  const evaluate = (row: InputRow) => {
    const args = evals.map(evalArg => evalArg(row));
    return funcDef.func.apply(null, args);
  };
  return semSuccessExpr(funcDef.returnType, evaluate);
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
        const evaluate = evalUnarySafe<number>(evaluateExpr, x => +x);
        return semSuccessExpr("number", evaluate);
      }
      return semFailure(ctx, expr, "Cannot use unary + on " + operandDataType);
    }
    case UnaryOperator.Minus: {
      if (operandDataType !== "number") {
        const evaluate = evalUnarySafe<number>(evaluateExpr, x => -x);
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
  const { dataType: dataTypeB, evaluate: evalB } = b.result;
  switch (expr.op) {
    case BinaryOperator.Minus:
      if (dataTypeA !== "number" || dataTypeB !== "number") {
        return semFailure(ctx, expr, `Cannot subtract '${dataTypeB}' from '${dataTypeA}'`);
      } else {
        const evaluate = evalBinarySafe<number, number, number>(evalA, evalB, (a, b) => a - b);
        return semSuccessExpr("number", evaluate);
      }
    case BinaryOperator.Plus:
      if (dataTypeA === "number") {
        if (dataTypeB === "number") {
          const evaluate = evalBinarySafe<number, number, number>(evalA, evalB, (a, b) => a + b);
          return semSuccessExpr("number", evaluate);
        } else if (dataTypeB === "string") {
          const evaluate = evalBinarySafe<number, string, string>(evalA, evalB, (a, b) => a + b);
          return semSuccessExpr("string", evaluate);
        }
      } else if (dataTypeA === "string") {
        if (dataTypeB === "number") {
          const evaluate = evalBinarySafe<string, number, string>(evalA, evalB, (a, b) => a + b);
          return semSuccessExpr("string", evaluate);
        } else if (dataTypeB === "string") {
          const evaluate = evalBinarySafe<string, string, string>(evalA, evalB, (a, b) => a + b);
          return semSuccessExpr("string", evaluate);
        }
      }
      return semFailure(ctx, expr, `Cannot subtract '${dataTypeB}' from '${dataTypeA}'`);
    case BinaryOperator.Multiply:
      if (dataTypeA !== "number" || dataTypeB !== "number") {
        return semFailure(ctx, expr, `Cannot multiply '${dataTypeA}' and '${dataTypeB}'`);
      } else {
        const evaluate = evalBinarySafe<number, number, number>(evalA, evalB, (a, b) => a * b);
        return semSuccessExpr("number", evaluate);
      }
    case BinaryOperator.Divide:
      if (dataTypeA !== "number" || dataTypeB !== "number") {
        return semFailure(ctx, expr, `Cannot divide '${dataTypeA}' with '${dataTypeB}'`);
      } else {
        const evaluate = evalBinarySafe<number, number, number>(evalA, evalB, (a, b) => a / b);
        return semSuccessExpr("number", evaluate);
      }
    case BinaryOperator.Equal:
    case BinaryOperator.NotEqual:
    case BinaryOperator.Less:
    case BinaryOperator.LessEq:
    case BinaryOperator.Greater:
    case BinaryOperator.GreaterEq:
      if (dataTypeA !== dataTypeB && dataTypeA !== "null" && dataTypeB !== "null") {
        return semFailure(ctx, expr, `Cannot compare '${dataTypeA}' with '${dataTypeB}'`);
      } else {
        switch (expr.op) {
          case BinaryOperator.Equal: {
            const evaluate = evalBinary(evalA, evalB, (a, b) => a === b)
            return semSuccessExpr("boolean", evaluate);
          }
          case BinaryOperator.NotEqual: {
            const evaluate = evalBinary(evalA, evalB, (a, b) => a !== b)
            return semSuccessExpr("boolean", evaluate);
          }
          case BinaryOperator.Less: {
            const evaluate = evalBinary(evalA, evalB, (a, b) => {
              if (a === null && b === null) return false;
              else if (a === null) return true;
              else if (b === null) return false;
              else return a < b;
            });
            return semSuccessExpr("boolean", evaluate);
          }
          case BinaryOperator.LessEq: {
            const evaluate = evalBinary(evalA, evalB, (a, b) => {
              if (a === null && b === null) return true;
              else if (a === null) return true;
              else if (b === null) return false;
              else return a <= b;
            });
            return semSuccessExpr("boolean", evaluate);
          }
          case BinaryOperator.Greater: {
            const evaluate = evalBinary(evalA, evalB, (a, b) => {
              if (a === null && b === null) return false;
              else if (a === null) return false;
              else if (b === null) return true;
              else return a > b;
            });
            return semSuccessExpr("boolean", evaluate);
          }
          case BinaryOperator.GreaterEq: {
            const evaluate = evalBinary(evalA, evalB, (a, b) => {
              if (a === null && b === null) return true;
              else if (a === null) return false;
              else if (b === null) return true;
              else return a >= b;
            });
            return semSuccessExpr("boolean", evaluate);
          }
        }
        throw Error("Unreachable");
      }
    case BinaryOperator.Contains:
      if (dataTypeA !== "string" || dataTypeB !== "string") {
        return semFailure(ctx, expr, `Cannot use contains with '${dataTypeA}' and '${dataTypeB}'`);
      } else {
        const evaluate = evalBinarySafe<string, string, boolean>(evalA, evalB, (a, b) => a.includes(b));
        return semSuccessExpr("boolean", evaluate);
      }
    case BinaryOperator.NotContains:
      if (dataTypeA !== "string" || dataTypeB !== "string") {
        return semFailure(ctx, expr, `Cannot use !contains with '${dataTypeA}' and '${dataTypeB}'`);
      } else {
        const evaluate = evalBinarySafe<string, string, boolean>(evalA, evalB, (a, b) => !a.includes(b));
        return semSuccessExpr("boolean", evaluate);
      }
    case BinaryOperator.And:
      if (dataTypeA !== "boolean" || dataTypeB !== "boolean") {
        if (dataTypeA === "null" || dataTypeB === null) {
          return semSuccessExpr("boolean", () => false);
        }
        return semFailure(ctx, expr, `And expects boolean operands, got '${dataTypeA}' and '${dataTypeB}'`);
      } else {
        const evaluate = evalBinarySafe<boolean, boolean, boolean>(evalA, evalB, (a, b) => a && b);
        return semSuccessExpr("boolean", evaluate);
      }
    case BinaryOperator.Or:
      if (dataTypeA !== "boolean" || dataTypeB !== "boolean") {
        if (dataTypeA === "null" && dataTypeB === null) {
          return semSuccessExpr("boolean", () => false);
        } else if (dataTypeA === "boolean" && dataTypeB === "null") {
          return semSuccessExpr("boolean", evalA);
        } else if (dataTypeA === "null" && dataTypeB === "boolean") {
          return semSuccessExpr("boolean", evalB);
        }
        return semFailure(ctx, expr, `And expects boolean operands, got '${dataTypeA}' and '${dataTypeB}'`);
      } else {
        const evaluate = evalBinarySafe<boolean, boolean, boolean>(evalA, evalB, (a, b) => a || b);
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
      return semSuccessExpr("null", () => null);
    }
    case "trueLit": {
      return semSuccessExpr("boolean", () => true);
    }
    case "falseLit": {
      return semSuccessExpr("boolean", () => false);
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
    case "functionCall":
      return semCheckFunctionCall(ctx, source, expr);
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

function semCheckAggregation(ctx: SemCheckContext, source: TableDef, aggr: Aggregation): SemCheckResult<CheckedAggr<Value>> {
  const funcName = aggr.expr.functionName.value;
  const funcDef = findFunctionDef(ctx, funcName);
  if (!funcDef) {
    return semFailure(ctx, aggr.expr, `No such function as '${funcName}' found`);
  }
  if (funcDef.initialValue === undefined) {
    return semFailure(ctx, aggr.expr, `Function '${funcName}' cannot be used as aggregation function, as it does not have initial value defined`);
  }
  const params = funcDef.parameters;
  if (funcDef.parameters.length === 0) {
    return semFailure(ctx, aggr.expr, `Aggregation function must have at least the accumulator parameter, '${funcName}' has none`)
  }
  const [accumName, accumType] = deconstructParam(funcDef.parameters[0]);
  if (funcDef.returnType !== accumType) {
    return semFailure(
      ctx,
      aggr.expr,
      `Invalid aggregation function '${funcName}'. Accumulator '${accumName}' type is '${accumType}' and return type is '${funcDef.returnType}'`
    );
  }

  // func(b, c)  is actually  func(acc, b, c)
  if (aggr.expr.args.length + 1 !== params.length) {
    return semFailure(ctx, aggr.expr, `Aggregation function '${funcName}' takes ${params.length - 1} arguments, ${aggr.expr.args.length} were given`);
  }

  const evals: ((row: InputRow) => Value)[] = [];
  for (let argI = 0; argI < aggr.expr.args.length; argI++) {
    const arg = aggr.expr.args[argI];
    const argResult = semCheckExpr(ctx, source, arg);
    if (!argResult.success) return argResult;

    const [paramName, paramDataType] = deconstructParam(params[argI + 1]);
    if (argResult.result.dataType !== paramDataType) {
      return semFailure(ctx, arg, `Function '${funcName}' parameter '${paramName}' has type ${paramDataType}, cannot pass argument of type ${argResult.result.dataType}`);
    }
    evals.push(argResult.result.evaluate);
  }

  const aggregate = (accum: Value, row: InputRow) => {
    const args = [accum].concat(evals.map(evalArg => evalArg(row)));
    return funcDef.func.apply(null, args);
  };
  const checkedAggr: CheckedAggr<Value> = {
    name: aggr.name.value,
    aggregate,
    initialValue: funcDef.initialValue,
    finalPass: funcDef.finalPass,
    dataType: funcDef.returnType
  };
  return semSuccess(checkedAggr);
}

function semCheckSummarize(ctx: SemCheckContext, source: TableDef, op: AstSummarize): SemCheckResult<CheckedSummarize> {
  const aggregationResults: [CheckedAggr<Value> | null, SemCheckFailure | null][] = op.aggregations.map(aggr => {
    // Check if aggr.name is not reserved or invalid - is used in group by? is a function name?

    const aggrResult = semCheckAggregation(ctx, source, aggr);
    if (!aggrResult.success) return [null, aggrResult];

    return [aggrResult.result, null];
  });

  const aggrErrors: SemCheckFailure[] = aggregationResults.map(([, error]) => error).filter(v => v !== null) as SemCheckFailure[];
  if (aggrErrors.length > 0) return aggrErrors[0];

  const aggregations: CheckedAggr<Value>[] = aggregationResults.map(([aggr]) => aggr).filter(v => v !== null) as CheckedAggr<Value>[];

  const groupBy = op.groupBy.map(column => column.value);
  const columnNotFound = groupBy.reduce(
    (error, columnName) => source.columns.find(([name]) => name === columnName) ? error : columnName,
    null as string | null
  );
  if (columnNotFound) {
    return semFailure(ctx, op, `No such column as '${columnNotFound}' found`);
  }

  const groupByColumns: [string, DataType][] = source.columns.filter(([name]) => groupBy.includes(name));
  const aggrColumns: [string, DataType][] = aggregations.map(aggr => [aggr.name, aggr.dataType]);

  const tableDef: TableDef = {
    columns: aggrColumns.concat(groupByColumns)
  };

  const summarize: CheckedSummarize = { _type: "summarize", tableDef, aggregations, groupBy };
  return semSuccessQuery(summarize);
}

function semCheckOrderBy(ctx: SemCheckContext, source: TableDef, op: AstOrderBy): SemCheckResult<CheckedOrderBy> {
  const { names: nameTokens, order } = op;
  const names = nameTokens.map(n => n.value);

  // find first name that is not found in the table def columns
  const notFoundColumn = names.find(name => source.columns.find(([columnName]) => columnName === name) === undefined);
  if (notFoundColumn) {
    return semFailure(ctx, op, "No such column as '" + notFoundColumn + "' found");
  }

  const orderBy: CheckedOrderBy = { _type: "orderBy", names, order, tableDef: source };
  return semSuccessQuery(orderBy);
}

function semCheckTopLevelOp(ctx: SemCheckContext, source: TableDef, op: Ast): SemCheckResult<CheckedOpQuery> {
  switch (op._type) {
    case "project":
      return semCheckProject(ctx, source, op);
    case "extend":
      return semCheckExtend(ctx, source, op);
    case "where":
      return semCheckWhere(ctx, source, op);
    case "summarize":
      return semCheckSummarize(ctx, source, op);
    case "orderBy":
      return semCheckOrderBy(ctx, source, op);
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


