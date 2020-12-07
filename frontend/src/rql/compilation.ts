import * as rxjs from 'rxjs';
import * as rxop from 'rxjs/operators';

import { InputRow, TableDef, FunctionDef } from './common';
import { CheckedQuery, CheckedCont, CheckedOpQuery, CheckedTable, CheckedExtend, CheckedProject, CheckedWhere } from './semcheck';

export type RowsObs = rxjs.Observable<InputRow>;

export type TableSource = {
  tableDef: TableDef;
  rows: RowsObs;
}
export type CompilationEnv = {
  tables: {
    [name: string]: TableSource;
  },
  userFunctions?: {
    [name: string]: FunctionDef;
  }
}

type Env = CompilationEnv;

export type CompilationResult = (env: Env) => RowsObs;


function compileTable(q: CheckedTable): CompilationResult {
  const tableName = q.name;
  return (env: Env) => {
    const tableSource = env.tables[tableName];
    if (tableSource === undefined) throw Error("Unreachable");
    return tableSource.rows;
  };
}

function compileProject(source: RowsObs, q: CheckedProject): CompilationResult {
  return (env: Env) => {
    return source.pipe(
      rxop.map(row => {
        return q.names.reduce((acc, column) => {
          acc[column] = row[column];
          return acc;
        }, {} as InputRow);
      })
    );
  };
}

function compileExtend(source: RowsObs, q: CheckedExtend): CompilationResult {
  const name = q.name;
  return (env: Env) => {
    return source.pipe(
      rxop.map(row => {
        const value = q.expr.evaluate(row);
        const mapped = { ...row, [name]: value };
        return mapped;
      })
    );
  };
}

function compileWhere(source: RowsObs, q: CheckedWhere): CompilationResult {
  return (env: Env) => {
    return source.pipe(
      rxop.filter(row => {
        const value = q.expr.evaluate(row);
        return value === true;
      })
    );
  };
}

function compileTopLevelOp(source: RowsObs, q: CheckedOpQuery): CompilationResult {
  switch (q._type) {
    case "project":
      return compileProject(source, q);
    case "extend":
      return compileExtend(source, q);
    case "where":
      return compileWhere(source, q);
  }
}

function compileCont(q: CheckedCont): CompilationResult {
  const sourceFn = compileTopLevel(q.source);
  return (env: Env) => {
    const source = sourceFn(env);
    const opFn = compileTopLevelOp(source, q.op);
    return opFn(env);
  };
}

function compileTopLevel(q: CheckedQuery): CompilationResult {
  switch (q._type) {
    case "table":
      return compileTable(q);
    case "cont":
      return compileCont(q);
  }
  throw Error("Unreachable");
}

export function compileChecked(q: CheckedQuery): CompilationResult {
  return compileTopLevel(q);
}

