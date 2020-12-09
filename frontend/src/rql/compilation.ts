import * as rxjs from 'rxjs';
import * as rxop from 'rxjs/operators';

import { Value, InputRow, TableDef, FunctionDef, CompilationEnv, RowsObs } from './common';
import { CheckedQuery, CheckedCont, CheckedOpQuery, CheckedTable, CheckedExtend, CheckedProject, CheckedWhere, CheckedSummarize, CheckedAggr } from './semcheck';

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

function compileSummarize(source: RowsObs, q: CheckedSummarize): CompilationResult {
  const aggregations$ = rxjs.from(q.aggregations);

  const group = rxop.groupBy((row: InputRow) => {
    const key = q.groupBy.reduce((acc, column) => {
      acc = acc + "_" + row[column];
      return acc;
    }, "");
    console.log("Key", key);
    return key;
  });

  const aggregate = (aggr: CheckedAggr<Value>) => {
    function aggregateValuesAndN([acc, N]: [Value, number], row: InputRow): [Value, number] {
      console.log("Aggr ", aggr.name, acc, N);
      return [aggr.aggregate(acc, row), N + 1];
    }
    return rxop.reduce(aggregateValuesAndN, [aggr.initialValue, 0]);
  };

  const mapToAggregateColumn = (aggr: CheckedAggr<Value>) => {
    return rxop.map(([value, N]: [Value, number]) => {
      if (aggr.finalPass) {
        return { name: aggr.name, value: aggr.finalPass(value, N) };
      } else {
        return { name: aggr.name, value };
      }
    })
  };

  const logOp = <T>(tag: string) => rxop.map((x: T) => { console.log(tag, ":", x); return x; });

  const withGroupBy = (): RowsObs => source.pipe(
    //logOp("INPUT"),
    group,
    rxop.map(grouped => {
      const aggregates = aggregations$.pipe(
        rxop.mergeMap((aggr) => grouped.pipe(
          aggregate(aggr), mapToAggregateColumn(aggr),
        )),
        rxop.reduce((acc, { name, value }) => {
          acc[name] = value;
          return acc;
        }, {} as InputRow),
      );
      return rxjs.zip(
        grouped,
        aggregates
      ).pipe(
        rxop.map(([row, aggregateColumns]) => {
          return { ...row, ...aggregateColumns };
        })
      );
    }),
    rxop.mergeAll() 
  );

  const noGrouping = (): RowsObs => aggregations$.pipe(
    rxop.mergeMap((aggr) => {
      return source.pipe(
        aggregate(aggr),
        mapToAggregateColumn(aggr),
      );
    }),
    rxop.reduce((acc, { name, value }) => {
      acc[name] = value;
      return acc;
    }, {} as InputRow)
  );

  if (q.groupBy.length === 0) {
    const result = noGrouping();
    return (env: Env) => {
      return result;
    }
  } else {
    const result = withGroupBy();
    return (env: Env) => {
      return result;
    }
  }
}

function compileTopLevelOp(source: RowsObs, q: CheckedOpQuery): CompilationResult {
  switch (q._type) {
    case "project":
      return compileProject(source, q);
    case "extend":
      return compileExtend(source, q);
    case "where":
      return compileWhere(source, q);
    case "summarize":
      return compileSummarize(source, q);
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

