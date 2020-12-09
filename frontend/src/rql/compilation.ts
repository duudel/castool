import * as rxjs from 'rxjs';
import * as rxop from 'rxjs/operators';

import { Value, InputRow, TableDef, FunctionDef, CompilationEnv, RowsObs } from './common';
import { CheckedQuery, CheckedCont, CheckedOpQuery, CheckedTable, CheckedExtend, CheckedProject, CheckedWhere, CheckedSummarize, CheckedAggr, CheckedOrderBy } from './semcheck';

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
    //console.log("Key", key);
    return key;
  });

  const aggregate = (aggr: CheckedAggr<Value>) => {
    function aggregateValuesAndN([acc, N]: [Value, number], row: InputRow): [Value, number] {
      //console.log("Aggr ", aggr.name, acc, N);
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

  //const logOp = <T>(tag: string) => rxop.map((x: T) => { console.log(tag, ":", x); return x; });

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

function compileOrderBy(source: RowsObs, q: CheckedOrderBy): CompilationResult {
  // TODO: have datatypes choose the final compare per column
  const compare = (a: InputRow, b: InputRow): number => {
    const comps = q.names.map(name => {
      const av = a[name];
      const bv = b[name];
      if (av === null && bv === null) {
        return 0;
      } else if (av === null) {
        return -1;
      } else if (bv === null) {
        return 1;
      }
      if (typeof av !== typeof bv) throw Error("Values should have same type");
      switch (typeof av) {
        case "string":
          return av.localeCompare(bv as string);
        case "number":
          return av - (bv as number);
        case "object":
          // HACK: should come up with a better and faster way to compare objects, or just not allow comparison.
          return JSON.stringify(av).localeCompare(JSON.stringify(bv));
        case "boolean":
          return av < bv ? -1 : (av > bv ? 1 : 0);
      }
      throw Error("Unreachable");
    });
    const res = comps.find(v => v !== 0);
    return res === undefined ? 0 : res;
  };
  const compareDesc = (a: InputRow, b: InputRow): number => -compare(a, b);
  if (q.order === "asc") {
    return (env: Env) => {
      return source.pipe(
        rxop.toArray(),
        rxop.tap(rows => rows.sort(compare)),
        rxop.concatMap(rows => rxjs.from(rows))
      )
    };
  } else {
    return (env: Env) => {
      return source.pipe(
        rxop.toArray(),
        rxop.tap(rows => rows.sort(compareDesc)),
        rxop.concatMap(rows => rxjs.from(rows))
      )
    };
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
    case "orderBy":
      return compileOrderBy(source, q);
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

