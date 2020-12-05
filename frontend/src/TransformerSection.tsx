import React, {useCallback} from 'react';
import { Dispatch, useMemo } from 'react';
import styled from 'styled-components';

import * as rxjs from 'rxjs';
import * as rxop from 'rxjs/operators';

import { Action, State, clearTxResults, addTxResults } from './reducer';

import useSessionStorage from './UseSessionStorageHook';
import {ColumnValueDataType, ResultRow} from './types';

import * as rql from './rql/rql';

const example = "Rows | where persistence_id container 'DHN'";
const parsed = rql.parse(example);

console.log(parsed);

function transform(row: any): any | undefined {
  if (row.event && typeof row.event === "string") {
    try {
      const json = JSON.parse(atob(row.event))
      return json.shipment;
      //return json;
    } catch {
      return;
    }
  }
  return;
}

function createTransformStream(rows$: rxjs.Observable<any>) {
  return rows$
    .pipe(
      rxop.flatMap(row => {
        const res = transform(row);
        if (res === undefined) {
          return rxjs.EMPTY;
        } else {
          return rxjs.of(res);
        }
      }),
      rxop.bufferTime(50)
    );
}

//async function executeTransform(rowIterator: () => any, produce: (result: any) => void) {
//  while (true) {
//    const item = rowIterator();
//    if (item === null) break;
//
//    const result = transform(item);
//    produce(result);
//  }
//}

function doTransform(state: State, dispatch: Dispatch<Action>) {
  dispatch(clearTxResults);

  function convertRow(row: ResultRow): any {
    const result: { [index: string]: any } = { };
    state.columnDefinitions.forEach((columnDef, i) => {
      //console.log("Adding column", columnDef, "to", result, ";", row);
      const columnValue = row.columnValues[i];
      if (columnValue.Null !== undefined) {
        result[columnDef.name] = null;
      } else {
        result[columnDef.name] = columnValue[columnDef.dataType].v;
      }
    });
    return result;
  }
/*
  const pages = state.results;
  const rowObservable = new rxjs.Observable(subscriber => {
    for (const pi in pages) {
      const page = pages[pi];
      for (const ri in page.rows) {
        const row = page.rows[ri];
        const rowObject = convertRow(row);
        subscriber.next(rowObject);
      }
    }
  });
  createTransformStream(rowObservable)
    .subscribe(results => {
      console.log("Tx results", results);
      dispatch(addTxResults(results));
  });*/
  const pages = state.results;
  for (const pi in pages) {
    const page = pages[pi];
    for (const ri in page.rows) {
      const row = page.rows[ri];
      const rowObject = convertRow(row);
      const result = transform(rowObject);
      if (result !== undefined) {
        dispatch(addTxResults([result]));
      }
    }
  }
}

function rowsObservable(state: State): rql.RowsObs {
  function convertRow(row: ResultRow): any {
    const result: { [index: string]: any } = { };
    state.columnDefinitions.forEach((columnDef, i) => {
      //console.log("Adding column", columnDef, "to", result, ";", row);
      const columnValue = row.columnValues[i];
      if (columnValue.Null !== undefined) {
        result[columnDef.name] = null;
      } else {
        result[columnDef.name] = columnValue[columnDef.dataType].v;
      }
    });
    return result;
  }

  const pages = state.results;
  return new rxjs.Observable(subscriber => {
    for (const pi in pages) {
      const page = pages[pi];
      for (const ri in page.rows) {
        const row = page.rows[ri];
        const rowObject = convertRow(row);
        subscriber.next(rowObject);
      }
    }
  });
}

interface TransformerSectionProps {
  forwardRef: { current: HTMLDivElement | null };
  state: State;
  dispatch: Dispatch<Action>;
}

export function TransformerSection(props: TransformerSectionProps) {
  const { forwardRef, state, dispatch } = props;
  const [script, setScript] = useSessionStorage("transform.script", "// Type script here");
  const parsed = useMemo(() => JSON.stringify(rql.parse(script), null, 2), [script]);
  const [compileError, compileResult] = useMemo(() => {
    const ast = rql.parse(script);
    if (typeof ast === "string") return [ast, undefined];
    const result = rql.compile(ast);
    return [undefined, result];
  },
    [script, state]
  );

  const execute = useCallback(() => {
    if (compileResult === undefined) return;
    const convertDataType = (dt: ColumnValueDataType): rql.DataType => {
      switch (dt) {
        case ColumnValueDataType.Bool: return "boolean";
        case ColumnValueDataType.Integer: return "number";
        case ColumnValueDataType.SmallInt: return "number";
        case ColumnValueDataType.TinyInt: return "number";
        default: return "string";
      }
    };
    const rows = rowsObservable(state);
    const env: rql.Env = {
      tables: {
        Rows: {
          columns: state.columnDefinitions.map(cd => {
            return [cd.name, convertDataType(cd.dataType)];
          }),
          rows
        }
      }
    };
    console.log("Env", env);
    const result = compileResult(env);
    console.log("Compilation result:", result);
    if (result.success) {
      result.result.rows
        .subscribe(results => {
          console.log("Tx results", results);
          dispatch(addTxResults([results]));
        });
    }
  }, [compileResult, state]);

  const pageStart = state.tx.rowsPerPage * state.tx.page;
  const resultsOnPage = state.tx.results.slice(pageStart, pageStart + state.tx.rowsPerPage);
  return (
    <Container ref={forwardRef}>
      <ScriptContainer>
        <ScriptInput rows={10} cols={120} value={script} onChange={ev => setScript(ev.target.value)} />
        {/*<ParsedContainer>{parsed}</ParsedContainer>*/}
      </ScriptContainer>
      <button disabled={compileError !== undefined} onClick={() => execute()}>Execute</button>
      {compileError && <span>{compileError}</span>}
      {resultsOnPage.map((item, i) => {
        return <div>{i} - {JSON.stringify(item)}</div>;
      })}
    </Container>
  );
}

const Container = styled.div`
  overflow: scroll;
`;

const ScriptContainer = styled.div`
  display: flex;
  flex-direction: row;
`;

const ScriptInput = styled.textarea`
  padding: 8px;
  font-size: 10pt;
  font-family: "Verdana";
`;

const ParsedContainer = styled.pre`
  padding: 10px;
`;

