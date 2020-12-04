import React from 'react';
import { Dispatch, useMemo } from 'react';
import styled from 'styled-components';

import * as rxjs from 'rxjs';
import * as rxop from 'rxjs/operators';

import { Action, State, clearTxResults, addTxResults } from './reducer';

import useSessionStorage from './UseSessionStorageHook';
import {ResultRow} from './types';

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

interface TransformerSectionProps {
  forwardRef: { current: HTMLDivElement | null };
  state: State;
  dispatch: Dispatch<Action>;
}

export function TransformerSection(props: TransformerSectionProps) {
  const { forwardRef, state, dispatch } = props;
  const [script, setScript] = useSessionStorage("transform.script", "// Type script here");
  const parsed = useMemo(() => JSON.stringify(rql.parse(script), null, 2), [script]);
  return (
    <Container ref={forwardRef}>
      <ScriptContainer>
        <ScriptInput rows={20} cols={120} value={script} onChange={ev => setScript(ev.target.value)} />
        <ParsedContainer>{parsed}</ParsedContainer>
      </ScriptContainer>
      <button onClick={() => doTransform(state, dispatch)}> Transform </button>
      {state.tx.pages.length > 0 && state.tx.pages[0].results.map((item, i) => {
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

