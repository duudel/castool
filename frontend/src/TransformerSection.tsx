import React, {useCallback, useEffect, useState} from 'react';
import { Dispatch, useMemo } from 'react';
import styled from 'styled-components';

import * as rxjs from 'rxjs';
import * as rxop from 'rxjs/operators';

import { Action, State, clearTxResults, addTxResults, setTxSchema } from './reducer';

import useSessionStorage from './UseSessionStorageHook';
import {ColumnValueDataType, ResultRow} from './types';

import * as rql from './rql/rql';

function rowsObservable(state: State): rql.RowsObs {
  function convertRow(row: ResultRow): any {
    const result: { [index: string]: any } = { };
    state.columnDefinitions.forEach((columnDef, i) => {
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
    subscriber.complete();
  });
}

interface ResultsContainerProps {
  state: Pick<State, "tx">;
}

function ResultsContainer(props: ResultsContainerProps) {
  const { state: { tx: { schema, results, page, rowsPerPage } } } = props;
  const pageStart = rowsPerPage * page;
  const resultsOnPage = results.slice(pageStart, pageStart + rowsPerPage);
  return (
    <ResultsTable>
      <thead>
        <Row>
          <RowNumber />
          {schema.columns.map(([name, dataType], index) =>
            <HeadCell key={"h-" + name}>{name}: {dataType}</HeadCell>
          )}
        </Row>
      </thead>
      {resultsOnPage.map((item, i) => {
        return (
          <Row key={"row-" + (pageStart + i)}>
            <RowNumber>{pageStart + i}</RowNumber>
            {schema.columns.map(([columnName]) => {
              const value = item[columnName];
              return <Cell key={columnName}>
                {value === null ? "null" : value.toString()}
              </Cell>;
            })}
          </Row>
        );
      })}
    </ResultsTable>
  );
}

interface TransformerSectionProps {
  forwardRef: { current: HTMLDivElement | null };
  state: State;
  dispatch: Dispatch<Action>;
}

export function TransformerSection(props: TransformerSectionProps) {
  const { forwardRef, state, dispatch } = props;
  const [script, setScript] = useSessionStorage("transform.script", "// Type script here");
  //const parsed = useMemo(() => JSON.stringify(rql.parse(script), null, 2), [script]);
  const [env, setEnv] = useState<rql.CompilationEnv | null>(null);
  const [compileResult, setCompileResult] = useState<rql.CompileResult | null>(null);
  useEffect(() => {
    if (env === null) {
      setCompileResult(null);
      return;
    }
    console.log("Recompiling...");
    const result = rql.compile(script, env);
    setCompileResult(result);
  }, [script, env]);

  const canExecute = compileResult && compileResult.program !== null;
  useEffect(() => {
    if (compileResult && compileResult.checked !== null) {
      const tableDef = compileResult.checked.tableDef;
      if (JSON.stringify(state.tx.schema.columns) !== JSON.stringify(tableDef.columns))
        dispatch(setTxSchema(tableDef));
    }
  }, [compileResult, dispatch]);

  useEffect(() => {
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
    const columns: [string, rql.DataType][] = state.columnDefinitions.map(cd => [cd.name, convertDataType(cd.dataType)]);
    const tableDef = { columns };
    const env: rql.CompilationEnv = {
      tables: {
        Rows: { tableDef, rows }
      }
    };

    setEnv(env);
  }, [state, state.columnDefinitions, state.results]);

  const execute = useCallback(() => {
    if (!env) return null;
    if (compileResult === null || compileResult.program === null) return;

    dispatch(clearTxResults);
    const result = compileResult.program(env);
    result
      .pipe(
        rxop.bufferTime(100)
      )
      .subscribe(results => {
        //console.log("Tx results", results);
        if (results.length !== 0) dispatch(addTxResults(results));
      });
  }, [compileResult, env, dispatch]);

  const formatError = (res: rql.CompileResult | null) => {
    if (!res || !res.error) return null;

    const { error, message, sourcePos } = res.error;
    return error + " at " + sourcePos[0] + ":" + sourcePos[1]  + ": " + message;
  };
  const error = formatError(compileResult);

  return (
    <Container ref={forwardRef}>
      <ScriptContainer>
        <ScriptInput value={script} onChange={ev => setScript(ev.target.value)} />
        {/*<ParsedContainer>{parsed}</ParsedContainer>*/}
        <Button disabled={!canExecute} onClick={() => execute()}>Execute</Button>
        {/*compileResult && compileResult.error && <span>{JSON.stringify(compileResult.error)}</span>*/}
        {error && <span>{error}</span>}
        {state.tx.results.length} results
      </ScriptContainer>
      <ResultsContainer state={state} />
    </Container>
  );
}

const Container = styled.div`
  overflow: scroll;
`;

const ScriptContainer = styled.div`
  position: sticky;
  top: 0;
  left: 0;
  display: flex;
  flex-direction: column;

  background: white;
`;

const ScriptInput = styled.textarea`
  padding: 8px;
  width: 100%;
  height: 160px;
  font-size: 10pt;
  font-family: "Verdana";
`;

const Button = styled.button<{ disabled?: boolean }>`
  padding: 10px;
  font-size: 12pt;
  color: ${({ disabled }) => (disabled ? "#888" : "black")};
  background: #bbb;
  border: 2.5px solid #989;
  border-radius: 2px;
`;

const ParsedContainer = styled.pre`
  padding: 10px;
`;

const ResultsTable = styled.table`
  overflow: scroll;
  border-collapse: collapse;
  border-spacing: 0px;
`;

const Row = styled.tr``;

const RowNumber = styled.td`
  padding: 5px;
  border: 0.5px solid #ccc;
  background-color: #eee;
`;

const Cell = styled.td`
  margin: 0;
  padding: 5px;
  border: 0.5px solid #ccc;
  vertical-align: top;
`;

const HeadCell = styled.th`
  position: sticky;
  top: 0;
  margin: 0;
  padding: 5px;
  border: 0.5px solid #ccc;
  background-color: #eee;
`;

const ColumnName = styled.span`
  font-weight: bold;
  color: #000;
`;

const ColumnDataType = styled.span`
  font-weight: normal;
  color: #888;
`;

