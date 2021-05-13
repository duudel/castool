import React, {useCallback, useEffect, useRef, useState} from 'react';
import { Dispatch, useMemo } from 'react';
import styled from 'styled-components';

import Split from './Split';

import * as rxjs from 'rxjs';
import * as rxop from 'rxjs/operators';

import { Action, State, clearTxResults, addTxResults, setTxSchema } from './reducer';

import useSessionStorage from './utils/UseSessionStorageHook';
import { ColumnValueDataTypeCode, ResultRow } from './types';
import type { ColumnValueDataType } from './types';

import * as rql from './rql/rql';
import { JsonSyntaxHighlight } from './json-syntax/JsonSyntaxHighlight';

function rowsObservable(state: State): rql.RowsObs {
  function convertRow(row: ResultRow): any {
    const result: { [index: string]: any } = { };
    state.columnDefinitions.forEach((columnDef, i) => {
      const columnValue = row.columnValues[i];
      if (columnValue.Null !== undefined) {
        result[columnDef.name] = null;
      } else {
        result[columnDef.name] = columnValue[columnDef.dataType.code].v;
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
  return (schema.columns.length > 0) ? (
  <ResultsTableContainer>
    <ResultsTable>
      <thead>
        <Row>
          <RowNumber />
          {schema.columns.map(([name, dataType], index) =>
            <HeadCell key={"h-" + name}>{name}: {dataType}</HeadCell>
          )}
        </Row>
      </thead>
      <tbody>
        {resultsOnPage.map((item, i) => {
          return (
            <Row key={"row-" + (pageStart + i)}>
              <RowNumber>{pageStart + i}</RowNumber>
              {schema.columns.map(([columnName, columnDataType]) => {
                const value = item[columnName];
                if (value === undefined) {
                  //console.log("Undefined: ", columnName, item);
                  return <Cell key={columnName}>Undefined</Cell>;
                }
                switch (columnDataType) {
                  case "object":
                    return (
                      <Cell key={columnName}>
                        <JsonSyntaxHighlight value={JSON.stringify(value, null, 2)}/>
                      </Cell>
                    );
                }
                return <Cell key={columnName}>
                  {value === null ? "null" : value.toString()}
                </Cell>;
              })}
            </Row>
          );
        })}
      </tbody>
    </ResultsTable>
  </ResultsTableContainer>
  ) : null;
}

const userFunctions: rql.UserFunctions = {
  decode_base64: {
    parameters: [{input: "string"}],
    returnType: "string",
    func: (input: string): string => atob(input),
  },
  parse_json: {
    parameters: [{s: "string"}],
    returnType: "object",
    func: (s: string) => {
      try { return JSON.parse(s); } catch { return null; }
    },
  }
};

interface TransformerSectionProps {
  state: State;
  dispatch: Dispatch<Action>;
}

export function TransformerSection(props: TransformerSectionProps) {
  const { state, dispatch } = props;
  const scriptRef = useRef<HTMLDivElement | null>(null);
  const resultsRef = useRef<HTMLDivElement | null>(null);
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
      // HACK: without this condition the schema would be reset infinitely
      if (JSON.stringify(state.tx.schema.columns) !== JSON.stringify(tableDef.columns))
        dispatch(setTxSchema(tableDef));
    }
  }, [compileResult, dispatch]);

  useEffect(() => {
    const convertDataType = (dt: ColumnValueDataType): rql.DataType => {
      switch (dt.code) {
        case ColumnValueDataTypeCode.Bool: return "boolean";
        case ColumnValueDataTypeCode.Integer: return "number";
        case ColumnValueDataTypeCode.SmallInt: return "number";
        case ColumnValueDataTypeCode.TinyInt: return "number";
        case ColumnValueDataTypeCode.Date: return "date";
        case ColumnValueDataTypeCode.Timestamp: return "date";
        default: return "string";
      }
    };
    const convertValue = (dt: ColumnValueDataType, v: any): rql.Value => {
      switch (dt.code) {
        //case ColumnValueDataType.Bool: return "boolean";
        //case ColumnValueDataType.Integer: return "number";
        //case ColumnValueDataType.SmallInt: return "number";
        //case ColumnValueDataType.TinyInt: return "number";
        case ColumnValueDataTypeCode.Date: return new Date(v);
        case ColumnValueDataTypeCode.Timestamp: return new Date(v);
        default: return v;
      }
    };
    const rows = rowsObservable(state).pipe(
      rxop.map(row => {
        state.columnDefinitions.forEach(cd => row[cd.name] = convertValue(cd.dataType, row[cd.name]))
        return row;
      })
    );
    const columns: [string, rql.DataType][] = state.columnDefinitions.map(cd => [cd.name, convertDataType(cd.dataType)]);
    const tableDef = { columns };
    const env: rql.CompilationEnv = {
      tables: {
        Rows: { tableDef, rows }
      },
      userFunctions
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
    <Container>
      <ScriptContainer ref={scriptRef}>
        <div style={{display: "flex", flexDirection: "row"}}>
          <ScriptInput value={script} onChange={ev => setScript(ev.target.value)} />
          <pre style={{overflow: "scroll", height: "200px"}}>{compileResult && compileResult.ast && JSON.stringify(compileResult.ast, null, 2)}</pre>
        </div>
        <ScriptExecutionControls>
          <Button disabled={!canExecute} onClick={() => execute()}>Execute</Button>
          <ResultsNum>{state.tx.results.length} results</ResultsNum>
        </ScriptExecutionControls>
        {error && <span>{error}</span>}
      </ScriptContainer>
      <ResultsContainer state={state} />
    </Container>
  );
}

const Container = styled.div`
  overflow: hidden;
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
  width: 80%;
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

const ScriptExecutionControls = styled.div`
  display: flex;
  flex-direction: row;
`;

const ResultsNum = styled.div`
  padding: 10px;
`;

const ResultsTableContainer = styled.div`
  position: relative;
  overflow: scroll;
  white-space: nowrap;
  max-height: 600px;
  background: white;
`;

const ResultsTable = styled.table`
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

