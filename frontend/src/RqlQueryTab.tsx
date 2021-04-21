import React, { useCallback, useEffect, useReducer, useState } from "react";
import styled from 'styled-components';

import { useWebsocket } from "./utils/UseWebsocketHook";
import useSessionStorage from "./utils/UseSessionStorageHook";

interface ResultRow {
  num: number;
  columns: RqlValue[];
}

interface QueryResult {
  columns: [string, RqlValueType][];
  rows: ResultRow[];
}

interface State {
  queryResult: QueryResult | null;
  queryError: string | null;
}

const initialState: State = {
  queryResult: null,
  queryError: null,
};

enum ActionType {
  ON_MESSAGE,
}

interface OnMessage {
  type: ActionType;
  message: any;
}

type Action = OnMessage;

enum RqlValueType {
  Null = "Null",
  Bool = "Bool",
  Num = "Num",
  Str = "Str",
  Obj = "Obj",
}

//interface RqlNull {
//  Null: { }
//}
//interface RqlBool {
//  Bool: { v: boolean; }
//}
//interface RqlNum {
//  Num: { v: number; }
//}
//interface RqlStr {
//  Str: { v: string }
//}
//interface RqlObj {
//  Obj: {
//    v: {
//      [index: string]: RqlValue
//    }
//  }
//}
type RqlNull = null;
type RqlBool = boolean;
type RqlNum = number;
type RqlStr = string;
type RqlObj = object;

type RqlValue = RqlNull | RqlBool | RqlNum | RqlStr | RqlObj;


interface RqlMessageSuccess {
  _type: "Success";
  columns: [string, RqlValueType][];
}
interface RqlMessageError {
  _type: "Error";
  error: string;
}
interface RqlMessageRows {
  _type: "Rows";
  rows: ResultRow[];
}
interface RqlMessageFinished {
  _type: "Finished";
}
type RqlMessage = RqlMessageSuccess | RqlMessageError | RqlMessageRows | RqlMessageFinished;

const reducer = (state: State, action: Action): State => {
  switch (action.type) {
    case ActionType.ON_MESSAGE: {
      const message = JSON.parse(action.message) as RqlMessage;
      console.log("Message", message);
      switch (message._type) {
        case "Success":
          return { ...state, queryResult: { columns: message.columns, rows: []}, queryError: null };
        case "Error":
          return { ...state, queryResult: null, queryError: message.error };
        case "Rows":
          const { columns = [], rows = [] } = state.queryResult || {};
          return { ...state, queryResult: { columns, rows: rows.concat(message.rows) } };
      }
      return state;
    }
  }
};

type ResultTableProps = {
  columns: [string, RqlValueType][];
  rows: ResultRow[];
};

function valueToString(value: RqlValue): string {
  if (value === null) return "null";
  else if (typeof value === "boolean") return value ? "true" : "false";
  else if (typeof value === "number") return value.toString();
  else if (typeof value === "string") return value;
  else return "{...}";
}

function renderValue(value: RqlValue) {
  if (value === null) return <NullValue>NULL</NullValue>;
  else if (typeof value === "boolean") return <BooleanValue>{value ? "true" : "false"}</BooleanValue>;
  else if (typeof value === "number") return <NumberValue>{value}</NumberValue>;
  else if (typeof value === "string") return <TextValue>{value}</TextValue>;
  else if (typeof value === "object") return <LargeValue>{"{...}"}</LargeValue>;
}

function copyValueToClipBoard(value: RqlValue) {
  const str = valueToString(value);
  navigator.clipboard.writeText(str);
}

const ResultTable = React.memo(function ResultTable(props: ResultTableProps) {
  const { columns, rows } = props;
  return (
    <Table>
      <thead>
        <tr>
          <HeadCell>#</HeadCell>
          {columns.map(([name, dataType]) => {
            return (
              <HeadCell key={"header-" + name}>
                <ColumnName>{name}</ColumnName><ColumnDataType>: {dataType}</ColumnDataType>
              </HeadCell>
            );
          })}
        </tr>
      </thead>
      <tbody>
        {rows.map(({ num, columns }) => {
          return (
            <tr key={"row-" + num}>
              <RowNumber>{num + 1}</RowNumber>
              {columns.map((value, i) => (
                <Cell key={"col-" + i} numeric={typeof value === "number"}>
                  <CopyIconContainer>
                    <CopyIcon onClick={() => copyValueToClipBoard(value)}>⧉</CopyIcon>
                  </CopyIconContainer>
                  {renderValue(value)}
                </Cell>
              ))}
            </tr>
          );
        })}
      </tbody>
    </Table>
  );
});

type PageSelectProps = {
  page: number;
  setPage: (page: number) => void;
  maxPage: number;
};

function PageSelect({ page, setPage, maxPage }: PageSelectProps) {
  const arr: number[] = [];
  for (let i = 0; i <= maxPage; i++) arr.push(i);
  return (
    <PageSelectContainer>
      {arr.map(i =>
        <PageLink key={"page-" + i} selected={i === page} onClick={() => setPage(i)}>{i + 1}</PageLink>
      )}
    </PageSelectContainer>
  );
}

type RqlQueryTabProps = {
};

function RqlQueryTab(props: RqlQueryTabProps) {
  const [state, dispatch] = useReducer(reducer, initialState);
  const dispatchAction = useCallback((message: MessageEvent<any>) => {
    dispatch({ type: ActionType.ON_MESSAGE, message: message.data })
  }, []);
  const [sendQuery, wsStatus] = useWebsocket("ws://localhost:8080/rql", dispatchAction);

  const [queryInput, setQueryInput] = useSessionStorage("rql.query.input", "");

  const submit = useCallback(() => {
    console.log("Submit query: ", queryInput);
    sendQuery(queryInput);
  }, [queryInput]);

  const [rows, setRows]         = useState<ResultRow[]>([]);
  const [page, setPage]         = useState(0);
  const [maxPage, setMaxPage]   = useState(0);
  const PAGE_SIZE = 100;

  const allRows = state.queryResult ? state.queryResult.rows : [];
  const columns = state.queryResult ? state.queryResult.columns : [];

  useEffect(() => {
    if (state.queryResult) {
      const newMaxPage = Math.trunc(allRows.length / PAGE_SIZE);
      if (maxPage !== newMaxPage) setMaxPage(newMaxPage);

      const currentPage = page > maxPage ? maxPage : page;
      if (currentPage !== page) setPage(currentPage);

      const newRows = allRows.slice(currentPage * PAGE_SIZE, (currentPage + 1) * PAGE_SIZE);
      setRows(newRows);
    } else {
      setRows([]);
      setPage(0);
      setMaxPage(0);
    }
  }, [state.queryResult, page]);

  const resultsNum = allRows.length;

  return (
    <TabContainer>
      <TopSection>
        RQL query {wsStatus}
        <QueryInput
          value={queryInput}
          onChange={ev => setQueryInput(ev.target.value)}
          onSubmit={() => submit()}
        />
        <Button onClick={() => submit()}>Query</Button>
      </TopSection>
      {state.queryError && <div>{state.queryError}</div>}
      {state.queryResult && (
        <>
          <ResultCounter><span>{resultsNum}</span> results</ResultCounter>
          <PageSelect page={page} setPage={setPage} maxPage={maxPage} />
          <ResultTable columns={columns} rows={rows} />
          <PageSelect page={page} setPage={setPage} maxPage={maxPage} />
        </>
      )}
    </TabContainer>
  );
}

export default RqlQueryTab;

const TOP_HEIGHT="200px";

const TabContainer = styled.div`
  padding: 5px;
`;

const TopSection = styled.div`
  /*position: sticky;*/
  top: 0;
  left: 0;
  display: flex;
  flex-direction: column;
  min-height: ${TOP_HEIGHT};
  padding: 2px;
  background-color: #fff;
`;

const QueryInput = styled.textarea`
  padding: 10px;
  font-size: 14pt;
  font-family: Courier;
  border: 1px solid black;
  width: 100%;
  max-width: 100%;
  height: 100%;
`;

const Button = styled.button`
  padding: 10px;
  font-size: 12pt;
  background: #bbb;
  border: 2.5px solid #989;
  border-radius: 2px;
`;

const ResultCounter = styled.div`
  font-size: 12pt;
  /*margin-left: 50px;*/
  width: 100%;
  text-align: center;
`;

const PageSelectContainer = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: center;
  align-items: center;
`;

const PageLink = styled.button<{ selected: boolean }>`
  background: transparent;
  color: #668;
  border: 0;
  cursor: pointer;
  padding: 4px;
  font-size: 11pt;
  font-weight: bold;
  ${({ selected }) =>
    selected ? "text-decoration: none; font-size: 14pt; border: 2px solid #bbb" : "text-decoration: underline;"
  };
`;

const Table = styled.table``;

const HeadCell = styled.th`
  position: sticky;
  /*top: ${TOP_HEIGHT};*/
  top: 0;
  margin: 0;
  padding: 5px;
  border: 0.5px solid #ccc;
  background-color: #eee;
`;

const RowNumber = styled.td`
  padding: 5px;
  border: 0.5px solid #ccc;
  background-color: #eee;
  text-align: right;
`;

const CopyIconContainer = styled.div`
  position: relative;
  float: right;
  margin-right: 24px;
`;

const CopyIcon = styled.div`
  position: absolute;
  /*float: right;*/
  padding: 2px;
  height: 24px;
  width: 20px;
  font-size: 16px;
  border: 1px solid black;
  border-radius: 2px;
  align-text: center;
  background: #e8e8ef;
  cursor: pointer;
  opacity: 0.0;
  transition: opacity linear 0.3s;
`;

const Cell = styled.td<{ numeric?: boolean }>`
  margin: 0;
  padding: 5px;
  border: 0.5px solid #ccc;
  vertical-align: top;
  ${({ numeric }) => (numeric ? "text-align: right" : "")};

  &:hover ${CopyIcon} {
    opacity: 0.9;
  }
`;

const ColumnName = styled.span`
  font-weight: bold;
  color: #000;
`;

const ColumnDataType = styled.span`
  font-weight: normal;
  color: #888;
`;

const NullValue = styled.span`
  color: #aa9;
  font-size: 10pt;
  font-weight: bold;
`;

const BooleanValue = styled.span`
  color: #22a;
  font-weight: bold;
`;

const NumberValue = styled.span`
  color: #c2c;
`;

const TextValue = styled.span`
  color: #d22;
`;

const LargeValue = styled.div`
  overflow: scroll;
  max-height: 260px;
`;

const UnknownValue = styled.span`
  color: #888;
`;


