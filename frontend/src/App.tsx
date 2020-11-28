import React from "react";
import styled from 'styled-components';
import { Dispatch, useCallback, useEffect, useReducer, useRef, useState } from "react";
//import casLogo from "./279px-Cassandra_logo.svg.png";
//import casLogo from "./Apache-cassandra-icon.png";
import casLogo from "./CasTool-2-icon.png";
import "./App.css";

import { useWebsocket } from './UseWebsocketHook';

import { JsonSyntaxHighlight } from './JsonSyntaxHighlight';

type ColumnValue_Null = {
  Null: { };
};

type ColumnValue_Ascii = {
  Ascii: { };
};

type ColumnValue_Text = {
  Text: { v: string; };
};

type ColumnValue_Integer = {
  Integer: { v: number; };
};

type ColumnValue_BigInt = {
  BigInt: { v: number; };
};

type ColumnValue = ColumnValue_Null
  | ColumnValue_Ascii
  | ColumnValue_BigInt
  | ColumnValue_Integer
  | ColumnValue_Text
  | any;

interface ColumnDefinition {
  name: string;
  dataType: string;
}

interface ResultRow {
  index: number;
  columnValues: ColumnValue[];
}

enum QueryStatus {
  Done,
  InProgress
}

interface State {
  columnDefinitions: ColumnDefinition[];
  results: ResultRow[];
  queryError: string | null;
  queryStatus: QueryStatus;
}

const initialState: State = {
  columnDefinitions: [],
  results: [],
  queryError: null,
  queryStatus: QueryStatus.Done,
};

enum ActionType {
  ON_CLEAR_RESULTS,
  ON_START_QUERY,
  ON_MESSAGE,
};

interface OnClearResultsAction {
  type: ActionType.ON_CLEAR_RESULTS;
}

interface OnStartQueryAction {
  type: ActionType.ON_START_QUERY;
}

interface OnMessageAction {
  type: ActionType.ON_MESSAGE;
  message: any;
}

type Action = OnClearResultsAction | OnStartQueryAction | OnMessageAction;

interface QueryMessageSuccess {
  type: "QueryMessageSuccess";
  columns: ColumnDefinition[];
}

interface QueryMessageError {
  type: "QueryMessageError";
  error: string;
}

interface QueryMessageRows {
  type: "QueryMessageRows";
  rows: ResultRow[];
}

interface QueryMessageFinished {
  type: "QueryMessageFinished";
}

type QueryMessage =
    QueryMessageSuccess
  | QueryMessageError
  | QueryMessageRows
  | QueryMessageFinished;

function parseMessage(s: string): QueryMessage | undefined {
  const msg = JSON.parse(s);
  function extract(field: string): QueryMessage | undefined {
    if (msg[field] !== undefined) {
      return {
        type: field,
        ...msg[field]
      }
    } else return undefined;
  }
  return extract("QueryMessageSuccess")
    || extract("QueryMessageError")
    || extract("QueryMessageRows")
    || extract("QueryMessageFinished");
}

function handleMessage(state: State, msg: QueryMessage): State {
  switch (msg.type) {
    case "QueryMessageSuccess": {
      const { columns }: QueryMessageSuccess = msg;
      return { ...state, columnDefinitions: columns };
    }
    case "QueryMessageError": {
      const { error }: QueryMessageError = msg;
      return { ...state, results: [], queryError: error };
    }
    case "QueryMessageRows": {
      const rowsMessage: QueryMessageRows = msg;
      const rows = rowsMessage.rows;
      return { ...state, results: state.results.concat(rows) };
    }
    case "QueryMessageFinished": {
      return { ...state, queryStatus: QueryStatus.Done };
    }
  }
  return state;
}

const reducer = (state: State, action: Action) => {
  switch (action.type) {
    case ActionType.ON_CLEAR_RESULTS: {
      return { ...state, results: [] };
    }
    case ActionType.ON_START_QUERY: {
      return { ...state, queryStatus: QueryStatus.InProgress };
    }
    case ActionType.ON_MESSAGE: {
      const { message } = action;
      const m = parseMessage(message);
      if (m === undefined) throw Error("Invalid query message: " + message);
      return handleMessage(state, m);
    }
  }
  return state;
};

function columnValueToString(column: ColumnValue) {
  if (column.Null !== undefined) {
    return "NULL";
  } else if (column.Ascii !== undefined) {
    return column.Ascii.v;
  } else if (column.Bool !== undefined) {
    return column.Bool.v ? "true" : "false";
  } else if (column.BigInt !== undefined) {
    return column.BigInt.v.toString();
  } else if (column.Integer !== undefined) {
    return column.Integer.v.toString();
  } else if (column.SmallInt !== undefined) {
    return column.SmallInt.v.toString();
  } else if (column.TinyInt !== undefined) {
    return column.TinyInt.v.toString();
  } else if (column.TimeUuid !== undefined) {
    return column.TimeUuid.v;
  } else if (column.Text !== undefined) {
    return column.Text.v;
  } else if (column.Blob !== undefined) {
    const s = atob(column.Blob.v);
    const json = JSON.parse(s);
    const pretty = JSON.stringify(json, null, 2);
    return pretty;
  } else {
    return JSON.stringify(column);
  }
}

function renderColumnValue(column: ColumnValue, index: number) {
  if (column.Null !== undefined) {
    return <NullValue>NULL</NullValue>;
  } else if (column.Ascii !== undefined) {
    return <TextValue>{column.Ascii.v}</TextValue>;
  } else if (column.Bool !== undefined) {
    return <BooleanValue>{column.Bool.v ? "true" : "false"}</BooleanValue>;
  } else if (column.BigInt !== undefined) {
    return <IntegerValue>{column.BigInt.v}</IntegerValue>;
  } else if (column.Integer !== undefined) {
    return <IntegerValue>{column.Integer.v}</IntegerValue>;
  } else if (column.SmallInt !== undefined) {
    return <IntegerValue>{column.SmallInt.v}</IntegerValue>;
  } else if (column.TinyInt !== undefined) {
    return <IntegerValue>{column.TinyInt.v}</IntegerValue>;
  } else if (column.TimeUuid !== undefined) {
    return <IntegerValue>{column.TimeUuid.v}</IntegerValue>;
  } else if (column.Text !== undefined) {
    return <TextValue>{column.Text.v}</TextValue>;
  } else if (column.Blob !== undefined) {
    const s = atob(column.Blob.v);
    if (false)
      return <LargeValue>{s}</LargeValue>;
    else {
      const json = JSON.parse(s);
      const pretty = JSON.stringify(json, null, 2);
      return <LargeValue>
        <JsonSyntaxHighlight value={pretty} nopre={false} />
      </LargeValue>;
    }
  } else {
    return <UnknownValue>{JSON.stringify(column)}</UnknownValue>;
  }
}

function renderColumnDef(def: ColumnDefinition, index: number) {
  const { name, dataType } = def;
  return (
    <HeadCell key={name}>
      <ColumnName>{name}</ColumnName><ColumnDataType>: {dataType}</ColumnDataType>
    </HeadCell>
  );
}

function copyToClipBoardColumnValue(value: ColumnValue) {
  const str = columnValueToString(value);
  navigator.clipboard.writeText(str);
}

function renderRow(row: ResultRow, rowIndex: number) {
  return (
    <Row key={"row-" + rowIndex}>
      <RowNumber>{rowIndex}</RowNumber>
      {row.columnValues.map((column, i) => {
        return (
          <Cell key={"c" + i}>
            <CopyIconContainer>
              <CopyIcon onClick={() => copyToClipBoardColumnValue(column)}>⧉</CopyIcon>
            </CopyIconContainer>
            {renderColumnValue(column, i)}
          </Cell>
        );
      })}
    </Row>
  );
}

interface QueryResultsSectionProps {
  queryError: string | null;
  columnDefs: ColumnDefinition[];
  rows: ResultRow[];
}

function QueryResultsSectionImpl(props: QueryResultsSectionProps) {
  const { queryError, columnDefs, rows } = props;
  if (queryError !== null) {
    return (
    <ErrorContainer>
      <ErrorCaption>Error: </ErrorCaption>
      <ErrorContent>{queryError}</ErrorContent>
    </ErrorContainer>
    );
  } else {
    return (
      <ResultsTable>
        <thead>
          <Row key="column-defs">
            <HeadCell />
            {columnDefs.map((def, i) => renderColumnDef(def, i))}
          </Row>
        </thead>
        <tbody>
          {rows.map((row, i) => renderRow(row, i))}
        </tbody>
      </ResultsTable>
    );
  }
}

const QueryResultsSection = React.memo(QueryResultsSectionImpl);

/*
function renderQueryResults(state: State) {
  const { columnDefinitions, results, queryError } = state;
  if (queryError !== null) {
    return (
    <ErrorContainer>
      <ErrorCaption>Error: </ErrorCaption>
      <ErrorContent>{queryError}</ErrorContent>
    </ErrorContainer>
    );
  } else {
    return (
      <ResultsTable>
        <thead>
          <Row key="column-defs">
            <HeadCell />
            {columnDefinitions.map((def, i) => renderColumnDef(def, i))}
          </Row>
        </thead>
        <tbody>
          {results.map((row, i) => renderRow(row, i))}
        </tbody>
      </ResultsTable>
    );
  }
}*/

function App() {
  const [queryInput, setQueryInput] = useState("SELECT * FROM calloff.messages;");

  const [state, dispatch] = useReducer(reducer, initialState);
  const dispatchAction = useCallback((message: MessageEvent<any>) => {
    dispatch({ type: ActionType.ON_MESSAGE, message: message.data })
  //}, [dispatch]);
  }, []);
  const [sendQuery] = useWebsocket("ws://localhost:8080/squery", dispatchAction);

  const doQuery = useCallback(() => {
    console.log("query:", queryInput);
    dispatch({ type: ActionType.ON_CLEAR_RESULTS });
    dispatch({ type: ActionType.ON_START_QUERY });
    sendQuery(queryInput);
  }, [sendQuery, queryInput]);

  const logoIdle = state.queryStatus === QueryStatus.Done;

  return (
    <AppContainer>
      <Header>
        <AppLogo src={casLogo} alt="cassandra-tool-logo" idle={logoIdle} />
        <img src={casLogo} className="App-logo" alt="logo" />
        <h1>Cassandra Tool</h1>
      </Header>
      <QueryInputContainer>
        <QueryInput
          value={queryInput}
          onChange={ev => setQueryInput(ev.target.value)}
        />
        <Button onClick={() => doQuery()}>Execute</Button>
      </QueryInputContainer>
      <section>
        {/*<JsonSyntaxHighlight
          value={'{\n "Kala": kukko,\n "other-object": {]\n}'}
        />
        <JsonSyntaxHighlight value={invalidJson} />*/}
        <QueryResultsSection
          queryError={state.queryError}
          columnDefs={state.columnDefinitions}
          rows={state.results}
        />
      </section>
    </AppContainer>
  );
}

export default App;

const AppContainer = styled.div`
  overflow: scroll;
`;

const AppLogo = styled.img<{ idle: boolean }>`
  height: 10vmin;
  pointer-events: none;
  animation: ${({ idle }) =>
    (idle ? "App-logo-blink infinite 16s linear" : "App-logo-exec infinite 1.6s linear")
  }
`;

const Header = styled.header`
  position: sticky;
  left: 0;
  
  background-color: #282c34;
  min-height: 12vh;
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: center;

  font-family: "Courier";
  font-style: italic;
  font-size: calc(8px + 1vmin);
  color: white;
`;

const QueryInputContainer = styled.div`
  position: sticky;
  left: 0;
  
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: center;
`;

const Button = styled.button`
  padding: 10px;
  font-size: 12pt;
  background: #bbb;
  border: 2.5px solid #989;
  border-radius: 2px;
`;

const QueryInput = styled.input`
  padding: 10px;
  font-size: 11pt;
  border: 1px solid black;
  width: 60%;
`;

const ErrorContainer = styled.div`
  padding: 10px;
  font-size: 11pt;
`;

const ErrorCaption = styled.div`
  font-size: 12pt;
`;

const ErrorContent = styled.div`
  font-size: 10pt;
  color: #d22;
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

  &:hover ${CopyIcon} {
    opacity: 0.9;
  }
`;

const HeadCell = styled.th`
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

const NullValue = styled.span`
  color: #aa9;
  font-size: 10pt;
  font-weight: bold;
`;

const BooleanValue = styled.span`
  color: #22a;
  font-weight: bold;
`;

const IntegerValue = styled.span`
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


