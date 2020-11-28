import React from "react";
import styled from 'styled-components';
import { Dispatch, useCallback, useEffect, useReducer, useRef, useState } from "react";
import casLogo from "./279px-Cassandra_logo.svg.png";
import "./App.css";

import { JsonSyntaxHighlight } from './JsonSyntaxHighlight';

const useWebsocket = (
  url: string,
  //receive: (event: MessageEvent<any>) => void
  dispatch: (msg: MessageEvent<any>) => void
): [(msg: any) => void] => {
  //const [reconnection, setReconnection] = useState(0);
  const ws = useRef<WebSocket | null>(null);
  const pending = useRef<any[]>([]);

  const send = useCallback((msg: any): void => {
    if (ws.current && ws.current.readyState === WebSocket.OPEN) {
      ws.current.send(msg);
    } else {
      pending.current.push(msg);
      //setReconnection(reconnection + 1);
    }
  }, []);

  useEffect(() => {
    if (ws.current === null || ws.current.readyState === WebSocket.CLOSED) {
      console.log("Create new websocket");
      ws.current = new WebSocket(url);
    }
    return () => {
      if (ws.current && ws.current.readyState !== WebSocket.CLOSED) {
      console.log("Closing websocket");
        ws.current.close();
        ws.current = null;
      }
    };
  }, [url]);
  useEffect(() => {
    if (ws.current) {
      //console.log("Set message handler", receive);
      //ws.current.addEventListener('message', receive);
      console.log("Set message handler", dispatch);
      ws.current.addEventListener('message', dispatch);
      if (pending.current.length > 0) {
        const oldPending = pending.current;
        pending.current = [];
        oldPending.forEach(p => send(p));
      }
    }
  });
  return [send];
};

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

type ResultRow = {
  index: number;
  columnValues: ColumnValue[];
};

interface State {
  columnDefinitions: any[];
  results: ResultRow[];
  queryError: string | null;
}

const initialState: State = {
  columnDefinitions: [],
  results: [],
  queryError: null
};

enum ActionType {
  ON_CLEAR_RESULTS,
  ON_MESSAGE,
};

interface OnMessageAction {
  type: ActionType.ON_MESSAGE;
  message: any;
}

interface OnClearResultsAction {
  type: ActionType.ON_CLEAR_RESULTS;
}

type Action = OnClearResultsAction | OnMessageAction;

interface QueryMessageSuccess {
  type: "QueryMessageSuccess";
}

interface QueryMessageError {
  type: "QueryMessageError";
  error: string;
}

interface QueryMessageRows {
  type: "QueryMessageRows";
  rows: ResultRow[];
}

type QueryMessage = QueryMessageSuccess | QueryMessageError | QueryMessageRows;

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
    || extract("QueryMessageRows");
}

function handleMessage(state: State, msg: QueryMessage): State {
  switch (msg.type) {
    case "QueryMessageSuccess": {
      return state;
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
  }
  return state;
}

const reducer = (state: State, action: Action) => {
  switch (action.type) {
    case ActionType.ON_CLEAR_RESULTS: {
      return { ...state, results: [] };
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
    const json = JSON.parse(s);
    const pretty = JSON.stringify(json, null, 2);
    return <LargeValue>
      <JsonSyntaxHighlight value={pretty} nopre={false} />
    </LargeValue>;
  } else {
    return <UnknownValue>{JSON.stringify(column)}</UnknownValue>;
  }
}

function renderRow(row: ResultRow, rowIndex: number) {
  return (
    <Row key={"row-" + rowIndex}>
      <RowNumber>{rowIndex}</RowNumber>
      {row.columnValues.map((column, i) => {
        return (
          <Cell key={"c" + i}>
            {renderColumnValue(column, i)}
          </Cell>
        );
      })}
    </Row>
  );
}

function renderQueryResults(state: State) {
  const { results, queryError } = state;
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
        <thead></thead>
        <tbody>
          {results.map((row, i) => renderRow(row, i))}
        </tbody>
      </ResultsTable>
    );
  }
}

function App() {
  const [queryInput, setQueryInput] = useState("SELECT * FROM calloff.messages;");

  const [state, dispatch] = useReducer(reducer, initialState);
  const dispatchAction = useCallback((message: MessageEvent<any>) => {
    dispatch({ type: ActionType.ON_MESSAGE, message: message.data })
  }, [dispatch]);
  const [sendQuery] = useWebsocket("ws://localhost:8080/squery", dispatchAction);

  const doQuery = useCallback(() => {
    console.log("query:", queryInput);
    dispatch({ type: ActionType.ON_CLEAR_RESULTS });
    sendQuery(queryInput);
  }, [sendQuery, queryInput]);

  const invalidJson = '{\n    "callOff": {\n        "consignee": {\n            "addressLine1": ",\n            "addressLine2": "",\n            "finalDeliveryId": "",\n            "recipient": ""\n        },\n        "id": "mLK2DS67_A00046001078R01",\n        "inhouseCallOffId": "",\n        "messageInfo": {\n            "documentId": "mLK2DS67",\n            "issued": ?"2020-07-16T10:47:14.563Z"\n        },\n        "meta": {\n            "created": "2020-07-16T10:47:14.564Z",\n            "requestId": "NoRequestId",\n            "sourceSystem": "SAP",\n            "status": "Created"\n        },\n        "parts": \n            {\n                "deliveries": [\n                    {\n                        "amount": 24,\n                        "amountDecimal": 24,\n                        "isForecast": false,\n                        "pickup": "2020-07-16"\n                    },\n                    {\n                        "amount": 48,\n                        "amountDecimal": 48,\n                        "isForecast": false,\n                        "pickup": "2020-07-23"\n                    },\n                    {\n                        "amount": 15,\n                        "amountDecimal": 15,\n                        "isForecast": false,\n                        "pickup": "2020-07-30"\n                    }\n                ],\n                "isSynchro": false,\n                "name": "",\n                "partNumber": "A00046001078R01",\n                "salesOrderId": ""\n            }\n        ],\n        "serialNumber": "12345",\n        "supplier": {\n            "supplierId": "10001154K"\n        },\n        "synchro": false,\n        "synchroForecast": false\n    },\n    "requestId": "NoRequestId"\n}\n'

  return (
    <AppContainer>
      <Header>
        <img src={casLogo} className="App-logo" alt="logo" />
        <h1>Cassandra Tool</h1>
      </Header>
      <section>
        <QueryInput
          value={queryInput}
          onChange={ev => setQueryInput(ev.target.value)}
        />
        <Button onClick={() => doQuery()}>Execute</Button>
        <JsonSyntaxHighlight
          value={'{\n "Kala": kukko,\n "other-object": {]\n}'}
        />
        <JsonSyntaxHighlight value={invalidJson} />
        {renderQueryResults(state)}
      </section>
    </AppContainer>
  );
}

export default App;

const AppContainer = styled.div`
  overflow: scroll;
`;

const Header = styled.header`
  position: sticky;
  left: 0;
  
  background-color: #282c34;
  min-height: 20vh;
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: center;
  font-size: calc(10px + 2vmin);
  color: white;
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


