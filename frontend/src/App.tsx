import React from "react";
import styled from 'styled-components';
import { Dispatch, useCallback, useEffect, useReducer, useRef, useState } from "react";
import casLogo from "./279px-Cassandra_logo.svg.png";
import "./App.css";

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

type ColumnValue_Text = {
  Text: { s: string; };
};

type ColumnValue = ColumnValue_Text | any;

type ResultRow = {
  index: number;
  columnValues: ColumnValue[];
};

interface State {
  columnDefinitions: any[];
  results: ResultRow[];
}

const initialState: State = {
  columnDefinitions: [],
  results: []
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
      return state;
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
  if (column.Text !== undefined) {
    return <TextValue>{column.Text.s}</TextValue>;
  } else {
    return <UnknownValue>{JSON.stringify(column)}</UnknownValue>;
  }
}

function App() {
  //const [results, setResults] = useState<Results>([]);
  //const appendResults = useCallback(
  //  (newResults: Results) => setResults(results.concat(newResults)),
  //  [results, setResults]);
  //const [sendQuery] = useWebsocket("ws://localhost:8080/squery", event => appendResults([event.data]));
  
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

  return (
    <div className="App">
      <header className="App-header">
        <img src={casLogo} className="App-logo" alt="logo" />
        <h1>Cassandra Tool</h1>
      </header>
      <section>
        <QueryInput
          value={queryInput}
          onChange={ev => setQueryInput(ev.target.value)}
        />
        <Button onClick={() => doQuery()}>Execute</Button>
        <table>
        <thead></thead>
        <tbody>
          {state.results.map((row, i) => {
            return (
              <Row key={"row-" + i}>
                <RowNumber>{i}</RowNumber>
                {/*row.toString()*/}
                {row.columnValues.map((column, i) => {
                  return (
                    <Cell key={"c" + i}>
                      {renderColumnValue(column, i)}
                    </Cell>
                  );
                })}
              </Row>
            );
          })}
        </tbody>
        </table>
      </section>
    </div>
  );
}

export default App;

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

const Row = styled.tr``;

const RowNumber = styled.td``;

const Cell = styled.td``;

const TextValue = styled.span`
  color: #d22;
`;

const UnknownValue = styled.span`
  color: #888;
`;


