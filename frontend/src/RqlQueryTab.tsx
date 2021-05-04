import React, { useCallback, useEffect, useReducer, useState } from "react";
import styled from 'styled-components';

import { useWebsocket } from "./utils/UseWebsocketHook";
import useSessionStorage from "./utils/UseSessionStorageHook";
//import { RqlValue, RqlValueType, is_date, is_object } from "./server-rql/RqlValue";
import * as rql from "./server-rql/index";
import {RqlBool} from "./server-rql/RqlValue";

interface ResultRow {
  num: number;
  columns: rql.Value[];
}

interface QueryResult {
  columns: [string, rql.ValueType][];
  rows: ResultRow[];
}

interface State {
  queryResult: QueryResult | null;
  queryErrors: string[] | null;
}

const initialState: State = {
  queryResult: null,
  queryErrors: null,
};

enum ActionType {
  ON_MESSAGE,
}

interface OnMessage {
  type: ActionType;
  message: any;
}

type Action = OnMessage;

interface RqlMessageSuccess {
  _type: "Success";
  columns: [string, rql.ValueType][];
}
interface RqlMessageError {
  _type: "Error";
  errors: string[];
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
          return { ...state, queryResult: { columns: message.columns, rows: []}, queryErrors: null };
        case "Error":
          return { ...state, queryResult: null, queryErrors: message.errors };
        case "Rows":
          const { columns = [], rows = [] } = state.queryResult || {};
          return { ...state, queryResult: { columns, rows: rows.concat(message.rows) } };
      }
      return state;
    }
  }
};

type ResultTableProps = {
  columns: [string, rql.ValueType][];
  rows: ResultRow[];
};

function dateToString(value: rql.Date): string {
  const dt = new Date(value.date);
  return dt.toISOString();
}

class RqlPrinter implements rql.FoldHandler<void> {
  indent: string;
  result: string;

  constructor() {
    this.indent = "";
    this.result = "";
  }

  getResult() {
    return this.result;
  }

  put(s: string) {
    this.result += s;
    return this;
  }

  putIndent() {
    this.result += this.indent;
    return this;
  }

  nl() {
    this.result += "\n";
    return this;
  }

  indentMore() {
    this.indent += "  ";
    return this;
  }

  indentLess() {
    this.indent = this.indent.substr(0, this.indent.length - 2);
    return this;
  }

  putValue(value: rql.Value) {
    rql.fold_value(this)(value);
    return this;
  }

  null() { this.put("null"); }
  bool(x: rql.Bool) { this.put(x ? "true" : "false"); }
  num(x: rql.Num) { this.put(x.toString()); }
  str(x: rql.Str) { this.put(`"${x}"`); }
  date(x: rql.Date) { this.put(dateToString(x)); }
  obj(x: rql.Obj) {
    const fields = Object.entries(x.obj);
    if (fields.length === 0) {
      this.put("{ }");
    } else {
      this.put("{").nl().indentMore();
      fields.forEach(([key, value]) => {
        this.putIndent().put(key).put(": ").putValue(value).nl();
      });
      this.indentLess().putIndent().put("}");
    }
  }
}

class RqlJsonPrinter implements rql.FoldHandler<void> {
  indent: string;
  result: string;

  constructor() {
    this.indent = "";
    this.result = "";
  }

  getResult() {
    return this.result;
  }

  put(s: string) {
    this.result += s;
    return this;
  }

  putIndent() {
    this.result += this.indent;
    return this;
  }

  nl() {
    this.result += "\n";
    return this;
  }

  indentMore() {
    this.indent += "  ";
    return this;
  }

  indentLess() {
    this.indent = this.indent.substr(0, this.indent.length - 2);
    return this;
  }

  putValue(value: rql.Value) {
    rql.fold_value(this)(value);
    return this;
  }

  null() { this.put("null"); }
  bool(x: rql.Bool) { this.put(x ? "true" : "false"); }
  num(x: rql.Num) { this.put(x.toString()); }
  str(x: rql.Str) { this.put(`"${x}"`); }
  date(x: rql.Date) { this.put(`"${dateToString(x)}"`); }
  obj(x: rql.Obj) {
    const fields = Object.entries(x.obj);
    if (fields.length === 0) {
      this.put("{ }");
    } else {
      this.put("{").nl().indentMore();
      fields.forEach(([key, value]) => {
        this.putIndent().put(`"${key}"`).put(": ").putValue(value).put(",").nl();
      });
      this.indentLess().putIndent().put("}");
    }
  }
}

function objectToString(value: rql.Obj): string {
  const printer = new RqlJsonPrinter()
  printer.obj(value)
  return printer.getResult();
}

const rqlStringFolder: rql.FoldHandler<string> = {
  null: () => "null",
  bool: x => x.toString(),
  num: x => x.toString(),
  str: x => x,
  date: x => dateToString(x),
  obj: x => objectToString(x),
};

function valueToString(value: rql.Value): string {
  return rql.fold_value(rqlStringFolder)(value);
}

const rqlElementFolder: rql.FoldHandler<React.ReactElement> = {
  null: () => <NullValue>null</NullValue>,
  bool: x => <BooleanValue>{x ? "true" : "false"}</BooleanValue>,
  num: x => <NumberValue>{x}</NumberValue>,
  str: x => <TextValue>{x}</TextValue>,
  date: x => <NumberValue>{dateToString(x)}</NumberValue>,
  obj: x => <RQLObject obj={x} />
};

function renderRqlValue(value: rql.Value) {
  return rql.fold_value(rqlElementFolder)(value);
}

type RQLObjectProps = {
  obj: rql.Obj
}

function RQLObject(props: RQLObjectProps) {
  const { obj } = props;
  const [isOpen, setOpen] = useState(true);
  const fields = Object.entries(obj.obj);
  if (fields.length > 0) {
    return (<>
      <RQLObjectOpenParen onClick={() => setOpen(!isOpen)}>{isOpen ? "-" : "+"}{" {"}</RQLObjectOpenParen>
      {isOpen ? (
        fields.map(([key, value]) => {
            return (
            <RQLField>
              <RQLFieldKey>{key}</RQLFieldKey>: {renderRqlValue(value)}
            </RQLField>
            );
          })
        ) : (
          <RQLSpecial>...</RQLSpecial>
        )
      }
      <RQLSpecial>{"}"}</RQLSpecial>
    </>);
  } else {
    return <RQLSpecial>{"{ }"}</RQLSpecial>;
  }
}

const RQLObjectOpenParen = styled.button`
  margin-left: -8px;
  padding: 2px;
  color: #005050;
`;

const RQLSpecial = styled.span`
  color: #005050;
`;

const RQLField = styled.div`
  margin-left: 1em;
`;

const RQLFieldKey = styled.span`
  color: #00a0a0;
`;


function renderValue(value: rql.Value) {
  if (value === null) return <NullValue>NULL</NullValue>;
  else if (typeof value === "boolean") return <BooleanValue>{value ? "true" : "false"}</BooleanValue>;
  else if (typeof value === "number") return <NumberValue>{value}</NumberValue>;
  else if (typeof value === "string") return <TextValue>{value}</TextValue>;
  else if (rql.is_date(value)) return <NumberValue>{dateToString(value)}</NumberValue>;
  else if (rql.is_object(value)) return <LargeValue><RQLObject obj={value} /></LargeValue>;
  else if (typeof value === "object") return <LargeValue>{"{...}"}</LargeValue>;
}

function copyValueToClipBoard(value: rql.Value) {
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
      {state.queryErrors && (
        <div>
        {state.queryErrors.map((error, line) => <ErrorLine key={"error-line" + line}>{error}</ErrorLine>)}
        </div>
      )}
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

const ErrorLine = styled.pre`
  font-family: Courier;
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
  /*color: #aa9;*/
  color: #c000c0;
  font-size: 10pt;
  font-weight: bold;
`;

const BooleanValue = styled.span`
  /*color: #22a;*/
  color: #c000c0;
  font-weight: bold;
`;

const NumberValue = styled.span`
  /*color: #c2c;*/
  color: #c000c0;
`;

const TextValue = styled.span`
  /*color: #d22;*/
  color: #a0a000;
`;

const LargeValue = styled.div`
  overflow: scroll;
  max-height: 260px;
`;

const UnknownValue = styled.span`
  color: #888;
`;


