import React, { useCallback, useEffect, useMemo, useReducer, useRef, useState } from "react";
import styled from 'styled-components';

import { useWebsocket } from "./utils/UseWebsocketHook";
import useSessionStorage from "./utils/UseSessionStorageHook";
import useNotifyClickOutside from "./utils/UseNotifyClickOutside";
import { RqlInstructions } from "./RqlInstructions";
import * as rql from "./server-rql/index";

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
  list(x: rql.List) {
    if (x.length === 0) {
      this.put("{ }");
    } else {
      this.put("[").nl().indentMore();
      x.forEach(value => {
        this.putIndent().putValue(value).put(",").nl();
      });
      this.indentLess().putIndent().put("]");
    }
  }
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
  blob(x: rql.Blob) {
    this.put(`[${x.blob}]`)
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
  list(x: rql.List) {
    if (x.length === 0) {
      this.put("[ ]");
    } else {
      this.put("[").nl().indentMore();
      let first = true;
      x.forEach(value => {
        if (!first) this.put(",").nl();
        this.putIndent().putValue(value)
        first = false;
      });
      this.indentLess().nl().putIndent().put("]");
    }
  }
  obj(x: rql.Obj) {
    const fields = Object.entries(x.obj);
    if (fields.length === 0) {
      this.put("{ }");
    } else {
      this.put("{").nl().indentMore();
      let first = true;
      fields.forEach(([key, value]) => {
        if (!first) this.put(",").nl();
        this.putIndent().put(`"${key}"`).put(": ").putValue(value);
        first = false;
      });
      this.indentLess().nl().putIndent().put("}");
    }
  }
  blob(x: rql.Blob) {
    this.put(`"${x.blob}"`)
  }
}

function listToString(value: rql.List): string {
  const printer = new RqlJsonPrinter()
  printer.list(value)
  return printer.getResult();
}

function objectToString(value: rql.Obj): string {
  const printer = new RqlJsonPrinter()
  printer.obj(value)
  return printer.getResult();
}

function objectToJsonString(value: rql.Obj): string {
  const printer = new RqlJsonPrinter()
  printer.obj(value)
  return printer.getResult();
}

function blobToString(value: rql.Blob): string {
  const printer = new RqlJsonPrinter()
  printer.blob(value)
  return printer.getResult();
}

const rqlStringFolder: rql.FoldHandler<string> = {
  null: () => "null",
  bool: x => x.toString(),
  num: x => x.toString(),
  str: x => x,
  date: x => dateToString(x),
  list: x => listToString(x),
  obj: x => objectToString(x),
  blob: x => blobToString(x),
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
  list: x => <RQLList list={x} />,
  obj: x => <RQLObject obj={x} />,
  blob: x => <RQLBlob blob={x} />,
};

function renderRqlValue(value: rql.Value) {
  return rql.fold_value(rqlElementFolder)(value);
}

type RQLListProps = {
  list: rql.List
};

function RQLList(props: RQLListProps) {
  const { list } = props;
  const [isOpen, setOpen] = useState(true);
  if (list.length > 0) {
    return (<>
      <RQLObjectOpenParen onClick={() => setOpen(!isOpen)}>{"["}</RQLObjectOpenParen>
      {isOpen ? (
        list.map(value => {
            return (
            <RQLField>
              {renderRqlValue(value)}
            </RQLField>
            );
          })
        ) : (
          <RQLSpecial>...</RQLSpecial>
        )
      }
      <RQLSpecial>{"]"}</RQLSpecial>
    </>);
  } else {
    return <RQLSpecial>{"[ ]"}</RQLSpecial>;
  }
}

type RQLObjectProps = {
  obj: rql.Obj
};

function RQLObject(props: RQLObjectProps) {
  const { obj } = props;
  const [isOpen, setOpen] = useState(true);
  const fields = Object.entries(obj.obj);
  if (fields.length > 0) {
    return (<>
      <RQLObjectOpenParen onClick={() => setOpen(!isOpen)}>{"{"}</RQLObjectOpenParen>
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

function blobToBase64String(blob: rql.Blob, cappedLength: number = 0): string {
  return (cappedLength <= 0) ? blob.blob : blob.blob.substring(0, cappedLength);
}

function blobToHexString(blob: rql.Blob, cappedLength: number = 0): string {
  const lookup = ["0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f"];
  const enc = new TextEncoder();
  const bytes = enc.encode(atob(blob.blob));
  let str = "";
  (cappedLength <= 0 ? bytes : bytes.subarray(0, cappedLength)).forEach(byte => {
      const hex1 = byte & 0xf;
      const hex2 = (byte >> 4) & 0xf;
      str += lookup[hex1];
      str += lookup[hex2];
  });
  return str;
}

function blobToJsonString(blob: rql.Blob): string {
  const enc = new TextEncoder();
  const bytes = enc.encode(atob(blob.blob));
  let str = "[";
  if (bytes.length > 0) {
    str += bytes[0];
    for (let i = 1; i < bytes.length; i++) {
        str += ","
        str += bytes[i];
    }
  }
  return str + "]";
}

type RQLBlobProps = {
  blob: rql.Blob;
};

function RQLBlob(props: RQLBlobProps) {
  const { blob } = props;
  const [isOpen, setOpen] = useState(false);

  const cappedLength = 16;
  const str = useMemo(() => {
    return blobToHexString(blob, isOpen ? 0 : cappedLength);
  }, [blob, isOpen]);

  if (str.length > 0) {
    return (<>
      <RQLObjectOpenParen onClick={() => setOpen(!isOpen)}>[</RQLObjectOpenParen>
      <RQLSpecial>0x{str}{!isOpen && str.length > cappedLength && "..."}</RQLSpecial>
      <RQLSpecial>]</RQLSpecial>
    </>);
  } else {
    return <RQLSpecial>[ ]</RQLSpecial>;
  }
}

const RQLObjectOpenParen = styled.button`
  font-size: 16px;
  text-align: right;
  width: 16px;
  white-space: nowrap;
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
  else if (Array.isArray(value)) return <LargeValue><RQLList list={value} /></LargeValue>;
  else if (rql.is_object(value)) return <LargeValue><RQLObject obj={value} /></LargeValue>;
  else if (rql.is_blob(value)) return <RQLBlob blob={value} />;
  else if (typeof value === "object") return <LargeValue>{"{...}"}</LargeValue>;
}

function copyToClipBoard(x: string) {
  navigator.clipboard.writeText(x);
}

function copyValueToClipBoard(value: rql.Value) {
  const str = valueToString(value);
  copyToClipBoard(str);
}

function unixSeconds(date: rql.Date): number {
  return Math.trunc(new Date(date.date).getTime() / 1000);
}
function unixMillis(date: rql.Date): number {
  return new Date(date.date).getTime();
}

const ResultTable = React.memo(function ResultTable(props: ResultTableProps) {
  const { columns, rows } = props;
  const [copyCell, setCopyCell] = useState<[number, number] | null>(null);
  const copySelectRef = useRef<HTMLDivElement | null>(null);

  useNotifyClickOutside(copySelectRef, () => setCopyCell(null));

  const copyIcon = (row: number, column: number, value: rql.Value) => {
    if (copyCell !== null && copyCell[0] === row && copyCell[1] === column) {
      switch (rql.type_of(value)) {
        case rql.ValueType.Date:
          return <CopySelect ref={copySelectRef}>
            <CopyOption onClick={() => { setCopyCell(null); copyValueToClipBoard(value); }}>ISO</CopyOption>
            <CopyOption onClick={() => { setCopyCell(null); copyToClipBoard(unixSeconds(value as rql.Date).toString()); }}>Unix s</CopyOption>
            <CopyOption onClick={() => { setCopyCell(null); copyToClipBoard(unixMillis(value as rql.Date).toString()); }}>Unix ms</CopyOption>
          </CopySelect>;
        case rql.ValueType.List:
          return <CopySelect ref={copySelectRef}>
            <CopyOption onClick={() => { setCopyCell(null); copyValueToClipBoard(value); }}>JSON</CopyOption>
          </CopySelect>;
        case rql.ValueType.Obj:
          return <CopySelect ref={copySelectRef}>
            <CopyOption onClick={() => { setCopyCell(null); copyToClipBoard(objectToJsonString(value as rql.Obj)); }}>JSON</CopyOption>
          </CopySelect>;
        case rql.ValueType.Blob:
          return <CopySelect ref={copySelectRef}>
             <CopyOption onClick={() => { setCopyCell(null); copyToClipBoard(blobToBase64String(value as rql.Blob)); }}>base64</CopyOption>
            <CopyOption onClick={() => { setCopyCell(null); copyToClipBoard(blobToHexString(value as rql.Blob)); }}>hex</CopyOption>
            <CopyOption onClick={() => { setCopyCell(null); copyToClipBoard(blobToJsonString(value as rql.Blob)); }}>JSON</CopyOption>
          </CopySelect>;
      }
    } else {
      switch (rql.type_of(value)) {
        case rql.ValueType.Null:
        case rql.ValueType.Bool:
        case rql.ValueType.Num:
        case rql.ValueType.Str:
          return <CopyIcon onClick={() => copyValueToClipBoard(value)}>⧉</CopyIcon>;
        case rql.ValueType.Date:
        case rql.ValueType.List:
        case rql.ValueType.Obj:
        case rql.ValueType.Blob:
          return <CopyIcon onClick={() => setCopyCell([row, column])}>⧉</CopyIcon>;
      }
    }
  };

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
                    {copyIcon(num, i, value)}
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

  const [showInstructions, setShowInstructions] = useState(false);

  const resultsNum = allRows.length;

  return (
    <TabContainer>
      <TopSection>
        <Header>
          <span>RQL query {wsStatus}</span>
          <button onClick={() => setShowInstructions(!showInstructions)}>Help</button>
        </Header>
        {showInstructions && <RqlInstructions />}
        <QueryInput
          value={queryInput}
          rows={6}
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

const Header = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: space-between;
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
`;

const CopyIcon = styled.div`
  position: absolute;
  right: 0;
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

const CopySelect = styled.div`
  position: absolute;
  right: 0;
  display: flex;
  flex-direction: row;
  padding: 2px;
  height: 28px;
  font-size: 16px;
  border: 1px solid black;
  border-radius: 2px;
  background: #e8e8ef;
`;

const CopyOption = styled.button`
  padding: 2px;
  font-size: 10px;
  min-width: 32px;
  line-height: 0.8;
  border: 1px solid black;
  border-radius: 2px;
  background: #e8e8ef;
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


