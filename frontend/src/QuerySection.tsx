import React, { useMemo, useState } from 'react';
import styled from 'styled-components';
import { Dispatch, useCallback } from "react";

import { ColumnDefinition, ColumnValue, ResultRow, ResultPage, dataTypeToString, ColumnValueDataTypeCode } from './types';
import { Action, ActionType, State } from './reducer';

import { JsonSyntaxHighlight } from './json-syntax/JsonSyntaxHighlight';

import useSessionStorage from './utils/UseSessionStorageHook';
import { blobToBase64String, blobToHexString } from './utils/blob';
import {detailColor} from './colors';

function columnValueToString(columnDef: ColumnDefinition, column: ColumnValue): string {
  if (column === null) return "NULL";
  switch (columnDef.dataType.code) {
    case ColumnValueDataTypeCode.Ascii:
    case ColumnValueDataTypeCode.Text:
    case ColumnValueDataTypeCode.Uuid:
    case ColumnValueDataTypeCode.TimeUuid:
      return column as string;
    case ColumnValueDataTypeCode.Bool:
      return column === true ? "true" : "false";
    case ColumnValueDataTypeCode.TinyInt:
    case ColumnValueDataTypeCode.SmallInt:
    case ColumnValueDataTypeCode.Integer:
    case ColumnValueDataTypeCode.BigInt:
    case ColumnValueDataTypeCode.Float:
    case ColumnValueDataTypeCode.Double:
    case ColumnValueDataTypeCode.Counter:
      return column.toString();
    case ColumnValueDataTypeCode.Blob: {
      return blobToHexString(column);
    }
    case ColumnValueDataTypeCode.List: {
      const pretty = JSON.stringify(column, null, 2);
      return pretty;
    }
    case ColumnValueDataTypeCode.Set: {
      const pretty = JSON.stringify(column, null, 2);
      return pretty;
    }
    case ColumnValueDataTypeCode.Map: {
      const pretty = JSON.stringify(column, null, 2);
      return pretty;
    }
    default:
      return JSON.stringify(column);
  }
}

interface BlobProps {
  blob: string;
  type: "hex" | "base64";
}

function BlobText(props: BlobProps) {
  const { blob } = props;
  const [isOpen, setOpen] = useState(false);

  const cappedLength = 8;
  const str = useMemo(() => {
    return blobToHexString(blob, isOpen ? 0 : cappedLength);
  }, [blob, isOpen]);

  if (str.length > 0) {
    return (<BlobContainer>
      <BlobContent>
        <BlobButton onClick={() => setOpen(!isOpen)}>[</BlobButton>
        0x{str}{!isOpen && "..."}]
      </BlobContent>
      <BlobLen>{blob.length} bytes</BlobLen>
    </BlobContainer>);
  } else {
    return <BlobContainer><BlobContent>[ ]</BlobContent></BlobContainer>;
  }
}

const BlobButton = styled.button`
  font-size: 1.1em;
  display: flex;
  flex-direction: row;
  padding: 4px;
  margin: 0;
  border: 1px solid transparent;
  background: transparent;
`;

const BlobContainer = styled.div`
  display: flex;
  flex-direction: column;
  align-items: baseline;
  &:hover {
    ${BlobButton} {
      border: 1px solid #aaa;
      background: #eee;
    }
  }
`;

const BlobContent = styled.div`
  display: flex;
  flex-direction: row;
  align-items: baseline;
`;

const BlobLen = styled.div`
  color: ${detailColor};
`;

function renderColumnValue(columnDef: ColumnDefinition, column: ColumnValue, index: number) {
  if (column === null) return <NullValue>NULL</NullValue>;
  switch (columnDef.dataType.code) {
    case ColumnValueDataTypeCode.Ascii:
    case ColumnValueDataTypeCode.Text:
    case ColumnValueDataTypeCode.Uuid:
    case ColumnValueDataTypeCode.TimeUuid:
      return <TextValue>{column as string}</TextValue>;
    case ColumnValueDataTypeCode.Bool:
      return <BooleanValue>{column === true ? "true" : "false"}</BooleanValue>;
    case ColumnValueDataTypeCode.TinyInt:
    case ColumnValueDataTypeCode.SmallInt:
    case ColumnValueDataTypeCode.Integer:
    case ColumnValueDataTypeCode.BigInt:
    case ColumnValueDataTypeCode.Float:
    case ColumnValueDataTypeCode.Double:
    case ColumnValueDataTypeCode.Counter:
      return <IntegerValue>{column.toString()}</IntegerValue>;
    case ColumnValueDataTypeCode.Blob: {
      return <BlobText blob={column} type="hex" />;
    }
    case ColumnValueDataTypeCode.List: {
      const pretty = JSON.stringify(column, null, 2);
      return <JsonSyntaxHighlight value={pretty} nopre={false} />
    }
    case ColumnValueDataTypeCode.Set: {
      const pretty = JSON.stringify(column, null, 2);
      return <JsonSyntaxHighlight value={pretty} nopre={false} />
    }
    case ColumnValueDataTypeCode.Map: {
      const pretty = JSON.stringify(column, null, 2);
      return <JsonSyntaxHighlight value={pretty} nopre={false} />
    }
    default:
      const pretty = JSON.stringify(column);
      return <JsonSyntaxHighlight value={pretty} nopre={false} />
  }
}

function renderColumnDef(def: ColumnDefinition, index: number) {
  const { name, dataType } = def;
  return (
    <HeadCell key={name}>
      <ColumnName>{name}</ColumnName><ColumnDataType>: {dataTypeToString(dataType)}</ColumnDataType>
    </HeadCell>
  );
}

function copyToClipBoardColumnValue(columnDef: ColumnDefinition, value: ColumnValue) {
  const str = columnValueToString(columnDef, value);
  navigator.clipboard.writeText(str);
}

function renderRow(row: ResultRow, rowIndex: number, columnDefs: ColumnDefinition[]) {
  return (
    <Row key={"row-" + rowIndex}>
      <RowNumber>{row.index}</RowNumber>
      {row.columnValues.map((column, i) => {
        const columnDef = columnDefs[i];
        return (
          <Cell key={"c" + i}>
            <CopyIconContainer>
              <CopyIcon onClick={() => copyToClipBoardColumnValue(columnDef, column)}>⧉</CopyIcon>
            </CopyIconContainer>
            {renderColumnValue(columnDef, column, i)}
          </Cell>
        );
      })}
    </Row>
  );
}

interface QueryResultsSectionProps {
  queryError: string | null;
  columnDefs: ColumnDefinition[];
  page: ResultPage;
}

function QueryResultsSectionImpl(props: QueryResultsSectionProps) {
  const { queryError, columnDefs, page } = props;
  if (queryError !== null) {
    return (
    <ErrorContainer>
      <ErrorCaption>Error: </ErrorCaption>
      <ErrorContent>{queryError}</ErrorContent>
    </ErrorContainer>
    );
  } else {
    return (columnDefs.length > 0) ? (
      <ResultsTable>
        <thead>
          <Row key="column-defs">
            <HeadCell />
            {columnDefs.map((def, i) => renderColumnDef(def, i))}
          </Row>
        </thead>
        <tbody>
          {page.rows.map((row, i) => renderRow(row, i, columnDefs))}
        </tbody>
      </ResultsTable>
    ) : null;
  }
}

const QueryResultsSection = React.memo(QueryResultsSectionImpl);

interface QuerySectionProps {
  dispatch: Dispatch<Action>,
  sendQuery: (query: string) => void,
  state: State;
}

function QuerySection(props: QuerySectionProps) {
  const { dispatch, sendQuery, state: { columnDefinitions, resultsNum, results, page, queryError } } = props;
  const [queryInput, setQueryInput] = useSessionStorage("query.input", "SELECT * FROM calloff.messages;");

  const setPage = useCallback((page: number) => dispatch({ type: ActionType.ON_SET_PAGE, page }), [dispatch]);

  const doQuery = useCallback(() => {
    console.log("query:", queryInput);
    setPage(0);
    dispatch({ type: ActionType.ON_CLEAR_RESULTS });
    dispatch({ type: ActionType.ON_START_QUERY });
    sendQuery(queryInput);
  }, [setPage, dispatch, sendQuery, queryInput]);

  return <Container>
    <TopSection>
      <QueryInputContainer>
        <QueryInput
          value={queryInput}
          onChange={ev => setQueryInput(ev.target.value)}
        />
        <Button onClick={() => doQuery()}>Execute</Button>
        <ResultCounter>{resultsNum !== null && <span>{resultsNum} results</span>}</ResultCounter>
      </QueryInputContainer>
      <PageSelect>
        {results.map((p, i) =>
          <PageLink key={"page-" + i} selected={page === i} onClick={() => setPage(i)}>{i+1}</PageLink>)
        }
      </PageSelect>
    </TopSection>
    <QueryResultsSection
      queryError={queryError}
      columnDefs={columnDefinitions}
      page={results.length > 0 ? results[page] : { rows: [] }}
    />
  </Container>
}

export default QuerySection;

const Container = styled.div`
  overflow: scroll;
`;

const TOP_HEIGHT = "80px";

const TopSection = styled.div`
  position: sticky;
  top: 0;
  left: 0;
  display: flex;
  flex-direction: column;
  height: ${TOP_HEIGHT};
  padding: 2px;
  background-color: #fff;
`;

const QueryInputContainer = styled.div`
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

const ResultCounter = styled.span`
  font-size: 12pt;
  margin-left: 50px;
`;

const PageSelect = styled.div`
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
    selected ? "text-decoration: none; font-size: 12pt" : "text-decoration: underline;"
  };
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
  position: sticky;
  top: ${TOP_HEIGHT};
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



