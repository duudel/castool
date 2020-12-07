import React from 'react';
import styled from 'styled-components';
import { Dispatch, useCallback, useEffect, useState } from "react";

import { ColumnDefinition, ColumnValue, ResultRow, ResultPage } from './types';
import { Action, ActionType, State } from './reducer';

import { JsonSyntaxHighlight } from './json-syntax/JsonSyntaxHighlight';

import useSessionStorage from './utils/UseSessionStorageHook';

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
  } else if (column.Timestamp !== undefined) {
    return <TextValue>{column.Timestamp.v}</TextValue>;
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
      <RowNumber>{row.index}</RowNumber>
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
          {page.rows.map((row, i) => renderRow(row, i))}
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

  forwardRef: { current: HTMLDivElement | null };
}

function QuerySection(props: QuerySectionProps) {
  const { forwardRef, dispatch, sendQuery, state: { columnDefinitions, resultsNum, results, page, queryError } } = props;
  const [queryInput, setQueryInput] = useSessionStorage("query.input", "SELECT * FROM calloff.messages;");

  const setPage = useCallback((page: number) => dispatch({ type: ActionType.ON_SET_PAGE, page }), [dispatch]);

  const doQuery = useCallback(() => {
    console.log("query:", queryInput);
    setPage(0);
    dispatch({ type: ActionType.ON_CLEAR_RESULTS });
    dispatch({ type: ActionType.ON_START_QUERY });
    sendQuery(queryInput);
  }, [setPage, dispatch, sendQuery, queryInput]);

  return <Container ref={forwardRef}>
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



