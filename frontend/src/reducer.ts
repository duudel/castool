import { ColumnDefinition, ResultRow, ResultPage } from './types';

export enum QueryStatus {
  Done,
  InProgress
}

interface TxResultPage {
  results: any[];
}

export interface State {
  columnDefinitions: ColumnDefinition[];
  results: ResultPage[];
  resultsNum: number | null;
  queryError: string | null;
  queryStatus: QueryStatus;
  page: number;

  tx: {
    pages: TxResultPage[];
    page: number;
  };
}

export const initialState: State = {
  columnDefinitions: [],
  results: [],
  resultsNum: null,
  queryError: null,
  queryStatus: QueryStatus.Done,
  page: 0,

  tx: {
    pages: [],
    page: 0,
  }
};

export enum ActionType {
  ON_CLEAR_RESULTS,
  ON_START_QUERY,
  ON_MESSAGE,
  ON_SET_PAGE,

  ON_TX_RESULTS_CLEAR,
  ON_TX_RESULTS,
};

export interface OnClearResultsAction {
  type: ActionType.ON_CLEAR_RESULTS;
}

export interface OnStartQueryAction {
  type: ActionType.ON_START_QUERY;
}

export interface OnMessageAction {
  type: ActionType.ON_MESSAGE;
  message: any;
}

export interface OnSetPageAction {
  type: ActionType.ON_SET_PAGE;
  page: number;
}

export interface OnTxResultsClear {
  type: ActionType.ON_TX_RESULTS_CLEAR;
}

export interface OnTxResults {
  type: ActionType.ON_TX_RESULTS;
  results: any[];
}

export type Action = OnClearResultsAction | OnStartQueryAction | OnMessageAction | OnSetPageAction | OnTxResultsClear | OnTxResults;

export const clearTxResults: Action = { type: ActionType.ON_TX_RESULTS_CLEAR };
export const addTxResults = (results: any[]): Action => ({ type: ActionType.ON_TX_RESULTS, results });

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
      const page = {
        rows: rowsMessage.rows
      };
      const resultsNum = (state.resultsNum || 0) + page.rows.length;
      return { ...state, results: state.results.concat([page]), resultsNum };
    }
    case "QueryMessageFinished": {
      return { ...state, queryStatus: QueryStatus.Done };
    }
  }
  return state;
}

export const reducer = (state: State, action: Action) => {
  switch (action.type) {
    case ActionType.ON_CLEAR_RESULTS: {
      return { ...state, results: [], resultsNum: null, queryError: null };
    }
    case ActionType.ON_START_QUERY: {
      return { ...state, queryStatus: QueryStatus.InProgress, resultsNum: 0 };
    }
    case ActionType.ON_MESSAGE: {
      const { message } = action;
      const m = parseMessage(message);
      if (m === undefined) throw Error("Invalid query message: " + message);
      return handleMessage(state, m);
    }
    case ActionType.ON_SET_PAGE: {
      const { page } = action;
      return { ...state, page };
    }
    case ActionType.ON_TX_RESULTS_CLEAR: {
      return { ...state, tx: { pages: [], page: 0 } };
    }
    case ActionType.ON_TX_RESULTS: {
      const { results } = action;
      const pages = state.tx.pages;
      if (pages.length === 0) {
        pages.push({ results: [] });
      }
      const page = pages[0];
      page.results = page.results.concat(results);
      //const PAGE_SIZE = 50;
      //let pages = state.tx.pages;
      //if (pages.length === 0) {
      //  pages = [{ results: [] }];
      //}
      //while (results.length > 0) {
      //  const i = pages.length - 1;
      //  let p = pages[i];
      //  if (PAGE_SIZE - p.results.length <= 0) {
      //    pages = pages.concat([{ results: [] }]);
      //    p = pages[i + 1];
      //  }
      //  const max = PAGE_SIZE - p.results.length;
      //  const howMany = results.length > max ? max : results.length;
      //  p.results.concat(results.splice(0, howMany))
      //}
      console.log("pages", pages);
      return { ...state, tx: { pages, page: 0 } };
    }
  }
  return state;
};

