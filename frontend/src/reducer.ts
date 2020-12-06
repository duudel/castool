import { ColumnDefinition, ResultRow, ResultPage } from './types';

export enum QueryStatus {
  Done,
  InProgress
}

export interface State {
  columnDefinitions: ColumnDefinition[];
  results: ResultPage[];
  resultsNum: number | null;
  queryError: string | null;
  queryStatus: QueryStatus;
  page: number;

  tx: {
    results: any[];
    page: number;
    rowsPerPage: number;
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
    results: [],
    page: 0,
    rowsPerPage: 20,
  }
};

export enum ActionType {
  ON_CLEAR_RESULTS = "ON_CLEAR_RESULTS",
  ON_START_QUERY = "ON_START_QUERY",
  ON_MESSAGE = "ON_MESSAGE",
  ON_SET_PAGE = "ON_SET_PAGE",

  ON_TX_RESULTS_CLEAR = "ON_TX_RESULTS_CLEAR",
  ON_TX_RESULTS = "ON_TX_RESULTS",
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

export const reducer = (state: State, action: Action): State => {
  console.log("Action: ", action.type);
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
      return { ...state, tx: { ...state.tx, results: [], page: 0 } };
    }
    case ActionType.ON_TX_RESULTS: {
      const { results } = action;
      const prevResults = state.tx.results;
      const newResults = prevResults.concat(results);
      return { ...state, tx: { ...state.tx, results: newResults } };
    }
  }
  //return state;
};

