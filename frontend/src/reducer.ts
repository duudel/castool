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
}

export const initialState: State = {
  columnDefinitions: [],
  results: [],
  resultsNum: null,
  queryError: null,
  queryStatus: QueryStatus.Done,
  page: 0,
};

export enum ActionType {
  ON_CLEAR_RESULTS,
  ON_START_QUERY,
  ON_MESSAGE,
  ON_SET_PAGE,
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

export type Action = OnClearResultsAction | OnStartQueryAction | OnMessageAction | OnSetPageAction;

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
  }
  return state;
};

