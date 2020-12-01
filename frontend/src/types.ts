
export type ColumnValue_Null = {
  Null: { };
};

export type ColumnValue_Ascii = {
  Ascii: { };
};

export type ColumnValue_BigInt = {
  BigInt: { v: number; };
};

export type ColumnValue_Blob = {
  Blob: { v: string; };
};

export type ColumnValue_Text = {
  Text: { v: string; };
};

export type ColumnValue_Integer = {
  Integer: { v: number; };
};

export type ColumnValue_Timestamp = {
  Timestamp: { v: string; };
};

export type ColumnValue = ColumnValue_Null
  | ColumnValue_Ascii
  | ColumnValue_BigInt
  | ColumnValue_Blob
  | ColumnValue_Integer
  | ColumnValue_Text
  | ColumnValue_Timestamp
  | any;

export enum ColumnValueDataType {
  Ascii = "Ascii",
  BigInt = "BigInt",
  Blob = "Blob",
  Bool = "Bool",
  Counter = "Counter",
  Decimal = "Decimal",
  Double = "Double",
  Float = "Float",
  Integer = "Integer",
  SmallInt = "SmallInt",
  TinyInt = "TinyInt",
  Timestamp = "Timestamp",
  Uuid = "Uuid",
  VarInt = "VarInt",
  TimeUuid = "TimeUuid",
  Text = "Text",
  Inet = "Inet",
  Date = "Date",
  Time = "Time",
  Duration = "Duration",
}

export interface ColumnDefinition {
  name: string;
  dataType: ColumnValueDataType;
}

export interface ResultRow {
  index: number;
  columnValues: ColumnValue[];
}

export interface ResultPage {
  rows: ResultRow[];
}

