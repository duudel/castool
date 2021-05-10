
export type ColumnValue = any;

export enum ColumnValueDataTypeCode {
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
  List = "List",
  Set = "Set",
  Map = "Map",
}

const PrimitiveTypeCodes = {
  Ascii: "Ascii",
  BigInt: "BigInt",
  Blob: "Blob",
  Bool: "Bool",
  Counter: "Counter",
  Decimal: "Decimal",
  Double: "Double",
  Float: "Float",
  Integer: "Integer",
  SmallInt: "SmallInt",
  TinyInt: "TinyInt",
  Timestamp: "Timestamp",
  Uuid: "Uuid",
  VarInt: "VarInt",
  TimeUuid: "TimeUuid",
  Text: "Text",
  Inet: "Inet",
  Date: "Date",
  Time: "Time",
  Duration: "Duration",
};

interface PrimitiveType {
  //code: ColumnValueDataTypeCode;
  code: keyof typeof PrimitiveTypeCodes;
}
interface ListType {
  code: ColumnValueDataTypeCode.List;
  elem: ColumnValueDataType;
}
interface SetType {
  code: ColumnValueDataTypeCode.Set;
  elem: ColumnValueDataType;
}
interface MapType {
  code: ColumnValueDataTypeCode.Map;
  key: ColumnValueDataType;
  value: ColumnValueDataType;
}

export type ColumnValueDataType = PrimitiveType | ListType | SetType | MapType;

export function dataTypeToString(dataType: ColumnValueDataType): string {
  switch (dataType.code) {
    case ColumnValueDataTypeCode.List:
      return dataType.code + "<" + dataTypeToString(dataType.elem)  + ">";
    case ColumnValueDataTypeCode.Set:
      return dataType.code + "<" + dataTypeToString(dataType.elem)  + ">";
    case ColumnValueDataTypeCode.Map:
      return dataType.code + "<" + dataTypeToString(dataType.key) + "," + dataTypeToString(dataType.value)  + ">";
    default:
      return dataType.code;
  }
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

// Metadata

export enum ClusterinOrder {
  ASC = "ASC",
  DESC = "DESC",
}

export interface Table {
  id: string;
  name: string;
  columnDefs: ColumnDefinition[];
  partitionKey: string[];
  clusteringColumns: [string, ClusterinOrder][];
}

export interface Keyspace {
  name: string;
  replication: { class: string; replication_factor: string; };
  tables: Table[];
}

export interface Node {
  cassandraVersion: string;
  datacenter: string;
  endpoint: string;
  hostId: string;
  rack: string;
  schemaVersion: string;
}

export interface Metadata {
  clusterName: string;
  keyspaces: Keyspace[];
  nodes: Node[];
}

