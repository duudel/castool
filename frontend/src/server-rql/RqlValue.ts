
export enum RqlValueType {
  Null = "Null",
  Bool = "Bool",
  Num = "Num",
  Date = "Date",
  Str = "Str",
  List = "List",
  Obj = "Obj",
  Blob = "Blob",
}

export type RqlNull = null;
export type RqlBool = boolean;
export type RqlNum = number;
export type RqlDate = { date: string; };
export type RqlStr = string;
export type RqlList = RqlValue[];
export type RqlObj = {
  obj: {
    [key: string]: RqlValue;
  };
};
export type RqlBlob = { blob: string; };

export type RqlValue = RqlNull | RqlBool | RqlNum | RqlDate | RqlStr | RqlList | RqlObj | RqlBlob;

export function is_date(value: RqlValue): value is RqlDate {
  if (value === null) return false;
  return typeof value === "object" && "date" in value;
}

export function is_object(value: RqlValue): value is RqlObj {
  if (value === null) return false;
  return typeof value === "object" && "obj" in value;
}

export function is_blob(value: RqlValue): value is RqlBlob {
  if (value === null) return false;
  return typeof value === "object" && "blob" in value;
}

export function type_of(value: RqlValue): RqlValueType {
  if      (value === null)              return RqlValueType.Null;
  else if (typeof value === "boolean")  return RqlValueType.Bool;
  else if (typeof value === "number")   return RqlValueType.Num;
  else if (typeof value === "string")   return RqlValueType.Str;
  else if (is_date(value))              return RqlValueType.Date;
  else if (Array.isArray(value))        return RqlValueType.List;
  else if (is_object(value))            return RqlValueType.Obj;
  else if (is_blob(value))              return RqlValueType.Blob;

  return RqlValueType.Null;
}

export interface FoldHandler<T> {
  null(): T
  bool(x: boolean): T
  num(x: number): T
  str(x: string): T
  date(x: RqlDate): T
  list(x: RqlList): T
  obj(x: RqlObj): T
  blob(x: RqlBlob): T
}

export function fold_value<T>(handler: FoldHandler<T>) {
  return (value: RqlValue) => {
    if      (value === null)             return handler.null();
    else if (typeof value === "boolean") return handler.bool(value);
    else if (typeof value === "number")  return handler.num(value);
    else if (typeof value === "string")  return handler.str(value);
    else if (is_date(value))             return handler.date(value);
    else if (Array.isArray(value))       return handler.list(value);
    else if (is_object(value))           return handler.obj(value);
    else if (is_blob(value))             return handler.blob(value);

    return handler.null();
  };
}


