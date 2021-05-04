
//export type ValueType = RqlValueType;
export { RqlValueType as ValueType } from "./RqlValue";
export type { RqlValue as Value } from "./RqlValue";
export type { RqlBool as Bool } from "./RqlValue";
export type { RqlNull as Null } from "./RqlValue";
export type { RqlNum as Num } from "./RqlValue";
export type { RqlDate as Date } from "./RqlValue";
export type { RqlStr as Str } from "./RqlValue";
export type { RqlObj as Obj } from "./RqlValue";

export { is_date } from "./RqlValue";
export { is_object } from "./RqlValue";
export { type_of } from "./RqlValue";
export { fold_value } from "./RqlValue";
export type { FoldHandler } from "./RqlValue";

