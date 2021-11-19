package repository

import (
	"database/sql/driver"
	"fmt"
	"github.com/jinzhu/copier"
	"github.com/lib/pq"
	"reflect"
	"strings"
)

type operator string

const (
	c_Eq            = "=?"
	c_NotEq         = "<>?"
	c_IsNull        = "IS NULL"
	c_NotNull       = "IS NOT NULL"
	c_Empty         = "=''"
	c_Lt            = "<?"
	c_Lte           = "<=?"
	c_Gt            = ">?"
	c_Gte           = ">=?"
	c_In            = "IN (?)"
	c_NotIn         = "NOT IN (?)"
	c_ArrayMatchAny = "&& (?)"
	c_Between       = "BETWEEN ? AND ?"
	c_Like          = "LIKE ?"
	c_ILike         = "ILIKE ?"
	c_StartsWith    = "LIKE ?"
	c_IStartsWith   = "ILIKE ?"
	c_Contains      = "LIKE ?"
	c_IContains     = "ILIKE ?"
	c_NotLike       = "NOT LIKE ?"
	c_NotILike      = "NOT ILIKE ?"
	c_Raw           = "RAW"
)

func (op operator) ParamCount() int {
	switch op {
	case c_IsNull, c_NotNull, c_Empty, c_Raw:
		return 0
	case c_Between:
		return 2
	default:
		return 1
	}
}

// 1:ASC, -1:DESC
type ORDER int

const (
	ASC  ORDER = 1
	DESC ORDER = -1
)

func (o ORDER) String() string {
	if o < 0 {
		return "DESC"
	}
	return "ASC"
}

type FieldInterface interface {
	// db column name
	Column() string

	// field = ?
	Eq(val interface{}) Condition

	// field <> ?
	NotEq(val interface{}) Condition

	// field is null
	IsNull() Condition

	// field is not null
	NotNull() Condition

	// field = ''
	//
	// column type must be varchar, text
	Empty() Condition

	// field < ?
	Lt(val interface{}) Condition

	// field <= ?
	Lte(val interface{}) Condition

	// field > ?
	Gt(val interface{}) Condition

	// field >= ?
	Gte(val interface{}) Condition

	// field in (?)
	//
	// val should be array or slice
	In(val interface{}) Condition

	// field not in(?)
	//
	// val should be array or slice
	NotIn(val interface{}) Condition

	// field && (?)
	//
	// val should be array or slice
	ArrayMatchAny(val interface{}) Condition

	// field between ? and ?
	Between(val1, val2 interface{}) Condition

	// field like ?
	Like(val string) Condition
	// field ilike ? （ignore uppercase lowercase）
	ILike(val string) Condition
	// field like ?%
	StartsWith(val string) Condition
	// field ilike ?%（ignore uppercase lowercase）
	IStartsWith(val string) Condition
	// field like %?%
	Contains(val string) Condition
	// field ilike %?% （ignore uppercase lowercase）
	IContains(val string) Condition
	// field not like ?
	NotLike(val string) Condition
	// field not ilike ? （ignore uppercase lowercase）
	NotILike(val string) Condition

	// Raw(val interface{}) Condition

	// field ASC
	Asc() Option

	// field DESC
	Desc() Option
}

type SimpleField string

const _Id = SimpleField("id")

// implements hint
var _ FieldInterface = (*SimpleField)(nil)

func (s SimpleField) Column() string {
	return string(s)
}

func (s SimpleField) Eq(val interface{}) Condition {
	return &singleCondition{
		field:   s,
		op:      c_Eq,
		sqlArg1: val,
		rawVal1: val,
	}
}

func (s SimpleField) NotEq(val interface{}) Condition {
	return &singleCondition{
		field:   s,
		op:      c_NotEq,
		sqlArg1: val,
		rawVal1: val,
	}
}

func (s SimpleField) IsNull() Condition {
	return &singleCondition{
		field: s,
		op:    c_IsNull,
	}
}

func (s SimpleField) NotNull() Condition {
	return &singleCondition{
		field: s,
		op:    c_NotNull,
	}
}

func (s SimpleField) Empty() Condition {
	return &singleCondition{
		field: s,
		op:    c_Empty,
	}
}

func (s SimpleField) Lt(val interface{}) Condition {
	return &singleCondition{
		field:   s,
		op:      c_Lt,
		sqlArg1: val,
		rawVal1: val,
	}
}

func (s SimpleField) Lte(val interface{}) Condition {
	return &singleCondition{
		field:   s,
		op:      c_Lte,
		sqlArg1: val,
		rawVal1: val,
	}
}

func (s SimpleField) Gt(val interface{}) Condition {
	return &singleCondition{
		field:   s,
		op:      c_Gt,
		sqlArg1: val,
		rawVal1: val,
	}
}

func (s SimpleField) Gte(val interface{}) Condition {
	return &singleCondition{
		field:   s,
		op:      c_Gte,
		sqlArg1: val,
		rawVal1: val,
	}
}

// val should be array or slice
func asArray(val interface{}) interface{} {
	if _, ok := val.(driver.Valuer); ok {
		// already driver Value, e.g. pq.StringArray
		return val
	}
	switch val.(type) {
	case []int32, *[]int32, []int, *[]int:
		var i64a []int64
		_ = copier.Copy(&i64a, val)
		return pq.Array(&i64a) // todo 这地方有问题
	default:
		return pq.Array(val) // todo 这地方有问题
	}
}

// val should be array or slice
func (s SimpleField) In(val interface{}) Condition {
	if !IsArray(val) {
		panic("param for In should be array or slice")
	}
	return &singleCondition{
		field:   s,
		op:      c_In,
		sqlArg1: val,
		rawVal1: val,
	}
}

// val should be array or slice
func (s SimpleField) NotIn(val interface{}) Condition {
	if !IsArray(val) {
		panic("param for NotIn should be array or slice")
	}
	return &singleCondition{
		field:   s,
		op:      c_NotIn,
		sqlArg1: val,
		rawVal1: val,
	}
}

func IsArray(val interface{}) bool {
	rt := reflect.TypeOf(val)
	switch rt.Kind() {
	case reflect.Slice:
		return true
	case reflect.Array:
		return true
	default:
		return false
	}

}

// val should be array or slice
func (s SimpleField) ArrayMatchAny(val interface{}) Condition {
	if !IsArray(val) {
		panic("param for ArrayMatchAny should be array or slice")
	}
	return &singleCondition{
		field:   s,
		op:      c_ArrayMatchAny,
		sqlArg1: asArray(val),
		rawVal1: val,
	}
}

func (s SimpleField) Between(val1, val2 interface{}) Condition {
	return &singleCondition{
		field:   s,
		op:      c_Between,
		sqlArg1: val1,
		sqlArg2: val2,
		rawVal1: val1,
		rawVal2: val2,
	}
}

func (s SimpleField) Like(val string) Condition {
	return &singleCondition{
		field:   s,
		op:      c_Like,
		sqlArg1: val,
		rawVal1: val,
	}
}
func (s SimpleField) ILike(val string) Condition {
	return &singleCondition{
		field:   s,
		op:      c_ILike,
		sqlArg1: val,
		rawVal1: val,
	}
}
func (s SimpleField) StartsWith(val string) Condition {
	return &singleCondition{
		field:   s,
		op:      c_StartsWith,
		sqlArg1: val + "%",
		rawVal1: val,
	}
}
func (s SimpleField) IStartsWith(val string) Condition {
	return &singleCondition{
		field:   s,
		op:      c_IStartsWith,
		sqlArg1: val + "%",
		rawVal1: val,
	}
}
func (s SimpleField) Contains(val string) Condition {
	return &singleCondition{
		field:   s,
		op:      c_Contains,
		sqlArg1: "%" + val + "%",
		rawVal1: val,
	}
}
func (s SimpleField) IContains(val string) Condition {
	return &singleCondition{
		field:   s,
		op:      c_IContains,
		sqlArg1: "%" + val + "%",
		rawVal1: val,
	}
}
func (s SimpleField) NotLike(val string) Condition {
	return &singleCondition{
		field:   s,
		op:      c_NotLike,
		sqlArg1: val,
		rawVal1: val,
	}
}
func (s SimpleField) NotILike(val string) Condition {
	return &singleCondition{
		field:   s,
		op:      c_NotILike,
		sqlArg1: val,
		rawVal1: val,
	}
}

//func (s SimpleField) Raw(val interface{}) Condition {
//	return &singleCondition{
//		field:   s,
//		op:      c_Raw,
//		sqlArg1: val,
//		rawVal1: val,
//	}
//}

func (s SimpleField) Asc() Option {
	return &orderOption{
		field: s,
		order: ASC,
	}
}

func (s SimpleField) Desc() Option {
	return &orderOption{
		field: s,
		order: DESC,
	}
}

type reduceFieldImpl struct {
	FieldInterface
	reduceFmt string
}

func (rf *reduceFieldImpl) Column() string {
	return fmt.Sprintf(rf.reduceFmt, rf.FieldInterface.Column())
}

func Distinct(flds ...FieldInterface) FieldInterface {
	var cols []string
	for _, f := range flds {
		cols = append(cols, f.Column())
	}
	return &reduceFieldImpl{
		FieldInterface: SimpleField(strings.Join(cols, ",")),
		reduceFmt:      "DISTINCT %s",
	}
}

func Sum(fi FieldInterface) FieldInterface {
	return &reduceFieldImpl{
		FieldInterface: fi,
		reduceFmt:      "SUM(%s)",
	}
}

func Max(fi FieldInterface) FieldInterface {
	return &reduceFieldImpl{
		FieldInterface: fi,
		reduceFmt:      "MAX(%s)",
	}
}

func Min(fi FieldInterface) FieldInterface {
	return &reduceFieldImpl{
		FieldInterface: fi,
		reduceFmt:      "MIN(%s)",
	}
}

func Avg(fi FieldInterface) FieldInterface {
	return &reduceFieldImpl{
		FieldInterface: fi,
		reduceFmt:      "AVG(%s)",
	}
}
