package repository

import (
	"reflect"
	"strings"
)

type Condition interface {
	And(Condition) Condition
	Or(Condition) Condition
	flatten() (sql string, args []interface{})
}

type logic string

const (
	and logic = "AND"
	or  logic = "OR"
)

type singleCondition struct {
	field   FieldInterface
	op      operator
	sqlArg1 interface{}
	sqlArg2 interface{}
	rawVal1 interface{}
	rawVal2 interface{}
}

func (sc *singleCondition) And(condition Condition) Condition {
	return &compoundCondition{
		condition1: sc,
		condition2: condition,
		logic:      and,
	}
}

func (sc *singleCondition) Or(condition Condition) Condition {
	return &compoundCondition{
		condition1: sc,
		condition2: condition,
		logic:      or,
	}
}

func (sc *singleCondition) flatten() (sql string, args []interface{}) {
	sql = sc.field.Column() + " " + string(sc.op)
	switch sc.op.ParamCount() {
	case 1:
		args = append(args, sc.sqlArg1)
	case 2:
		args = append(args, sc.sqlArg1, sc.sqlArg2)
	default:

	}
	return
}

type compoundCondition struct {
	condition1 Condition
	condition2 Condition
	logic      logic
}

func (cc *compoundCondition) And(condition Condition) Condition {
	return &compoundCondition{
		condition1: cc,
		condition2: condition,
		logic:      and,
	}
}

func (cc *compoundCondition) Or(condition Condition) Condition {
	return &compoundCondition{
		condition1: cc,
		condition2: condition,
		logic:      or,
	}
}

func (cc *compoundCondition) flatten() (sql string, args []interface{}) {
	sql1, args1 := cc.condition1.flatten()
	sql2, args2 := cc.condition2.flatten()
	if sql1 == "" {
		return sql2, args2
	}
	if sql2 == "" {
		return sql1, args1
	}
	sql = "(" + sql1 + ")" + string(cc.logic) + "(" + sql2 + ")"
	args = append(args, args1...)
	args = append(args, args2...)
	return
}

type conditionGroup struct {
	conditions []Condition
	logic      logic
}

func (cg *conditionGroup) And(condition Condition) Condition {
	return &compoundCondition{
		condition1: cg,
		condition2: condition,
		logic:      and,
	}
}

func (cg *conditionGroup) Or(condition Condition) Condition {
	return &compoundCondition{
		condition1: cg,
		condition2: condition,
		logic:      or,
	}
}

func (cg *conditionGroup) flatten() (sql string, args []interface{}) {
	var sqls []string
	for _, c := range cg.conditions {
		s, a := c.flatten()
		if s == "" {
			continue
		}
		if _, ok := c.(*singleCondition); !ok {
			s = "(" + s + ")"
		}
		sqls = append(sqls, s)
		args = append(args, a...)
	}
	sql = strings.Join(sqls, " "+string(cg.logic)+" ")
	return
}

func isZero(val interface{}) bool {
	vv := reflect.ValueOf(val)
	return vv.IsZero()
}

// conditionGroup 的直接子节点:
// 若其类型为 *singleCondition, 且 只有一个参数, 且参数值为 零 (reflect.IsZero), 则该 singleCondition 会被忽略.
// 若其类型为 *conditionGroup, 且, 内部conditions 为空, 则被忽略
func (cg *conditionGroup) IgnoreZero() *conditionGroup {
	var conds []Condition
	for _, c := range cg.conditions {
		if sc, ok := c.(*singleCondition); ok {
			if sc.op.ParamCount() == 1 && isZero(sc.rawVal1) {
				continue
			}
		}
		if cg, ok := c.(*conditionGroup); ok {
			if len(cg.conditions) == 0 {
				continue
			}
		}
		conds = append(conds, c)
	}
	return &conditionGroup{
		conditions: conds,
		logic:      cg.logic,
	}
}

func MatchAll(conditions ...Condition) *conditionGroup {
	return &conditionGroup{
		conditions: conditions,
		logic:      and,
	}
}

func MatchAny(conditions ...Condition) *conditionGroup {
	return &conditionGroup{
		conditions: conditions,
		logic:      or,
	}
}
