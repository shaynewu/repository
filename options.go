package repository

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"strings"
)

type Option interface {
	Sql(db *gorm.DB) *gorm.DB
}

type limitOption struct {
	offset int
	limit  int
}

// create a limitOption
func Limit(offset, limit int) *limitOption {
	return &limitOption{offset, limit}
}

// create a limitOption
func Limit32(offset, limit int32) *limitOption {
	return &limitOption{int(offset), int(limit)}
}

func (lo *limitOption) Sql(db *gorm.DB) *gorm.DB {
	return db.Offset(lo.offset).Limit(lo.limit)
}

type orderOption struct {
	field FieldInterface
	order ORDER
}

// ParseOrderOption parse string to orderOption, case insensitive
//
// CREATE_TIME -> create_time asc
//
// CREATE_TIME_DESC -> create_time desc
//
// CREATE_TIME DESC -> create_time desc
//
// CREATE_TIME_ASC -> create_time asc
//
// CREATE_TIME ASC -> create_time asc
func ParseOrderOption(str string) Option {
	str = strings.ToLower(str)
	if strings.HasSuffix(str, "desc") {
		return SimpleField(str[:len(str)-5]).Desc()
	} else if strings.HasSuffix(str, "asc") {
		return SimpleField(str[:len(str)-4]).Asc()
	}
	return SimpleField(str).Asc()
}

func (oo *orderOption) Sql(db *gorm.DB) *gorm.DB {
	return db.Order(fmt.Sprintf("%s %s", oo.field, oo.order.String()))
}

type selectOption struct {
	columns []FieldInterface
}

func (so *selectOption) Sql(db *gorm.DB) *gorm.DB {
	var cols []string
	for _, c := range so.columns {
		cols = append(cols, c.Column())
	}
	return db.Select(cols)
}

func Select(cols ...FieldInterface) *selectOption {
	return &selectOption{
		columns: cols,
	}
}
