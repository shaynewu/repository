package repository

import (
	"context"
	"errors"
	"github.com/jinzhu/gorm"
	"go.uber.org/zap"
	"reflect"
	"time"
)

var dbNilErr = errors.New("db is nil")

type Model interface {
	TableName() string
}

type Repository struct {
	Tm         TransactionManager
	Value      Model
	CreateFunc func(ctx context.Context, model Model) (err error)
	SaveFunc   func(ctx context.Context, model Model) (err error)
	UpdateFunc func(ctx context.Context, update interface{}, condition Condition) (err error)
	DeleteFunc func(ctx context.Context, condition Condition) (err error)
	// MandatoryCondition 是固有的 where 条件, 通常用来过滤 is_delete=0 的数据(软删除逻辑, 外界可以不用感知, 且每个 where 条件都有)
	MandatoryCondition Condition
}

// implements hint
var _ RepositoryInterface = (*Repository)(nil)

func (e *Repository) SetCreateFunc(fn func(context.Context, Model) error) {
	e.CreateFunc = fn
}

func (e *Repository) SetSaveFunc(fn func(context.Context, Model) error) {
	e.SaveFunc = fn
}

func (e *Repository) SetUpdateFunc(fn func(context.Context, interface{}, Condition) error) {
	e.UpdateFunc = fn
}

func (e *Repository) SetDeleteFunc(fn func(context.Context, Condition) error) {
	e.DeleteFunc = fn
}
func (e *Repository) GetCreateFunc() (fn func(context.Context, Model) error) {
	return e.CreateFunc
}

func (e *Repository) GetSaveFunc() (fn func(context.Context, Model) error) {
	return e.SaveFunc
}

func (e *Repository) GetUpdateFunc() (fn func(context.Context, interface{}, Condition) error) {
	return e.UpdateFunc
}

func (e *Repository) GetDeleteWithOptionsFunc() (fn func(context.Context, Condition) error) {
	return e.DeleteFunc
}

// InitRepoFields set every Field in fieldsStructPtr with a SimpleField
func (e *Repository) InitRepoFields(fieldsStructPtr interface{}) {
	fv := reflect.ValueOf(fieldsStructPtr).Elem()
	for _, gf := range (&gorm.Scope{}).New(e.Value).Fields() {
		f := fv.FieldByName(gf.Name)
		if f.CanSet() {
			f.Set(reflect.ValueOf(SimpleField(gf.DBName)))
		}
	}
}

type RepositoryInterface interface {
	// Get inner transaction manager
	GetTM() TransactionManager

	FindOne(context.Context, Condition) (Model, error)
	FindById(ctx context.Context, id interface{}) (Model, error)
	// additional will be AND as MatchAll (use to filter status etc.)
	FindByIds(ctx context.Context, ids interface{}, additional ...Condition) (interface{}, error)
	Find(ctx context.Context, condition Condition, options ...Option) (interface{}, error)
	FindAndCount(ctx context.Context, condition Condition, options ...Option) (interface{}, int, error)
	Count(ctx context.Context, condition Condition) (int, error)
	Create(ctx context.Context, model Model) error

	// update when PK has value, or create when PK is zero
	Save(ctx context.Context, model Model) error
	Update(ctx context.Context, update interface{}, condition Condition) error

	// support soft delete
	Delete(ctx context.Context, condition Condition) error
	// support soft delete
	DeleteById(ctx context.Context, id interface{}) error

	SetCreateFunc(func(context.Context, Model) error)
	SetSaveFunc(func(context.Context, Model) error)
	SetUpdateFunc(func(context.Context, interface{}, Condition) error)
	SetDeleteFunc(func(context.Context, Condition) error)
}

func NewRepository(model Model) *Repository {
	repo0 := &Repository{
		Value: model,
	}
	repo0.Tm = NewTransactionManager("", "")
	repo0.SetCreateFunc(func(ctx context.Context, data Model) error {
		db := repo0.Tm.GetDb(ctx)
		if db == nil {
			return dbNilErr
		}
		es := &execScope{
			model: data,
			scope: db.NewScope(data),
			rep:   repo0,
		}
		if err := es.beforeRepoCreateCallback(ctx, data); err != nil {
			return err
		}
		err := db.Create(data).Error
		if err != nil {
			return err
		}
		return es.afterRepoCreateCallback(ctx, data)
	})

	repo0.SetSaveFunc(func(ctx context.Context, data Model) error {
		db := repo0.Tm.GetDb(ctx)
		if db == nil {
			return dbNilErr
		}
		es := &execScope{
			model: data,
			scope: db.NewScope(data),
			rep:   repo0,
		}
		if es.scope.PrimaryKeyZero() {
			if err := es.beforeRepoCreateCallback(ctx, data); err != nil {
				return err
			}
			if err := db.Create(data).Error; err != nil {
				return err
			}
			return es.afterRepoCreateCallback(ctx, data)
		}

		if err := es.beforeRepoUpdateCallback(ctx, data); err != nil {
			return err
		}
		if err := db.Model(repo0.NewStruct()).Updates(data).Error; err != nil {
			return err
		}
		return es.afterRepoUpdateCallback(ctx, data)
	})

	repo0.SetUpdateFunc(func(ctx context.Context, update interface{}, condition Condition) error {
		query := repo0.parseWhere(ctx, condition)
		if query == nil {
			return dbNilErr
		}
		es := &execScope{
			model: update,
			scope: (&gorm.Scope{}).New(update),
			rep:   repo0,
		}
		if err := es.beforeRepoUpdateCallback(ctx, update); err != nil {
			return err
		}
		return query.Model(repo0.NewStruct()).Updates(update).Error
	})
	if _, ok := model.(SoftDeleteHook); ok {
		repo0.SetDeleteFunc(func(ctx context.Context, condition Condition) error {
			query := repo0.parseWhere(ctx, condition)
			if query == nil {
				return dbNilErr
			}
			val := repo0.NewStruct()
			err := (val.(SoftDeleteHook)).BeforeSoftDelete(ctx)
			if err != nil {
				return err
			}
			return query.Model(val).Updates(val).Error
		})
	} else {
		repo0.SetDeleteFunc(func(ctx context.Context, condition Condition) error {
			query := repo0.parseWhere(ctx, condition)
			if query == nil {
				return dbNilErr
			}
			return query.Delete(repo0.NewStruct()).Error
		})
	}

	return repo0
}

func ParseWhere(condition Condition, db *gorm.DB) *gorm.DB {
	if db == nil {
		return nil
	}
	sql, args := condition.flatten()
	if sql == "" {
		// no where clause
		return db
	}
	return db.Where(sql, args...)
}

func (e *Repository) parseWhere(ctx context.Context, condition Condition) *gorm.DB {
	if e.MandatoryCondition != nil {
		condition = condition.And(e.MandatoryCondition)
	}
	return ParseWhere(condition, e.Tm.GetDb(ctx))
}

func (e *Repository) parseOptions(ctx context.Context, db *gorm.DB, options ...Option) *gorm.DB {
	for _, opt := range options {
		db = opt.Sql(db)
	}
	return db
}

func (e *Repository) GetTM() TransactionManager {
	return e.Tm
}

func (e *Repository) FindOne(ctx context.Context, condition Condition) (data Model, err error) {

	startTime := time.Now()
	defer func() {
		s, _ := condition.flatten()
		Info("[loadlog][sql] FindOne", zap.Any("key", ctx.Value("key")), zap.String("table", e.Value.TableName()), zap.String("condition", s), zap.Int64("request_time", time.Since(startTime).Milliseconds()))
	}()

	data = e.NewStruct().(Model)
	db := e.parseWhere(ctx, condition)
	if db == nil {
		return nil, dbNilErr
	}

	err = db.Take(data).Error
	if err != nil {
		return nil, err
	}
	return
}

func (e *Repository) Count(ctx context.Context, condition Condition) (total int, err error) {
	query := e.parseWhere(ctx, condition)
	if query == nil {
		return 0, nil
	}
	err = query.Model(e.NewStruct()).Count(&total).Error
	return

}
func (e *Repository) FindAndCount(ctx context.Context, condition Condition, options ...Option) (slice interface{}, total int, err error) {
	total, err = e.Count(ctx, condition)
	if err != nil {
		return
	}
	if total == 0 {
		slice = e.NewSlice()
		return
	}
	slice, err = e.Find(ctx, condition, options...)
	return
}

func (e *Repository) Find(ctx context.Context, condition Condition, options ...Option) (slice interface{}, err error) {

	startTime := time.Now()
	defer func() {
		s, _ := condition.flatten()
		Info("[loadlog][sql] Find", zap.Any("key", ctx.Value("key")), zap.String("table", e.Value.TableName()), zap.String("condition", s), zap.Int64("request_time", time.Since(startTime).Milliseconds()))
	}()

	slice = e.NewSlice()
	query := e.parseWhere(ctx, condition)
	if query == nil {
		return
	}
	query = e.parseOptions(ctx, query, options...)
	err = query.Find(slice).Error
	return
}

func (e *Repository) FindById(ctx context.Context, id interface{}) (data Model, err error) {
	data, err = e.FindOne(ctx, _Id.Eq(id))
	return
}

func (e *Repository) Save(ctx context.Context, model Model) error {
	return e.SaveFunc(ctx, model)
}

func (e Repository) Create(ctx context.Context, model Model) error {
	startTime := time.Now()
	defer func() {
		Info("[loadlog][sql] Create", zap.Any("key", ctx.Value("key")), zap.String("table", e.Value.TableName()), zap.Any("model", model), zap.Int64("request_time", time.Since(startTime).Milliseconds()))
	}()
	return e.CreateFunc(ctx, model)
}

func (e *Repository) Update(ctx context.Context, update interface{}, condition Condition) error {
	return e.UpdateFunc(ctx, update, condition)
}

func (e *Repository) Delete(ctx context.Context, condition Condition) error {
	// s, _ := condition.flatten()
	// if s == "" {
	// return errors.New("delete without condition is not allowed")
	// }
	// gorm 默认会阻止 没有 where 条件的 update 和 delete
	return e.DeleteFunc(ctx, condition)
}

func (e *Repository) DeleteById(ctx context.Context, id interface{}) (err error) {
	val := e.NewStruct()
	if sdi, ok := val.(SoftDeleteHook); ok {
		return e.softDeleteById(ctx, id, sdi)
	}
	return e.DeleteFunc(ctx, _Id.Eq(id))
}

func (e *Repository) softDeleteById(ctx context.Context, id interface{}, model SoftDeleteHook) (err error) {
	db := e.Tm.GetDb(ctx)
	if f, ok := db.NewScope(model).FieldByName("id"); ok {
		err = f.Set(id)
		if err != nil {
			return
		}
	}
	err = model.BeforeSoftDelete(ctx)
	if err != nil {
		return
	}
	err = db.Model(e.NewStruct()).Where("id=?", id).Updates(model).Error
	if err != nil {
		return
	}
	return model.AfterSoftDelete(ctx)
}

func (e *Repository) FindByIds(ctx context.Context, ids interface{}, additional ...Condition) (data interface{}, err error) {
	if reflect.ValueOf(ids).Len() == 0 {
		return e.NewSlice(), nil
	}
	if len(additional) > 0 {
		data, err = e.Find(ctx, _Id.In(ids).And(MatchAll(additional...)))
	} else {
		data, err = e.Find(ctx, _Id.In(ids))
	}
	return
}

// gorm.IsRecordNotFoundError
func IsRecordNotFound(err error) bool {
	return err != nil && gorm.IsRecordNotFoundError(err)
}

// NewStruct initialize a struct for the Model
func (e *Repository) NewStruct() interface{} {
	return NewStruct(e.Value)
}

// NewSlice initialize a slice of struct for the Model
func (e *Repository) NewSlice() interface{} {
	return NewSlice(e.Value)
}

func NewSlice(model interface{}) interface{} {
	if model == nil {
		return nil
	}
	sliceType := reflect.SliceOf(reflect.TypeOf(model))
	slice := reflect.MakeSlice(sliceType, 0, 0)
	slicePtr := reflect.New(sliceType)
	slicePtr.Elem().Set(slice)
	return slicePtr.Interface()
}

func NewStruct(model interface{}) interface{} {
	if model == nil {
		return nil
	}
	return reflect.New(Indirect(reflect.ValueOf(model)).Type()).Interface()
}

// Indirect returns last value that v points to
func Indirect(v reflect.Value) reflect.Value {
	for v.Kind() == reflect.Ptr {
		v = reflect.Indirect(v)
	}
	return v
}
