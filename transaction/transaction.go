package transaction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"go.uber.org/zap"
	"repository/client"
	"repository/logger"
	"sync"
)

// 定义个事务类型
// todo 思考连接池的情况, 后面还得改改

// 自定义类型, 防止外部直接通过 string 访问
type wrapContextStringKey string

type dbWrapper struct {
	db            *gorm.DB
	inTransaction bool
	err           error
}

func (dbw *dbWrapper) reset() {
	dbw.db = nil
	dbw.inTransaction = false
	dbw.err = nil
}

type TransactionManager interface {
	GetDb(ctx context.Context) *gorm.DB
	Transaction(ctx context.Context, doTransaction func(ctx context.Context) (res interface{}, err error)) (interface{}, error)
}

type transactionManager struct {
	serviceName     string
	database        string
	ctxDbWrapperKey wrapContextStringKey
}

var tmMap = make(map[string]*transactionManager)
var mutex sync.Mutex

//NewTransactionManager 获取(或新建)一个事务管理器, 每个 serviceName, database 组合共享一个实例
func NewTransactionManager(serviceName, database string) TransactionManager {
	mapKey := serviceName + ":" + database
	if tm, ok := tmMap[mapKey]; ok {
		return tm
	}
	mutex.Lock()
	defer mutex.Unlock()

	if tm, ok := tmMap[mapKey]; ok {
		return tm
	}

	tm := &transactionManager{
		serviceName:     serviceName,
		database:        database,
		ctxDbWrapperKey: wrapContextStringKey("dbWrapper:" + mapKey),
	}
	tmMap[mapKey] = tm

	return tm
}

func (tm *transactionManager) getDb() *gorm.DB {
	return client.GetDB(tm.database, tm.serviceName)
}

// GetDb 应当仅在 model 层调用, 用于获取数据库连接
//
// 优先从 ctx 获取已有的连接(可能已经开启了事务). 如果没有连接, 则新建一个没有开启事务的连接.
func (tm *transactionManager) GetDb(ctx context.Context) *gorm.DB {
	wrapper := tm.getDbWrapper(ctx)
	if wrapper != nil && wrapper.db != nil {
		return wrapper.db
	}
	db := tm.getDb()
	return db
}

func (tm *transactionManager) getDbWrapper(ctx context.Context) *dbWrapper {
	wrapper := ctx.Value(tm.ctxDbWrapperKey)
	if dbWrapper0, ok := wrapper.(*dbWrapper); ok {
		return dbWrapper0
	}
	return nil
}

func (tm *transactionManager) setDbWrapper(ctx context.Context, db *dbWrapper) context.Context {
	return context.WithValue(ctx, tm.ctxDbWrapperKey, db)
}

// Transaction 在事务中执行 doTransaction 方法, 如果当前 ctx 中没有已开启事务的连接, 则开启事务.
//
// doTransaction 方法 返回 error 或 panic(或其内嵌事务发生 error, panic), 则会自动rollback. 否则自动 commit
func (tm *transactionManager) Transaction(ctx context.Context, doTransaction func(ctx context.Context) (res interface{}, err error)) (res interface{}, err error) {
	// 是否是wrapper的开启方,如果是开启方,才可以提交事务
	txOpenByMe := false

	wrapper := tm.getDbWrapper(ctx)

	if wrapper == nil || wrapper.db == nil {
		db := tm.getDb()
		if db != nil {
			tx := db.BeginTx(ctx, &sql.TxOptions{})
			wrapper = &dbWrapper{
				db:            tx,
				inTransaction: true,
			}
			ctx = tm.setDbWrapper(ctx, wrapper)
			// 本方法开启的事务,由本方法提交
			txOpenByMe = true
		} else {
			return nil, errors.New("can not get db connection")
		}
	} else {
		if wrapper.err != nil {
			return nil, fmt.Errorf("transaction already has error:%w", wrapper.err)
		}
		if !wrapper.inTransaction {
			wrapper.db = wrapper.db.BeginTx(ctx, &sql.TxOptions{})
			wrapper.inTransaction = true
			txOpenByMe = true
		}
	}

	defer func() {
		if r := recover(); r != nil {
			err0, isErr := r.(error)
			if !isErr {
				err0 = fmt.Errorf("recover:%v", r)
			}
			err = err0
			logger.Error(ctx, "panic in Transaction", zap.Error(err))
			wrapper.err = err
			if !txOpenByMe {
				return
			}
			rberr := wrapper.db.Rollback().Error
			if rberr != nil {
				logger.Error(ctx, "rollback failed in recover", zap.Any("panic", r), zap.Error(rberr))
			}
			wrapper.reset()
		}
	}()

	if ctx.Err() != nil {
		// 可能由于获取链接时间比较长, timeout, 或 cancel 了,
		return nil, ctx.Err()
	}
	returnData, bizErr := doTransaction(ctx)
	if bizErr != nil {
		wrapper.err = bizErr
		logger.Error(ctx, "doTransaction err", zap.Error(bizErr))
	}
	if ctx.Err() != nil {
		// 执行完以后, context 已经超时或取消了, 不再 commit/rollback (其实已经rollback 了)
		return nil, ctx.Err()
	}
	if txOpenByMe {
		defer wrapper.reset()
		// 当前doTransaction方法 返回 error
		if bizErr != nil {
			rberr := wrapper.db.Rollback().Error
			if rberr != nil {
				logger.Error(ctx, "rollback failed", zap.NamedError("bizErr", bizErr), zap.Error(rberr))
			}
			return returnData, bizErr
		} else if wrapper.err != nil {
			// 如果执行当前doTransaction方法没有error, 但其内部嵌套的事务发生了错误, 且当前doTransaction方法没有返回这个 error, 同样要回滚
			logger.Error(ctx, "inner transaction has err, should rollback", zap.NamedError("bizErr", wrapper.err))
			rberr := wrapper.db.Rollback().Error
			if rberr != nil {
				logger.Error(ctx, "rollback for inner transaction failed", zap.NamedError("bizErr", wrapper.err), zap.Error(rberr))
			}
			return returnData, wrapper.err
		}

		commitError := wrapper.db.Commit().Error
		if commitError != nil {
			logger.Error(ctx, "commit failed", zap.Error(commitError))
		}
		return returnData, commitError
	}

	return returnData, bizErr
}
