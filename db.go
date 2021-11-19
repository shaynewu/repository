package repository

import (
	"github.com/jinzhu/gorm"
	"sync"
	"time"
)

// todo 后面改下，这个地方有问题， 初始化db 实例库
// todo 弄成一个数据库连接池。

type DBInfo struct {
	ServiceName string
	DbConfig    *DBConfig
	Conn        *gorm.DB
}

type DBConfig struct {
	Dialect     string        `toml:"dialect"`
	Dsn         string        `toml:"dsn"`                       // data source name
	DriverName  string        `toml:"driver_name"`               // data source driver name
	Retry       int           `toml:"retry"`                     // retry time
	MaxIdle     int           `toml:"db_conn_pool_max_idle"`     // zero means defaultMaxIdleConns; negative means 0
	MaxOpen     int           `toml:"db_conn_pool_max_open"`     // <= 0 means unlimited
	MaxLifetime time.Duration `toml:"db_conn_pool_max_lifetime"` // maximum amount of time a connection may be reused
}

var dbRegister = make(map[string]*DBInfo, 1)
var lock = &sync.Mutex{}
var ServiceConfigMap = make(map[string]*DBConfig, 0)

func GetDBByDatabaseName(database, serviceName string) *DBInfo {
	mapKey := serviceName + "#" + database
	_, ok := dbRegister[mapKey]
	if !ok {
		lock.Lock()
		defer lock.Unlock()
		_, recheck := dbRegister[mapKey]
		if !recheck {
			var dbConf *DBConfig
			dbConf, ok := ServiceConfigMap[serviceName]
			if !ok {
				panic("can not find service config, please set config first!!!")
			}

			dbInfo := &DBInfo{
				ServiceName: serviceName,
				DbConfig:    dbConf,
			}
			dbInfo.InitDBConnect()
			dbRegister[mapKey] = dbInfo
		}
	}
	return dbRegister[mapKey]
}

func GetDB(database, serviceName string) *gorm.DB {
	dbInfo := GetDBByDatabaseName(database, serviceName)
	return dbInfo.Conn
}

func (s *DBInfo) InitDBConnect() {
	dbConf := s.DbConfig
	db, err := gorm.Open(dbConf.Dialect, dbConf.Dsn)
	if err != nil {
		panic(err)
	}

	s.Conn = db
}

// SetServiceDBConfig todo 有问题，需要加锁
func SetServiceDBConfig(serviceName string, dbConf *DBConfig) {
	ServiceConfigMap[serviceName] = dbConf
}

