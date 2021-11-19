package repository

import (
	"context"
	"github.com/jinzhu/gorm"
	"strings"
	"time"
)

type execScope struct {
	model interface{}
	scope *gorm.Scope
	rep   *Repository
}

func (es *execScope) beforeRepoCreateCallback(ctx context.Context, data Model) (err error) {

	if err = es.handleAutoTimeTag("AUTOCREATETIME"); err != nil {
		return err
	}

	if i0, ok := data.(interface {
		BeforeRepoCreate(ctx context.Context) error
	}); ok {
		err = i0.BeforeRepoCreate(ctx)
	}
	return
}

func (es *execScope) afterRepoCreateCallback(ctx context.Context, data Model) (err error) {
	if i0, ok := data.(interface {
		AfterRepoCreate(ctx context.Context) error
	}); ok {
		err = i0.AfterRepoCreate(ctx)
	}
	return
}

func (es *execScope) beforeRepoUpdateCallback(ctx context.Context, data interface{}) (err error) {

	if err = es.handleAutoTimeTag("AUTOUPDATETIME"); err != nil {
		return err
	}

	if i0, ok := data.(interface {
		BeforeRepoUpdate(ctx context.Context) error
	}); ok {
		err = i0.BeforeRepoUpdate(ctx)
	}
	return
}

func (es *execScope) afterRepoUpdateCallback(ctx context.Context, data Model) (err error) {
	if i0, ok := data.(interface {
		AfterRepoUpdate(ctx context.Context) error
	}); ok {
		err = i0.AfterRepoUpdate(ctx)
	}
	return
}

func (es *execScope) handleAutoTimeTag(tag string) (err error) {
	for _, f := range es.scope.Fields() {
		if v, ok := f.TagSettingsGet(tag); ok {
			switch strings.ToLower(v) {
			case "milli":
				if err = f.Set(time.Now().UnixNano() / 1e6); err != nil {
					return err
				}
			case "nano":
				if err = f.Set(time.Now().UnixNano()); err != nil {
					return err
				}
			default:
				if err = f.Set(time.Now().Unix()); err != nil {
					return err
				}
			}
		}
	}
	return
}

// Model 实现此接口, DeleteById 将会通过 Updates 执行, 需要update 哪些字段, 请在 BeforeSoftDelete 中实现
type SoftDeleteHook interface {
	// 在 Delete, DeleteById 中生效
	BeforeSoftDelete(ctx context.Context) error
	// 仅在 DeleteById 时调用
	AfterSoftDelete(ctx context.Context) error
}
