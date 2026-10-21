// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gormctx

import (
	"context"

	"gorm.io/gorm"
)

// FromContextOrDefault returns defaultDb bound to ctx, so its queries carry
// ctx's cancellation and values. A handle already bound to ctx is returned as
// it is.
func FromContextOrDefault(ctx context.Context, defaultDb *gorm.DB) *gorm.DB {
	if defaultDb != nil && defaultDb.Statement != nil && defaultDb.Statement.Context == ctx {
		return defaultDb
	}
	return defaultDb.WithContext(ctx)
}
