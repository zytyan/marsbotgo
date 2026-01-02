package q

import "go.uber.org/zap"

func NewWithLogger(db DBTX, logger *zap.Logger) *Queries {
	q := New(db)
	q.logger = logger
	return q
}
