package postgresql

import (
	"context"
	"testing"

	"github.com/firebase/genkit/go/genkit"
)

func TestInit_PoolIsNil(t *testing.T) {
	ctx := context.Background()
	cfg := engineConfig{}
	engine := PostgresEngine{Pool: cfg.connPool}
	defer func() {
		r := recover()
		if r == nil {
			t.Error("panic not called")
		}
		if s, ok := r.(string); ok {
			if s != "postgres.Init engine has no pool" {
				t.Errorf("no message 'postgres.Init engine has no pool'. got %s", s)
			}
		}

	}()
	gcsp := &Postgres{engine: engine}
	_ = gcsp.Init(ctx, &genkit.Genkit{})
}
