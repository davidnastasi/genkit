package postgresql

import (
	"context"
	"fmt"
	"slices"

	"github.com/firebase/genkit/go/ai"
)

type docStore struct {
	engine          PostgresEngine
	embedder        ai.Embedder
	embedderOptions any
}

func (ds *docStore) query(ctx context.Context, ss *SimilaritySearch, embbeding []float32) (*ai.RetrieverResponse, error) {
	res := &ai.RetrieverResponse{}

	query := ss.buildQuery(embbeding)
	rows, err := ds.engine.Pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fieldDescriptions := rows.FieldDescriptions()
	columnNames := make([]string, len(fieldDescriptions))

	for i, fieldDescription := range fieldDescriptions {
		columnNames[i] = fieldDescription.Name
	}

	for rows.Next() {
		values := make([]interface{}, len(columnNames))
		valuesPrt := make([]interface{}, len(columnNames))

		for i := range columnNames {
			valuesPrt[i] = &values[i]
		}
		if err := rows.Scan(valuesPrt...); err != nil {
			return nil, fmt.Errorf("scan row failed: %v", err)
		}

		meta := make(map[string]any, ss.count)
		var content []*ai.Part
		for i, col := range columnNames {
			if (len(ss.metadataColumns) > 0 && !slices.Contains(ss.metadataColumns, col)) &&
				ss.contentColumn != col &&
				ss.metadataJsonColumn != col {
				continue
			}

			if ss.contentColumn == col {
				content = append(content, ai.NewTextPart(values[i].(string)))
			}

			if ss.metadataJsonColumn == col {
				content = append(content, ai.NewJSONPart(values[i].(string)))
				continue
			}

			meta[col] = values[i]
		}

		doc := &ai.Document{
			Metadata: meta,
			Content:  content,
		}

		res.Documents = append(res.Documents, doc)
	}

	return res, nil
}

type RetrieverOptions struct {
	SimilaritySearch *SimilaritySearch
}

func (ds *docStore) Retrieve(ctx context.Context, req *ai.RetrieverRequest) (*ai.RetrieverResponse, error) {
	if req.Options == nil {
		ss := NewSimilaritySearch()
		req.Options = &RetrieverOptions{SimilaritySearch: ss}
	}

	ropt, ok := req.Options.(*RetrieverOptions)
	if !ok {
		return nil, fmt.Errorf("postgres.Retrieve options have type %T, want %T", req.Options, &RetrieverOptions{})
	}

	if ropt.SimilaritySearch == nil {
		ss := NewSimilaritySearch()
		ropt.SimilaritySearch = ss
	}

	ereq := &ai.EmbedRequest{
		Documents: []*ai.Document{req.Query},
		Options:   ds.embedderOptions,
	}
	eres, err := ds.embedder.Embed(ctx, ereq)
	if err != nil {
		return nil, fmt.Errorf("postgres.Retrieve retrieve embedding failed: %v", err)
	}
	res, err := ds.query(ctx, ropt.SimilaritySearch, eres.Embeddings[0].Embedding)
	if err != nil {
		return nil, fmt.Errorf("googlecloudsql.postgres.Retrieve failed to execute the query: %v", err)
	}
	return res, nil
}

// Index implements the genkit Retriever.Index method.
func (ds *docStore) Index(ctx context.Context, req *ai.IndexerRequest) error {
	if len(req.Documents) == 0 {
		return nil
	}
	//TODO: implement method
	return nil
}

// newDocStore instantiate a docStore
func newDocStore(ctx context.Context, p *Postgres, cfg Config) (*docStore, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.initted {
		panic("postgres.Init not called")
	}
	if cfg.Name == "" {
		return nil, fmt.Errorf("name is empty")
	}
	if cfg.Embedder == nil {
		return nil, fmt.Errorf("embedder is required")
	}

	if cfg.SchemaName == "" {
		cfg.SchemaName = defaultSchemaName
	}

	if cfg.TableName == "" {
		cfg.TableName = defaultTable
	}

	return &docStore{
		engine:          p.engine,
		embedder:        cfg.Embedder,
		embedderOptions: cfg.EmbedderOptions,
	}, nil
}
