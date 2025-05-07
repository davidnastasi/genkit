package postgresql

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/firebase/genkit/go/ai"
)

// RetrieverOptions options for retriever
type RetrieverOptions struct {
	Filter           any
	K                int
	DistanceStrategy DistanceStrategy
}

func (p *Postgres) Retrieve(ctx context.Context, req *ai.RetrieverRequest) (*ai.RetrieverResponse, error) {
	if req.Options == nil {
		req.Options = &RetrieverOptions{
			Filter:           nil,
			K:                defaultCount,
			DistanceStrategy: defaultDistanceStrategy,
		}
	}

	ropt, ok := req.Options.(*RetrieverOptions)
	if !ok {
		return nil, fmt.Errorf("postgres.Retrieve options have type %T, want %T", req.Options, &RetrieverOptions{})
	}

	ereq := &ai.EmbedRequest{
		Documents: []*ai.Document{req.Query},
		Options:   p.config.EmbedderOptions,
	}
	eres, err := p.config.Embedder.Embed(ctx, ereq)
	if err != nil {
		return nil, fmt.Errorf("postgres.Retrieve retrieve embedding failed: %v", err)
	}
	res, err := p.query(ctx, ropt, eres.Embeddings[0].Embedding)
	if err != nil {
		return nil, fmt.Errorf("googlecloudsql.postgres.Retrieve failed to execute the query: %v", err)
	}
	return res, nil
}

func (p *Postgres) query(ctx context.Context, ropt *RetrieverOptions, embbeding []float32) (*ai.RetrieverResponse, error) {
	res := &ai.RetrieverResponse{}

	query := p.buildQuery(ropt, embbeding)
	rows, err := p.engine.Pool.Query(ctx, query)
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

		meta := make(map[string]any, ropt.K)
		var content []*ai.Part
		for i, col := range columnNames {
			if (len(p.config.MetadataColumns) > 0 && !slices.Contains(p.config.MetadataColumns, col)) &&
				p.config.ContentColumn != col &&
				p.config.MetadataJSONColumn != col {
				continue
			}

			if p.config.ContentColumn == col {
				content = append(content, ai.NewTextPart(values[i].(string)))
			}

			if p.config.MetadataJSONColumn == col {
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

func (p *Postgres) buildQuery(ropt *RetrieverOptions, embedding []float32) string {

	vectorToString := func(v []float32) string {
		stringArray := make([]string, len(v))
		for i, val := range v {
			stringArray[i] = strconv.FormatFloat(float64(val), 'f', -1, 32)
		}
		return "[" + strings.Join(stringArray, ", ") + "]"
	}

	operator := ropt.DistanceStrategy.operator()
	searchFunction := ropt.DistanceStrategy.similaritySearchFunction()
	columns := append(p.config.MetadataColumns, p.config.ContentColumn)
	if p.config.MetadataJSONColumn != "" {
		columns = append(columns, p.config.MetadataJSONColumn)
	}
	columnNames := strings.Join(columns, `, `)
	whereClause := ""
	if ropt.Filter != nil {
		whereClause = fmt.Sprintf("WHERE %s", ropt.Filter)
	}

	stmt := fmt.Sprintf(`
        SELECT %s, %s(%s, '%s') AS distance FROM "%s"."%s" %s ORDER BY %s %s '%s' LIMIT %d;`,
		columnNames, searchFunction, p.config.EmbeddingColumn, vectorToString(embedding), p.config.SchemaName, p.config.TableName,
		whereClause, p.config.EmbeddingColumn, operator, vectorToString(embedding), ropt.K)

	return stmt
}
