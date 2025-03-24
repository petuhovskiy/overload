package autoai

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/petuhovskiy/overload/internal/log"
	"github.com/sashabaranov/go-openai"
	"go.uber.org/zap"
)

type Query struct {
	SQL string
}

type Generator struct {
	client     *openai.Client
	history    *DBHistory
	prevPrompt string
}

func NewGenerator(client *openai.Client, history *DBHistory) *Generator {
	return &Generator{
		client:  client,
		history: history,
	}
}

// TableInfo holds basic information for a table.
type TableInfo struct {
	Schema string
	Name   string
}

func (g *Generator) SavePrevResult(success, failed string) {
	if success != "" || failed != "" {
		g.prevPrompt = fmt.Sprintf("\n\nYou previously generated some queries that were executed with the following results:%s%s\n", failed, success)
	}
}

// DumpSchema retrieves the schema of the database and returns it as a string.
// It gathers full information about tables, including columns, types, constraints,
// indexes, relationships, and sizes of tables and indexes.
func (g *Generator) DumpSchema(conn *pgx.Conn) (string, error) {
	ctx := context.Background()
	var sb strings.Builder

	// Query to retrieve all user tables (exclude system schemas)
	tableQuery := `
		SELECT table_schema, table_name 
		FROM information_schema.tables 
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
		ORDER BY table_schema, table_name;
	`
	// Load all table info into a slice so we don't hold an open query while issuing new ones.
	rows, err := conn.Query(ctx, tableQuery)
	if err != nil {
		return "", err
	}
	var tables []TableInfo
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			rows.Close()
			return "", err
		}
		tables = append(tables, TableInfo{Schema: schema, Name: table})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()

	// Process each table from the buffered slice.
	for _, t := range tables {
		fullTableName := fmt.Sprintf("%s.%s", t.Schema, t.Name)
		sb.WriteString(fmt.Sprintf("Table: %s\n", fullTableName))

		// Retrieve columns for the current table.
		colQuery := `
			SELECT column_name, data_type, is_nullable, column_default
			FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = $2
			ORDER BY ordinal_position;
		`
		colRows, err := conn.Query(ctx, colQuery, t.Schema, t.Name)
		if err != nil {
			return "", err
		}
		for colRows.Next() {
			var column, dataType, isNullable string
			var colDefault *string
			if err := colRows.Scan(&column, &dataType, &isNullable, &colDefault); err != nil {
				colRows.Close()
				return "", err
			}
			defaultStr := "NULL"
			if colDefault != nil {
				defaultStr = *colDefault
			}
			sb.WriteString(fmt.Sprintf("  Column: %s, Type: %s, Nullable: %s, Default: %s\n",
				column, dataType, isNullable, defaultStr))
		}
		colRows.Close()

		// Retrieve constraints (primary keys, unique, foreign keys, etc.)
		consQuery := `
			SELECT constraint_type, constraint_name
			FROM information_schema.table_constraints
			WHERE table_schema = $1 AND table_name = $2;
		`
		consRows, err := conn.Query(ctx, consQuery, t.Schema, t.Name)
		if err != nil {
			return "", err
		}
		for consRows.Next() {
			var consType, consName string
			if err := consRows.Scan(&consType, &consName); err != nil {
				consRows.Close()
				return "", err
			}
			sb.WriteString(fmt.Sprintf("  Constraint: %s - %s\n", consType, consName))
		}
		consRows.Close()

		// Retrieve indexes for the current table.
		idxQuery := `
			SELECT indexname, indexdef
			FROM pg_indexes
			WHERE schemaname = $1 AND tablename = $2;
		`
		idxRows, err := conn.Query(ctx, idxQuery, t.Schema, t.Name)
		if err != nil {
			return "", err
		}
		for idxRows.Next() {
			var idxName, idxDef string
			if err := idxRows.Scan(&idxName, &idxDef); err != nil {
				idxRows.Close()
				return "", err
			}
			sb.WriteString(fmt.Sprintf("  Index: %s, Definition: %s\n", idxName, idxDef))
		}
		idxRows.Close()

		// Retrieve foreign key relationships.
		fkQuery := `
			SELECT tc.constraint_name, kcu.column_name, 
			       ccu.table_schema AS foreign_table_schema,
			       ccu.table_name AS foreign_table_name,
			       ccu.column_name AS foreign_column_name
			FROM information_schema.table_constraints AS tc
			JOIN information_schema.key_column_usage AS kcu
			  ON tc.constraint_name = kcu.constraint_name
			  AND tc.table_schema = kcu.table_schema
			JOIN information_schema.constraint_column_usage AS ccu
			  ON ccu.constraint_name = tc.constraint_name
			  AND ccu.table_schema = tc.table_schema
			WHERE tc.constraint_type = 'FOREIGN KEY'
			  AND tc.table_schema = $1
			  AND tc.table_name = $2;
		`
		fkRows, err := conn.Query(ctx, fkQuery, t.Schema, t.Name)
		if err != nil {
			return "", err
		}
		for fkRows.Next() {
			var consName, colName, foreignSchema, foreignTable, foreignCol string
			if err := fkRows.Scan(&consName, &colName, &foreignSchema, &foreignTable, &foreignCol); err != nil {
				fkRows.Close()
				return "", err
			}
			sb.WriteString(fmt.Sprintf("  Foreign Key: %s, Column: %s references %s.%s(%s)\n",
				consName, colName, foreignSchema, foreignTable, foreignCol))
		}
		fkRows.Close()

		// Retrieve the total table size (including indexes).
		var tableSize int64
		err = conn.QueryRow(ctx, `SELECT pg_total_relation_size($1)`, fullTableName).Scan(&tableSize)
		if err != nil {
			return "", err
		}
		sb.WriteString(fmt.Sprintf("  Total Size: %d bytes\n", tableSize))

		// Retrieve the total indexes size for the table.
		var indexesSize int64
		err = conn.QueryRow(ctx, `SELECT pg_indexes_size($1)`, fullTableName).Scan(&indexesSize)
		if err != nil {
			return "", err
		}
		sb.WriteString(fmt.Sprintf("  Indexes Size: %d bytes\n", indexesSize))

		sb.WriteString("\n")
	}

	return sb.String(), nil
}

func (g *Generator) Generate(conn *pgx.Conn) ([]Query, error) {
	schema, err := g.DumpSchema(conn)
	if err != nil {
		return nil, err
	}

	const promptTemplate = `
You have a postgres database. Your task is to generate SQL queries for simulating real-life OLTP workload for this database.
You are not allowed to use DELETE queries. You can use INSERT, UPDATE, SELECT, CREATE queries.
Don't be afraid to use complex queries, including joins, subqueries, aggregations, etc.
Don't be afraid to generate CREATE TABLE IF NOT EXISTS if you need to create a new table.
You can also create an index if you need to.
Don't be afraid to generate INSERT, UPDATE, SELECT queries. Don't generate queries that
will be too long to complete (such as iterating over all rows in a table larger than 100 MB),
instead prefer to modify/select only part of the table.

Each query will be executed multiple times, please use postgres builtin random functions for generating data instead of random values.
Try not to trigger seqscans on large tables, prefer to use indexes. If the table is really small (less than 10 megabytes), your queries scan the whole table.
Try not to assume anything about value ranges when writing WHERE clauses, instead prefer using select subqueries to select some random existing values in the table - the easy way to do this is to use LIMIT and OFFSET with random constants.
Each query should not take more than 30 seconds to run, otherwise it will considered as failed.

The schema of this postgres database is the following:

%s
%s
Please generate 5 SQL queries. Do not explain them, just return 5 markdown code blocks with SQL queries.
Queries must be valid SQL queries and must be executable in database with the given schema.
Each query must be in a separate code block, and the code block must be marked with "sql" language specifier.
`

	prompt := fmt.Sprintf(promptTemplate, schema, g.prevPrompt)

	resp, err := g.client.CreateChatCompletion(context.Background(), openai.ChatCompletionRequest{
		Model: openai.GPT4o,
		Messages: []openai.ChatCompletionMessage{
			{
				Role:    openai.ChatMessageRoleUser,
				Content: prompt,
			},
		},
	})
	if err != nil {
		return nil, err
	}

	fmt.Println("Prompt:")
	fmt.Println(prompt)
	fmt.Println()

	for i, choice := range resp.Choices {
		fmt.Printf("Choice %v:\n", i)
		fmt.Println(choice.Message.Content)
	}

	queries, err := g.splitQueries(resp.Choices[0].Message.Content)
	if err != nil {
		return nil, err
	}

	for _, query := range queries {
		if err := g.history.SaveGeneratedQuery(prompt, query.SQL, resp.Model); err != nil {
			log.Error(context.Background(), "Failed to save generated query: %v", zap.Error(err))
		}
	}

	return queries, nil
}

func (g *Generator) splitQueries(markdown string) ([]Query, error) {
	// Split the markdown string into separate queries based on code blocks
	queries := strings.Split(markdown, "```")
	if len(queries) < 2 {
		return nil, fmt.Errorf("invalid markdown format")
	}

	var result []Query
	for _, part := range queries {
		if strings.HasPrefix(part, "sql\n") {
			result = append(result, Query{SQL: strings.TrimSpace(part[4:])})
		}
	}

	return result, nil
}

func (g *Generator) DoIteration(ctx context.Context, connstr string) error {
	conn, err := pgx.Connect(ctx, connstr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	queries, err := g.Generate(conn)

	wg := sync.WaitGroup{}
	wg.Add(len(queries))

	failedQueries := ""
	successQueries := ""

	resMutex := sync.Mutex{}

	for _, query := range queries {
		go func(q Query) {
			defer wg.Done()
			stats, err := StressQuery(ctx, connstr, q)

			resMutex.Lock()
			defer resMutex.Unlock()
			if err != nil {
				log.Error(ctx, "failed to execute query", zap.String("query", q.SQL), zap.Error(err))
				failedQueries += fmt.Sprintf("\n\nThis query failed to execute with an error:\n```sql\n%s\n```", q.SQL)
			} else if stats.Count == 0 {
				failedQueries += fmt.Sprintf("\n\nThis query never finished, most likely timed out:\n```sql\n%s\n```", q.SQL)
			} else if stats.Avg != 0 {
				qps := float32(time.Second / stats.Avg)
				successQueries += fmt.Sprintf("\n\nThis was a good query that was running at a rate %v QPS:\n```sql\n%s\n```", qps, q.SQL)
			}
		}(query)
	}

	wg.Wait()

	fmt.Println("Successful queries:" + successQueries)
	fmt.Println("Failed queries:" + failedQueries)

	g.SavePrevResult(failedQueries, successQueries)

	return nil
}
