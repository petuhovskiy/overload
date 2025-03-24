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
	launcher   *Launcher
}

func NewGenerator(client *openai.Client, history *DBHistory) *Generator {
	return &Generator{
		client:   client,
		history:  history,
		launcher: &Launcher{db: history},
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
// It returns a compact representation of tables with their columns, primary keys, and foreign keys.
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
	// Load all table info into a slice
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

	// Process each table
	for _, t := range tables {
		fullTableName := fmt.Sprintf("%s.%s", t.Schema, t.Name)

		// Get table size for size indication
		var tableSize int64
		err = conn.QueryRow(ctx, `SELECT pg_total_relation_size($1)`, fullTableName).Scan(&tableSize)
		if err != nil {
			return "", err
		}

		// Format size in a readable way
		sizeStr := "small"
		if tableSize > 10*1024*1024 { // 10MB
			sizeStr = "medium"
		}
		if tableSize > 100*1024*1024 { // 100MB
			sizeStr = "large"
		}

		sb.WriteString(fmt.Sprintf("TABLE %s (%s):\n", fullTableName, sizeStr))

		// Retrieve columns with condensed output
		colQuery := `
			SELECT 
				column_name, 
				data_type, 
				is_nullable,
				CASE WHEN column_default IS NOT NULL THEN 'DEFAULT ' || column_default ELSE '' END as default_value,
				CASE WHEN EXISTS (
					SELECT 1 FROM information_schema.table_constraints tc
					JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
					WHERE tc.table_schema = $1 AND tc.table_name = $2
					AND tc.constraint_type = 'PRIMARY KEY' AND ccu.column_name = columns.column_name
				) THEN 'PK' ELSE '' END AS is_pk
			FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = $2
			ORDER BY ordinal_position;
		`
		colRows, err := conn.Query(ctx, colQuery, t.Schema, t.Name)
		if err != nil {
			return "", err
		}

		for colRows.Next() {
			var column, dataType, isNullable, defaultValue, isPK string
			if err := colRows.Scan(&column, &dataType, &isNullable, &defaultValue, &isPK); err != nil {
				colRows.Close()
				return "", err
			}

			nullable := ""
			if isNullable == "NO" {
				nullable = "NOT NULL"
			}

			pkStr := ""
			if isPK == "PK" {
				pkStr = "PRIMARY KEY"
			}

			parts := []string{dataType}
			if nullable != "" {
				parts = append(parts, nullable)
			}
			if defaultValue != "" {
				parts = append(parts, defaultValue)
			}
			if pkStr != "" {
				parts = append(parts, pkStr)
			}

			sb.WriteString(fmt.Sprintf("  %s: %s\n", column, strings.Join(parts, " ")))
		}
		colRows.Close()

		// Retrieve foreign keys - simplified output
		fkQuery := `
			SELECT 
				kcu.column_name,  
				ccu.table_schema || '.' || ccu.table_name AS references_table,
				ccu.column_name AS references_column
			FROM information_schema.table_constraints AS tc
			JOIN information_schema.key_column_usage AS kcu
			  ON tc.constraint_name = kcu.constraint_name
			JOIN information_schema.constraint_column_usage AS ccu
			  ON ccu.constraint_name = tc.constraint_name
			WHERE tc.constraint_type = 'FOREIGN KEY'
			  AND tc.table_schema = $1
			  AND tc.table_name = $2;
		`
		fkRows, err := conn.Query(ctx, fkQuery, t.Schema, t.Name)
		if err != nil {
			return "", err
		}

		hasForeignKeys := false
		for fkRows.Next() {
			if !hasForeignKeys {
				sb.WriteString("  FOREIGN KEYS:\n")
				hasForeignKeys = true
			}

			var colName, refsTable, refsCol string
			if err := fkRows.Scan(&colName, &refsTable, &refsCol); err != nil {
				fkRows.Close()
				return "", err
			}
			sb.WriteString(fmt.Sprintf("    %s -> %s(%s)\n", colName, refsTable, refsCol))
		}
		fkRows.Close()

		// Get important indexes (non-primary key indexes) - simplified
		idxQuery := `
			SELECT indexname, indexdef
			FROM pg_indexes
			WHERE schemaname = $1 AND tablename = $2
			AND indexname NOT LIKE '%_pkey';
		`
		idxRows, err := conn.Query(ctx, idxQuery, t.Schema, t.Name)
		if err != nil {
			return "", err
		}

		hasIndexes := false
		for idxRows.Next() {
			if !hasIndexes {
				sb.WriteString("  INDEXES:\n")
				hasIndexes = true
			}

			var idxName, idxDef string
			if err := idxRows.Scan(&idxName, &idxDef); err != nil {
				idxRows.Close()
				return "", err
			}
			// Extract just the essential part of the index definition
			parts := strings.Split(idxDef, "USING")
			if len(parts) > 1 {
				sb.WriteString(fmt.Sprintf("    %s: USING%s\n", idxName, parts[1]))
			} else {
				sb.WriteString(fmt.Sprintf("    %s\n", idxName))
			}
		}
		idxRows.Close()

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
			stats := g.launcher.Run(ctx, connstr, q)

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
