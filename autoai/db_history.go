package autoai

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"
)

/*
CREATE TABLE generated_queries (
    id SERIAL PRIMARY KEY,
    prompt TEXT NOT NULL,         -- what you asked the API
    generated_sql TEXT NOT NULL,  -- the SQL query returned by OpenAI
    created_at TIMESTAMPTZ DEFAULT NOW(),  -- timestamp of when it was saved
    model_used TEXT               -- optional: which OpenAI model was used
);
*/

type GeneratedQueryDB struct {
	ID           int    `db:"id"`
	Prompt       string `db:"prompt"`
	GeneratedSQL string `db:"generated_sql"`
	CreatedAt    string `db:"created_at"`
	ModelUsed    string `db:"model_used"`
	Info         any    `db:"info"`
}

/*
CREATE TABLE query_exec_info (
    id SERIAL PRIMARY KEY,
    query TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    is_failed BOOLEAN,
    qps REAL,
    conns INT,
    comment TEXT,
    info JSONB
);
*/

type QueryExecInfo struct {
	ID        int     `db:"id"`
	Query     string  `db:"query"`
	CreatedAt string  `db:"created_at"`
	IsFailed  bool    `db:"is_failed"`
	QPS       float32 `db:"qps"`
	Conns     int     `db:"conns"`
	Comment   string  `db:"comment"`
	Info      any     `db:"info"`
}

type DBHistory struct {
	db *pgxpool.Pool
}

func NewDBHistory(db *pgxpool.Pool) *DBHistory {
	return &DBHistory{db: db}
}

func (d *DBHistory) SaveGeneratedQuery(prompt, generatedSQL, modelUsed string) error {
	_, err := d.db.Exec(context.Background(), `
        INSERT INTO generated_queries (prompt, generated_sql, model_used)
        VALUES ($1, $2, $3)`, prompt, generatedSQL, modelUsed)
	return err
}

func (d *DBHistory) SaveQueryExecInfo(info *QueryExecInfo) error {
	infoJSON, err := json.Marshal(info.Info)
	if err != nil {
		return err
	}

	_, err = d.db.Exec(context.Background(), `
		INSERT INTO query_exec_info (query, is_failed, qps, conns, comment, info)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		info.Query, info.IsFailed, info.QPS, info.Conns, info.Comment, infoJSON)
	if err != nil {
		return err
	}

	return nil
}
