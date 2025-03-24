package autoai

import (
	"context"

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
