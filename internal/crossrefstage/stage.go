package crossrefstage

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"
)

const Schema = `
CREATE TABLE import_records (
    id INTEGER PRIMARY KEY,
    category TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    raw_json_text TEXT NOT NULL
);

CREATE INDEX import_records_category_id_idx
    ON import_records (category, id);

CREATE TABLE import_counts (
    category TEXT PRIMARY KEY,
    records INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE import_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;
`

type Writer struct {
	path       string
	tmpPath    string
	db         *sql.DB
	tx         *sql.Tx
	insertStmt *sql.Stmt
	counts     map[string]int64
	closed     bool
}

func NewWriter(path string) (*Writer, error) {
	tmpPath := path + ".tmp"
	_ = os.Remove(tmpPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", tmpPath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := execAll(db,
		"PRAGMA journal_mode=OFF;",
		"PRAGMA synchronous=OFF;",
		"PRAGMA temp_store=MEMORY;",
		"PRAGMA locking_mode=EXCLUSIVE;",
		Schema,
	); err != nil {
		db.Close()
		return nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		db.Close()
		return nil, err
	}
	stmt, err := tx.Prepare("INSERT INTO import_records (category, source_ref, raw_json_text) VALUES (?, ?, ?)")
	if err != nil {
		tx.Rollback()
		db.Close()
		return nil, err
	}
	return &Writer{
		path:       path,
		tmpPath:    tmpPath,
		db:         db,
		tx:         tx,
		insertStmt: stmt,
		counts:     map[string]int64{},
	}, nil
}

func (w *Writer) Insert(category, sourceRef string, raw []byte) error {
	if strings.TrimSpace(category) == "" {
		return fmt.Errorf("category is required")
	}
	if strings.TrimSpace(sourceRef) == "" {
		return fmt.Errorf("source_ref is required")
	}
	if _, err := w.insertStmt.Exec(category, sourceRef, string(raw)); err != nil {
		return err
	}
	w.counts[category]++
	return nil
}

func (w *Writer) Count(category string) int64 {
	return w.counts[category]
}

func (w *Writer) Close(metadata map[string]string) error {
	if w.closed {
		return nil
	}
	w.closed = true
	if w.insertStmt != nil {
		if err := w.insertStmt.Close(); err != nil {
			w.tx.Rollback()
			w.db.Close()
			return err
		}
	}

	countStmt, err := w.tx.Prepare("INSERT OR REPLACE INTO import_counts (category, records) VALUES (?, ?)")
	if err != nil {
		w.tx.Rollback()
		w.db.Close()
		return err
	}
	for category, count := range w.counts {
		if _, err := countStmt.Exec(category, count); err != nil {
			countStmt.Close()
			w.tx.Rollback()
			w.db.Close()
			return err
		}
	}
	if err := countStmt.Close(); err != nil {
		w.tx.Rollback()
		w.db.Close()
		return err
	}

	if metadata == nil {
		metadata = map[string]string{}
	}
	for category, count := range w.counts {
		metadata["records_"+category] = strconv.FormatInt(count, 10)
	}
	metaStmt, err := w.tx.Prepare("INSERT OR REPLACE INTO import_metadata (key, value) VALUES (?, ?)")
	if err != nil {
		w.tx.Rollback()
		w.db.Close()
		return err
	}
	for key, value := range metadata {
		if _, err := metaStmt.Exec(key, value); err != nil {
			metaStmt.Close()
			w.tx.Rollback()
			w.db.Close()
			return err
		}
	}
	if err := metaStmt.Close(); err != nil {
		w.tx.Rollback()
		w.db.Close()
		return err
	}

	if err := w.tx.Commit(); err != nil {
		w.db.Close()
		return err
	}
	if _, err := w.db.Exec("PRAGMA optimize;"); err != nil {
		w.db.Close()
		return err
	}
	if err := w.db.Close(); err != nil {
		return err
	}
	return os.Rename(w.tmpPath, w.path)
}

func (w *Writer) Abort() {
	if w.closed {
		return
	}
	w.closed = true
	if w.insertStmt != nil {
		_ = w.insertStmt.Close()
	}
	if w.tx != nil {
		_ = w.tx.Rollback()
	}
	if w.db != nil {
		_ = w.db.Close()
	}
	_ = os.Remove(w.tmpPath)
}

func OpenReadOnly(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA query_only=ON;"); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func execAll(db *sql.DB, statements ...string) error {
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}
