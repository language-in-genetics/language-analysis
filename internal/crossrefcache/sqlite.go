package crossrefcache

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"
)

const sqliteSchema = `
CREATE TABLE doi_cache (
    doi_hash BLOB PRIMARY KEY,
    text_fingerprint BLOB NOT NULL,
    work_id INTEGER NOT NULL,
    work_version_id INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE cache_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;
`

type SQLiteWriter struct {
	path       string
	tmpPath    string
	db         *sql.DB
	tx         *sql.Tx
	insertStmt *sql.Stmt
	records    int
	duplicates int
	closed     bool
}

type SQLiteLookup struct {
	db      *sql.DB
	stmt    *sql.Stmt
	records int
}

func NewSQLiteWriter(path string) (*SQLiteWriter, error) {
	tmpPath := path + ".tmp"
	_ = os.Remove(tmpPath)

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
		sqliteSchema,
	); err != nil {
		db.Close()
		return nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		db.Close()
		return nil, err
	}
	stmt, err := tx.Prepare("INSERT OR IGNORE INTO doi_cache (doi_hash, text_fingerprint, work_id, work_version_id) VALUES (?, ?, ?, ?)")
	if err != nil {
		tx.Rollback()
		db.Close()
		return nil, err
	}
	return &SQLiteWriter{path: path, tmpPath: tmpPath, db: db, tx: tx, insertStmt: stmt}, nil
}

func (w *SQLiteWriter) Insert(record Record) error {
	result, err := w.insertStmt.Exec(record.DOIHash[:], record.TextFingerprint[:], int64(record.WorkID), int64(record.WorkVersionID))
	if err != nil {
		return err
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		w.duplicates++
		return nil
	}
	w.records++
	return nil
}

func (w *SQLiteWriter) Records() int {
	return w.records
}

func (w *SQLiteWriter) Duplicates() int {
	return w.duplicates
}

func (w *SQLiteWriter) Close(metadata map[string]string) error {
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
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadata["records"] = strconv.Itoa(w.records)
	metadata["duplicate_hashes"] = strconv.Itoa(w.duplicates)
	metaStmt, err := w.tx.Prepare("INSERT OR REPLACE INTO cache_metadata (key, value) VALUES (?, ?)")
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

func (w *SQLiteWriter) Abort() {
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

func OpenSQLiteLookup(path string, copyToMemory bool) (*SQLiteLookup, error) {
	var db *sql.DB
	var err error
	if copyToMemory {
		db, err = sql.Open("sqlite", "file:crossref_cache?mode=memory&cache=shared")
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(1)
		if err := execAll(db,
			"PRAGMA temp_store=MEMORY;",
			sqliteSchema,
			"ATTACH DATABASE "+quoteSQLiteString(path)+" AS disk;",
			"INSERT INTO doi_cache SELECT doi_hash, text_fingerprint, work_id, work_version_id FROM disk.doi_cache;",
			"INSERT OR REPLACE INTO cache_metadata SELECT key, value FROM disk.cache_metadata;",
			"DETACH DATABASE disk;",
		); err != nil {
			db.Close()
			return nil, err
		}
	} else {
		db, err = sql.Open("sqlite", path)
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(1)
		if _, err := db.Exec("PRAGMA query_only=ON;"); err != nil {
			db.Close()
			return nil, err
		}
	}

	lookup := &SQLiteLookup{db: db}
	lookup.records, err = lookup.readRecordCount()
	if err != nil {
		db.Close()
		return nil, err
	}
	lookup.stmt, err = db.Prepare("SELECT text_fingerprint, work_id, work_version_id FROM doi_cache WHERE doi_hash = ?")
	if err != nil {
		db.Close()
		return nil, err
	}
	return lookup, nil
}

func (l *SQLiteLookup) Len() int {
	return l.records
}

func (l *SQLiteLookup) Lookup(doiHash [32]byte) (Record, bool, error) {
	var fingerprint []byte
	var workID int64
	var workVersionID int64
	err := l.stmt.QueryRow(doiHash[:]).Scan(&fingerprint, &workID, &workVersionID)
	if err == sql.ErrNoRows {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	if len(fingerprint) != 32 {
		return Record{}, false, fmt.Errorf("cache row has %d-byte text fingerprint for DOI hash %s", len(fingerprint), hex.EncodeToString(doiHash[:]))
	}
	record := Record{
		DOIHash:       doiHash,
		WorkID:        uint64(workID),
		WorkVersionID: uint64(workVersionID),
	}
	copy(record.TextFingerprint[:], fingerprint)
	return record, true, nil
}

func (l *SQLiteLookup) Close() error {
	var err error
	if l.stmt != nil {
		err = l.stmt.Close()
	}
	if closeErr := l.db.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (l *SQLiteLookup) readRecordCount() (int, error) {
	var value string
	err := l.db.QueryRow("SELECT value FROM cache_metadata WHERE key = 'records'").Scan(&value)
	if err == nil {
		records, parseErr := strconv.Atoi(value)
		if parseErr != nil {
			return 0, parseErr
		}
		return records, nil
	}
	if err != sql.ErrNoRows {
		return 0, err
	}
	var records int
	if err := l.db.QueryRow("SELECT count(*) FROM doi_cache").Scan(&records); err != nil {
		return 0, err
	}
	return records, nil
}

func execAll(db *sql.DB, statements ...string) error {
	for _, statement := range statements {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := db.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

func quoteSQLiteString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
