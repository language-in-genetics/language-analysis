package main

import (
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/lib/pq"
)

// Response represents the top-level JSON structure
type Response struct {
	Items []json.RawMessage `json:"items"`
}

func initDB(dbConnStr string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("error pinging database: %w", err)
	}

	// Create table if it doesn't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS articles (
			id SERIAL PRIMARY KEY,
			data JSONB NOT NULL
		)
	`)
	if err != nil {
		return nil, fmt.Errorf("error creating table: %w", err)
	}

	return db, nil
}

func processFile(filename string, db *sql.DB) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Create a gzip reader
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("error creating gzip reader: %w", err)
	}
	defer gzReader.Close()

	// Create a decoder for streaming JSON
	decoder := json.NewDecoder(gzReader)

	// Read opening bracket
	_, err = decoder.Token()
	if err != nil {
		return fmt.Errorf("error reading opening token: %w", err)
	}

	// Read "items" key
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("error reading items key: %w", err)
	}
	if token != "items" {
		return fmt.Errorf("expected 'items' key, got %v", token)
	}

	// Read opening bracket of items array
	_, err = decoder.Token()
	if err != nil {
		return fmt.Errorf("error reading items array opening: %w", err)
	}

	// Prepare the insert statement
	stmt, err := db.Prepare(`
		INSERT INTO articles (data)
		VALUES ($1)
	`)
	if err != nil {
		return fmt.Errorf("error preparing statement: %w", err)
	}
	defer stmt.Close()

	// Stream each item
	count := 0
	for decoder.More() {
		var item json.RawMessage
		if err := decoder.Decode(&item); err != nil {
			log.Printf("Error decoding item in %s: %v", filename, err)
			continue
		}

		// Insert the JSON directly into the database
		_, err = stmt.Exec(item)
		if err != nil {
			log.Printf("Error inserting item into database: %v", err)
			continue
		}

		count++
		if count%1000 == 0 {
			log.Printf("Processed %d items from %s", count, filename)
		}
	}

	log.Printf("Completed processing %s: inserted %d items", filename, count)
	return nil
}

func main() {
	inputDir := flag.String("dir", ".", "Directory containing .json.gz files")
	dbConnStr := flag.String("dbconn", "host=/var/run/postgresql dbname=crossref sslmode=disable","PostgreSQL connection string")
	flag.Parse()

	// Initialize database connection
	db, err := initDB(*dbConnStr)
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	defer db.Close()

	// Walk through all .json.gz files in the directory
	err = filepath.Walk(*inputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip if not a .json.gz file
		if !strings.HasSuffix(path, ".json.gz") {
			return nil
		}

		// Process each file
		log.Printf("Processing file: %s", path)
		if err := processFile(path, db); err != nil {
			log.Printf("Error processing file %s: %v", path, err)
		}
		return nil
	})

	if err != nil {
		log.Fatalf("Error walking directory: %v", err)
	}
}
