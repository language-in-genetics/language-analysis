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

	// Set search path to use languageingenetics schema
	_, err = db.Exec(`SET search_path TO languageingenetics, public`)
	if err != nil {
		return nil, fmt.Errorf("error setting search path: %w", err)
	}

	// Create tables if they don't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS languageingenetics.articles (
			id SERIAL PRIMARY KEY,
			data JSONB NOT NULL
		);

		CREATE TABLE IF NOT EXISTS languageingenetics.journals (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			enabled BOOLEAN NOT NULL DEFAULT true,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		return nil, fmt.Errorf("error creating tables: %w", err)
	}

	// Populate default journals if table is empty
	if err := initializeJournals(db); err != nil {
		return nil, fmt.Errorf("error initializing journals: %w", err)
	}

	return db, nil
}

func initializeJournals(db *sql.DB) error {
	// Check if journals table has any entries
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM journals").Scan(&count)
	if err != nil {
		return err
	}

	// If table is empty, populate with default journals
	if count == 0 {
		defaultJournals := []string{
			"Journal of Genetic Counselling",
			"European Journal of Human Genetics",
			"The American Journal of Human Genetics",
			"Heredity",
			"Human Genetics",
			"Journal of Community Genetics",
			"Familial Cancer",
			"Human Genetics and Genomic Advances",
			"Human Genomics",
			"Genetic Epidemiology",
		}

		stmt, err := db.Prepare("INSERT INTO journals (name) VALUES ($1)")
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, journal := range defaultJournals {
			if _, err := stmt.Exec(journal); err != nil {
				log.Printf("Warning: Could not insert journal %s: %v", journal, err)
			}
		}

		log.Printf("Initialized journals table with %d default journals", len(defaultJournals))
	}

	return nil
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
