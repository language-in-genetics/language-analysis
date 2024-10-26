package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// Item represents a single article from the Crossref dump
type Item struct {
	ContainerTitle []string `json:"container-title"`
	// We could add other fields we care about, but we only need container-title for filtering
}

// Response represents the top-level JSON structure
type Response struct {
	Items []Item `json:"items"`
}

// Set of target journals we want to filter for
var targetJournals = map[string]bool{
	"Journal of Genetic Counselling":          true,
	"European Journal of Human Genetics":      true,
	"American Journal of Human Genetics":      true,
	"Heredity":                               true,
	"Human Genetics":                         true,
	"Journal of Community Genetics":          true,
	"Familial Cancer":                        true,
	"Human Genetics and Genomic Advances":    true,
	"Human Genomics":                         true,
	"Genetic Epidemiology":                   true,
}

func processFile(filename string) error {
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

	// Stream each item
	for decoder.More() {
		var item map[string]interface{}
		err := decoder.Decode(&item)
		if err != nil {
			// If we hit an error, log it and try to continue with next file
			log.Printf("Error decoding item in %s: %v", filename, err)
			return nil
		}

		// Check if this item has a matching journal
		if containerTitle, ok := item["container-title"].([]interface{}); ok && len(containerTitle) > 0 {
			if journal, ok := containerTitle[0].(string); ok {
				if targetJournals[journal] {
					// Output the full JSON for matching items
					output, err := json.Marshal(item)
					if err != nil {
						log.Printf("Error marshaling matching item: %v", err)
						continue
					}
					fmt.Println(string(output))
				}
			}
		}
	}

	return nil
}

func main() {
	dirPath := flag.String("dir", ".", "Directory containing .json.gz files")
	flag.Parse()

	// Walk through all .json.gz files in the directory
	err := filepath.Walk(*dirPath, func(path string, info os.FileInfo, err error) error {
		//fmt.Printf("Considering %s\n", path)
		if err != nil {
			return err
		}

		// Skip if not a .json.gz file
		if !strings.HasSuffix(path, ".json.gz") {
			return nil
		}
		//fmt.Printf("Working on %s\n", path)

		// Process each file
		err = processFile(path)
		if err != nil {
			log.Printf("Error processing file %s: %v", path, err)
		}
		return nil
	})

	if err != nil {
		log.Fatalf("Error walking directory: %v", err)
	}
}
