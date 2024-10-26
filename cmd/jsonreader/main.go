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
	DOI           string   `json:"DOI"`
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

// sanitizePath replaces problematic characters in path components
func sanitizePath(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, " ", "_"), "/", "_")
}

// writeJSONToFile writes the item to a pretty-printed JSON file
func writeJSONToFile(item map[string]interface{}, journal string, outputDir string) error {
	// Get the DOI
	doi, ok := item["DOI"].(string)
	if !ok || doi == "" {
		return fmt.Errorf("missing or invalid DOI")
	}

	// Create the directory path
	dirPath := filepath.Join(outputDir, sanitizePath(journal), sanitizePath(doi))
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("error creating directory %s: %w", dirPath, err)
	}

	// Create the file path
	filePath := filepath.Join(dirPath, "metadata.json")

	// Pretty print the JSON
	jsonData, err := json.MarshalIndent(item, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling JSON: %w", err)
	}

	// Write to file
	if err := os.WriteFile(filePath, jsonData, 0644); err != nil {
		return fmt.Errorf("error writing file %s: %w", filePath, err)
	}

	log.Printf("Written: %s", filePath)
	return nil
}

func processFile(filename string, outputDir string) error {
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
					if err := writeJSONToFile(item, journal, outputDir); err != nil {
						log.Printf("Error writing item to file: %v", err)
					}
				}
			}
		}
	}

	return nil
}

func main() {
	inputDir := flag.String("dir", ".", "Directory containing .json.gz files")
	outputDir := flag.String("output", "output", "Directory for output files")
	flag.Parse()

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		log.Fatalf("Error creating output directory: %v", err)
	}

	// Walk through all .json.gz files in the directory
	err := filepath.Walk(*inputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip if not a .json.gz file
		if !strings.HasSuffix(path, ".json.gz") {
			return nil
		}

		// Process each file
		log.Printf("Processing file: %s", path)
		err = processFile(path, *outputDir)
		if err != nil {
			log.Printf("Error processing file %s: %v", path, err)
		}
		return nil
	})

	if err != nil {
		log.Fatalf("Error walking directory: %v", err)
	}
}
