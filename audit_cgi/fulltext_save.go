package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/cgi"
	"net/url"
	"os"
	"strconv"
)

func main() {
	if err := cgi.Serve(http.HandlerFunc(handleFulltextSave)); err != nil {
		panic(err)
	}
}

func handleFulltextSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form: "+err.Error(), http.StatusBadRequest)
		return
	}

	batch := r.FormValue("batch")
	articleID, err := strconv.Atoi(r.FormValue("article_id"))
	if batch == "" || err != nil {
		http.Error(w, "Missing batch or article_id", http.StatusBadRequest)
		return
	}

	reviewer := os.Getenv("REMOTE_USER")
	if reviewer == "" {
		http.Error(w, "REMOTE_USER not set; check HTTP auth configuration", http.StatusForbidden)
		return
	}

	config := GetConfig()
	db, err := OpenDatabase(config.DBPath)
	if err != nil {
		http.Error(w, "Failed to open audit database: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	var existingTerminology sql.NullInt64
	var existingCaucasian sql.NullInt64
	var existingWhite sql.NullInt64
	var existingEuropean sql.NullInt64
	var existingOther sql.NullInt64
	err = db.QueryRow(`
		SELECT
			terminology_present,
			caucasian_present,
			white_present,
			european_present,
			other_present
		FROM fulltext_articles
		WHERE batch_slug = ? AND article_id = ?
	`, batch, articleID).Scan(
		&existingTerminology,
		&existingCaucasian,
		&existingWhite,
		&existingEuropean,
		&existingOther,
	)
	if err != nil {
		http.Error(w, "Failed to load existing verification state: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var terminologyPresent any = nil
	var caucasianPresent any = nil
	var whitePresent any = nil
	var europeanPresent any = nil
	var otherPresent any = nil
	if raw := r.FormValue("terminology_present"); raw != "" {
		if raw == "1" {
			terminologyPresent = 1
		} else {
			terminologyPresent = 0
		}
		caucasianPresent = checkboxInt(r.FormValue("caucasian_present"))
		whitePresent = checkboxInt(r.FormValue("white_present"))
		europeanPresent = checkboxInt(r.FormValue("european_present"))
		otherPresent = checkboxInt(r.FormValue("other_present"))
	} else {
		terminologyPresent = nullableInt(existingTerminology)
		caucasianPresent = nullableInt(existingCaucasian)
		whitePresent = nullableInt(existingWhite)
		europeanPresent = nullableInt(existingEuropean)
		otherPresent = nullableInt(existingOther)
	}

	_, err = db.Exec(`
		UPDATE fulltext_articles
		SET terminology_present = ?,
			caucasian_present = ?,
			white_present = ?,
			european_present = ?,
			other_present = ?,
			quoted_evidence = ?,
			reviewer_username = ?,
			review_notes = ?,
			reviewed_at = CASE WHEN ? IS NULL THEN reviewed_at ELSE CURRENT_TIMESTAMP END,
			updated_at = CURRENT_TIMESTAMP
		WHERE batch_slug = ?
		  AND article_id = ?
	`, terminologyPresent, caucasianPresent, whitePresent, europeanPresent, otherPresent, r.FormValue("quoted_evidence"), reviewer, r.FormValue("review_notes"), terminologyPresent, batch, articleID)
	if err != nil {
		http.Error(w, "Failed to save full-text verification: "+err.Error(), http.StatusInternalServerError)
		return
	}

	target := fmt.Sprintf("/cgi-bin/fulltext-verify.cgi?batch=%s", url.QueryEscape(batch))
	if r.FormValue("action") == "stay" {
		target = fmt.Sprintf("%s&article_id=%d", target, articleID)
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func checkboxInt(value string) int {
	if value == "1" {
		return 1
	}
	return 0
}

func nullableInt(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}
