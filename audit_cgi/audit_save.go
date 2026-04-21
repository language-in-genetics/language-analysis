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
	if err := cgi.Serve(http.HandlerFunc(handleAuditSave)); err != nil {
		panic(err)
	}
}

func handleAuditSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form: "+err.Error(), http.StatusBadRequest)
		return
	}

	sampleBatch := r.FormValue("sample_batch")
	targetLabel := r.FormValue("target_label")
	articleID, err := strconv.Atoi(r.FormValue("article_id"))
	if sampleBatch == "" || targetLabel == "" || err != nil {
		http.Error(w, "Missing sample_batch, target_label, or article_id", http.StatusBadRequest)
		return
	}

	targetConfirmedRaw := r.FormValue("target_confirmed")
	action := r.FormValue("action")
	reviewNotes := r.FormValue("review_notes")
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

	var targetConfirmed any = nil
	if targetConfirmedRaw != "" {
		if targetConfirmedRaw == "1" {
			targetConfirmed = 1
		} else {
			targetConfirmed = 0
		}
	} else {
		var existing sql.NullInt64
		err := db.QueryRow(`
			SELECT target_confirmed
			FROM audit_articles
			WHERE sample_batch = ?
			  AND target_label = ?
			  AND article_id = ?
		`, sampleBatch, targetLabel, articleID).Scan(&existing)
		if err != nil {
			http.Error(w, "Failed to load existing review state: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if existing.Valid {
			targetConfirmed = existing.Int64
		}
	}

	_, err = db.Exec(`
		UPDATE audit_articles
		SET target_confirmed = ?,
			reviewer_username = ?,
			review_notes = ?,
			reviewed_at = CURRENT_TIMESTAMP,
			updated_at = CURRENT_TIMESTAMP
		WHERE sample_batch = ?
		  AND target_label = ?
		  AND article_id = ?
	`, targetConfirmed, reviewer, reviewNotes, sampleBatch, targetLabel, articleID)
	if err != nil {
		http.Error(w, "Failed to save review: "+err.Error(), http.StatusInternalServerError)
		return
	}

	target := fmt.Sprintf("/cgi-bin/audit.cgi?batch=%s&target_label=%s", url.QueryEscape(sampleBatch), url.QueryEscape(targetLabel))
	if action == "stay" {
		target = fmt.Sprintf("%s&article_id=%d", target, articleID)
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}
