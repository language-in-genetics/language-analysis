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
	if err := cgi.Serve(http.HandlerFunc(handleHumanSubjectSave)); err != nil {
		panic(err)
	}
}

func handleHumanSubjectSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form: "+err.Error(), http.StatusBadRequest)
		return
	}

	batch := r.FormValue("batch")
	classificationID, err := strconv.Atoi(r.FormValue("classification_id"))
	if batch == "" || err != nil {
		http.Error(w, "Missing batch or classification_id", http.StatusBadRequest)
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

	var reviewerAboutHumans any = nil
	if raw := r.FormValue("reviewer_about_humans"); raw != "" {
		if raw == "1" {
			reviewerAboutHumans = 1
		} else {
			reviewerAboutHumans = 0
		}
	} else {
		var existing sql.NullInt64
		err := db.QueryRow(`
			SELECT reviewer_about_humans
			FROM human_subject_audit_articles
			WHERE batch_slug = ?
			  AND classification_id = ?
		`, batch, classificationID).Scan(&existing)
		if err != nil {
			http.Error(w, "Failed to load existing review state: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if existing.Valid {
			reviewerAboutHumans = existing.Int64
		}
	}

	_, err = db.Exec(`
		UPDATE human_subject_audit_articles
		SET reviewer_about_humans = ?,
			reviewer_username = ?,
			review_notes = ?,
			reviewed_at = CASE WHEN ? IS NULL THEN reviewed_at ELSE CURRENT_TIMESTAMP END,
			updated_at = CURRENT_TIMESTAMP
		WHERE batch_slug = ?
		  AND classification_id = ?
	`, reviewerAboutHumans, reviewer, r.FormValue("review_notes"), reviewerAboutHumans, batch, classificationID)
	if err != nil {
		http.Error(w, "Failed to save Homo sapiens review: "+err.Error(), http.StatusInternalServerError)
		return
	}

	target := fmt.Sprintf("/cgi-bin/audit-human-subject.cgi?batch=%s", url.QueryEscape(batch))
	if r.FormValue("action") == "stay" {
		target = fmt.Sprintf("%s&classification_id=%d", target, classificationID)
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}
