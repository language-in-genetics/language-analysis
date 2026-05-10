package main

import (
	"database/sql"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"net/http/cgi"
	"os"
	urlpath "path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const maxFulltextUploadBytes = 80 << 20

type FulltextUploadPageData struct {
	RemoteUser string
	Batch      FulltextBatchMeta
	Article    FulltextArticle
	Message    string
	Error      string
}

var safePathPartPattern = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

var fulltextUploadTemplate = template.Must(template.New("fulltext-upload").Funcs(templateFuncs).Funcs(template.FuncMap{
	"fulltextStatusDisplay": fulltextStatusDisplay,
	"fulltextAITermList":    fulltextAITermList,
}).Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>LIG Full-Text Verification Upload</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; background: #f6f7f9; color: #222; }
        .container { max-width: 1100px; margin: 0 auto; padding: 24px; }
        .card { background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.08); margin-bottom: 20px; }
        .nav { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 16px; }
        .nav a { padding: 8px 12px; border-radius: 8px; background: #edf2f7; color: #0b63ce; text-decoration: none; }
        textarea, input[type="text"] { width: 100%; box-sizing: border-box; font: inherit; padding: 10px; }
        textarea[name="extracted_text"] { min-height: 360px; }
        textarea[name="review_notes"] { min-height: 120px; }
        button { border: 0; border-radius: 8px; padding: 10px 14px; font: inherit; cursor: pointer; background: #1f6feb; color: white; }
        .meta, .small { color: #666; font-size: 0.9rem; }
        .message { background: #e8f7ed; border-radius: 8px; padding: 10px 12px; }
        .error { background: #fdeaea; border-radius: 8px; padding: 10px 12px; }
        .abstract { white-space: pre-wrap; line-height: 1.55; }
        code { background: #f1f3f5; padding: 2px 4px; border-radius: 4px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="card">
            <h1>LIG Full-Text Verification Upload</h1>
            <p class="meta">Signed in as <strong>{{.RemoteUser}}</strong> · batch <code>{{.Batch.BatchSlug}}</code></p>
            <div class="nav">
                <a href="/cgi-bin/fulltext-verify.cgi?batch={{.Batch.BatchSlug}}&article_id={{.Article.ArticleID}}">Back to verification item</a>
                <a href="/cgi-bin/fulltext-status.cgi?batch={{.Batch.BatchSlug}}&article_id={{.Article.ArticleID}}">Status detail</a>
            </div>
            {{if .Message}}<p class="message">{{.Message}}</p>{{end}}
            {{if .Error}}<p class="error">{{.Error}}</p>{{end}}
        </div>

        <div class="card">
            <h2>{{.Article.Title}}</h2>
            <p class="meta">{{.Article.JournalName}} · {{yearLabel .Article.PubYear}} · article {{.Article.ArticleID}}{{if .Article.DOI}} · <a href="https://doi.org/{{.Article.DOI}}" target="_blank" rel="noopener noreferrer">{{.Article.DOI}}</a>{{end}}</p>
            <p class="small">Current full-text status: <strong>{{fulltextStatusDisplay .Article.FulltextStatus}}</strong>. AI analysis status: <strong>{{.Article.AIAnalysisStatus}}</strong>{{if eq .Article.AIAnalysisStatus "processed"}} · AI terms: {{fulltextAITermList .Article}}{{end}}{{if .Article.AIError}} · last error: {{.Article.AIError}}{{end}}</p>
            {{if .Article.FulltextPath}}<p class="small">Stored file: <a href="{{.Article.FulltextPath}}" target="_blank" rel="noopener noreferrer">{{.Article.FulltextPath}}</a></p>{{end}}
            <h3>Abstract</h3>
            <div class="abstract">{{if .Article.Abstract}}{{.Article.Abstract}}{{else}}No abstract available.{{end}}</div>
        </div>

        <div class="card">
            <form method="POST" action="/cgi-bin/fulltext-upload.cgi" enctype="multipart/form-data">
                <input type="hidden" name="batch" value="{{.Batch.BatchSlug}}">
                <input type="hidden" name="article_id" value="{{.Article.ArticleID}}">

                <h3>Upload File</h3>
                <p class="small">Plain text, HTML, and PDF uploads are queued for the raksasa cron analysis. Pasted text below is used directly.</p>
                <input type="file" name="article_file">

                <h3>Full Article Text</h3>
                <textarea name="extracted_text">{{.Article.ExtractedText}}</textarea>

                <h3>Verification Notes</h3>
                <textarea name="review_notes">{{.Article.ReviewNotes}}</textarea>

                <p><button type="submit">Save Full Text And Queue Analysis</button></p>
            </form>
        </div>
    </div>
</body>
</html>`))

func main() {
	if err := cgi.Serve(http.HandlerFunc(handleFulltextUpload)); err != nil {
		panic(err)
	}
}

func handleFulltextUpload(w http.ResponseWriter, r *http.Request) {
	config := GetConfig()
	db, err := OpenDatabase(config.DBPath)
	if err != nil {
		http.Error(w, "Failed to open audit database: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	remoteUser := strings.TrimSpace(r.Header.Get("X-Remote-User"))
	if remoteUser == "" {
		remoteUser = strings.TrimSpace(r.RemoteAddr)
	}
	if envUser := strings.TrimSpace(os.Getenv("REMOTE_USER")); envUser != "" {
		remoteUser = envUser
	}
	if remoteUser == "" {
		remoteUser = "authenticated verifier"
	}

	if r.Method == http.MethodPost {
		handleFulltextUploadPost(w, r, db, remoteUser)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "GET or POST required", http.StatusMethodNotAllowed)
		return
	}
	renderFulltextUploadPage(w, r, db, remoteUser, "", "")
}

func renderFulltextUploadPage(w http.ResponseWriter, r *http.Request, db *sql.DB, remoteUser, message, pageError string) {
	batch, articleID, err := requestedFulltextUploadTarget(r, db)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	renderFulltextUploadPageForTarget(w, db, remoteUser, batch, articleID, message, pageError)
}

func renderFulltextUploadPageForTarget(w http.ResponseWriter, db *sql.DB, remoteUser, batch string, articleID int, message, pageError string) {
	meta, err := loadFulltextBatchMeta(db, batch)
	if err != nil {
		http.Error(w, "Failed to load batch metadata: "+err.Error(), http.StatusInternalServerError)
		return
	}
	article, err := loadFulltextArticle(db, batch, articleID)
	if err != nil {
		http.Error(w, "Failed to load full-text verification article: "+err.Error(), http.StatusInternalServerError)
		return
	}
	data := FulltextUploadPageData{
		RemoteUser: remoteUser,
		Batch:      meta,
		Article:    article,
		Message:    message,
		Error:      pageError,
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := fulltextUploadTemplate.Execute(w, data); err != nil {
		http.Error(w, "Template error: "+err.Error(), http.StatusInternalServerError)
	}
}

func requestedFulltextUploadTarget(r *http.Request, db *sql.DB) (string, int, error) {
	batch := strings.TrimSpace(r.FormValue("batch"))
	if batch == "" {
		batch = strings.TrimSpace(r.URL.Query().Get("batch"))
	}
	var err error
	if batch == "" {
		batch, err = loadCurrentFulltextBatch(db)
		if err != nil {
			return "", 0, fmt.Errorf("failed to determine current full-text batch: %w", err)
		}
	}
	if batch == "" {
		return "", 0, fmt.Errorf("no full-text verification batch has been loaded yet")
	}

	articleID := 0
	rawID := strings.TrimSpace(r.FormValue("article_id"))
	if rawID == "" {
		rawID = strings.TrimSpace(r.URL.Query().Get("article_id"))
	}
	if rawID != "" {
		articleID, err = strconv.Atoi(rawID)
		if err != nil || articleID <= 0 {
			return "", 0, fmt.Errorf("invalid article_id")
		}
	}
	if articleID == 0 {
		articleID, err = firstPendingFulltextArticleID(db, batch)
		if err != nil {
			return "", 0, fmt.Errorf("failed to choose next article: %w", err)
		}
	}
	if articleID == 0 {
		return "", 0, fmt.Errorf("no sampled full-text verification articles found")
	}
	return batch, articleID, nil
}

func handleFulltextUploadPost(w http.ResponseWriter, r *http.Request, db *sql.DB, remoteUser string) {
	r.Body = http.MaxBytesReader(w, r.Body, maxFulltextUploadBytes)
	if err := r.ParseMultipartForm(maxFulltextUploadBytes); err != nil {
		renderFulltextUploadPage(w, r, db, remoteUser, "", "Upload failed: "+err.Error())
		return
	}

	batch, articleID, err := requestedFulltextUploadTarget(r, db)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	extractedText := strings.TrimSpace(r.FormValue("extracted_text"))
	reviewNotes := strings.TrimSpace(r.FormValue("review_notes"))
	storedPath := ""

	file, header, err := r.FormFile("article_file")
	if err == nil {
		defer file.Close()
		uploadBytes, err := io.ReadAll(file)
		if err != nil {
			renderFulltextUploadPageForTarget(w, db, remoteUser, batch, articleID, "", "Failed to read uploaded file: "+err.Error())
			return
		}
		if len(uploadBytes) > 0 {
			contentType := header.Header.Get("Content-Type")
			storedPath, err = saveFulltextUpload(batch, articleID, header.Filename, uploadBytes)
			if err != nil {
				renderFulltextUploadPageForTarget(w, db, remoteUser, batch, articleID, "", "Failed to save uploaded file: "+err.Error())
				return
			}
			if extractedText == "" && looksTextUpload(header.Filename, contentType) {
				extractedText = strings.TrimSpace(string(uploadBytes))
			}
		}
	} else if err != http.ErrMissingFile {
		renderFulltextUploadPageForTarget(w, db, remoteUser, batch, articleID, "", "Upload failed: "+err.Error())
		return
	}

	if extractedText == "" && storedPath == "" {
		renderFulltextUploadPageForTarget(w, db, remoteUser, batch, articleID, "", "Paste full article text or choose a file to upload.")
		return
	}

	analysisStatus := "queued"
	if extractedText == "" && storedPath == "" {
		analysisStatus = "not_queued"
	}
	fulltextStatus := "available"
	fulltextSource := "manual_upload"
	_, err = db.Exec(`
		UPDATE fulltext_articles
		SET
			fulltext_status = ?,
			fulltext_source = ?,
			fulltext_path = CASE WHEN ? = '' THEN fulltext_path ELSE ? END,
			extracted_text = ?,
			ai_analysis_status = ?,
			ai_error = NULL,
			reviewer_username = ?,
			review_notes = ?,
			updated_at = CURRENT_TIMESTAMP
		WHERE batch_slug = ? AND article_id = ?
	`, fulltextStatus, fulltextSource, storedPath, storedPath, extractedText, analysisStatus, remoteUser, reviewNotes, batch, articleID)
	if err != nil {
		renderFulltextUploadPageForTarget(w, db, remoteUser, batch, articleID, "", "Failed to save upload metadata: "+err.Error())
		return
	}

	message := "Full text saved and queued for AI analysis."
	if extractedText == "" {
		message = "File saved and queued for raksasa-side text extraction and AI analysis."
	}
	renderFulltextUploadPageForTarget(w, db, remoteUser, batch, articleID, message, "")
}

func saveFulltextUpload(batch string, articleID int, filename string, data []byte) (string, error) {
	safeBatch := safePathPart(batch)
	safeName := safePathPart(filename)
	if safeName == "" {
		safeName = "article"
	}
	stamp := time.Now().UTC().Format("20060102T150405Z")
	storedName := stamp + "-" + safeName
	uploadDir := filepath.Join("..", "htdocs", "fulltext_uploads", safeBatch, strconv.Itoa(articleID))
	if err := os.MkdirAll(uploadDir, 0775); err != nil {
		return "", err
	}
	target := filepath.Join(uploadDir, storedName)
	if err := os.WriteFile(target, data, 0664); err != nil {
		return "", err
	}
	return urlpath.Join("/fulltext_uploads", safeBatch, strconv.Itoa(articleID), storedName), nil
}

func safePathPart(value string) string {
	value = strings.TrimSpace(filepath.Base(value))
	value = safePathPartPattern.ReplaceAllString(value, "-")
	value = strings.Trim(value, ".-")
	if len(value) > 120 {
		value = value[:120]
	}
	return value
}

func looksTextUpload(filename, contentType string) bool {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	ext := strings.ToLower(filepath.Ext(filename))
	if strings.HasPrefix(contentType, "text/") {
		return true
	}
	switch ext {
	case ".txt", ".text", ".md", ".markdown", ".html", ".htm", ".xml":
		return true
	default:
		return false
	}
}
