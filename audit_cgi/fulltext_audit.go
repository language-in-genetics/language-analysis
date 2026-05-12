package main

import (
	"html/template"
	"net/http"
	"net/http/cgi"
	"os"
	"strconv"
)

type FulltextAuditPageData struct {
	RemoteUser    string
	Batch         FulltextBatchMeta
	Summary       FulltextSummary
	Article       FulltextArticle
	PrevArticleID int
	NextArticleID int
}

var fulltextAuditTemplate = template.Must(template.New("fulltext-audit").Funcs(templateFuncs).Funcs(template.FuncMap{
	"fulltextStatusDisplay": fulltextStatusDisplay,
	"fulltextAITermList":    fulltextAITermList,
	"fulltextTermList":      fulltextTermList,
	"fulltextSummaryLabel":  fulltextSummaryLabel,
}).Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>LIG Full-Text AI Upload Queue</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; background: #f6f7f9; color: #222; }
        .container { max-width: 1240px; margin: 0 auto; padding: 24px; }
        .card { background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.08); margin-bottom: 20px; }
        .topline { display: flex; justify-content: space-between; gap: 12px; flex-wrap: wrap; align-items: center; margin-bottom: 12px; }
        .meta { color: #666; font-size: 0.95rem; }
        .pill { display: inline-block; padding: 4px 10px; border-radius: 999px; background: #eef3ff; color: #1e4fd4; font-size: 0.85rem; margin-right: 8px; margin-bottom: 6px; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }
        .stat { background: #f9fafb; border-radius: 8px; padding: 12px; }
        a { color: #0b63ce; text-decoration: none; }
        a:hover { text-decoration: underline; }
        h1, h2, h3 { margin-top: 0; }
        .nav { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 16px; }
        .nav a { padding: 8px 12px; border-radius: 8px; background: #edf2f7; }
        .article-title { font-size: 1.35rem; margin-bottom: 12px; }
        .abstract, .fulltext { white-space: pre-wrap; line-height: 1.55; }
        .fulltext { max-height: 520px; overflow: auto; border: 1px solid #dde2e6; border-radius: 8px; padding: 14px; background: #fbfcfd; }
        textarea { width: 100%; min-height: 110px; font: inherit; padding: 10px; box-sizing: border-box; }
        .actions { display: flex; gap: 12px; flex-wrap: wrap; margin-top: 14px; }
        button { border: 0; border-radius: 8px; padding: 10px 14px; font: inherit; cursor: pointer; }
        .btn-confirm { background: #1f8f52; color: white; }
        .btn-reject { background: #c0392b; color: white; }
        .btn-stay { background: #44556b; color: white; }
        .current { background: #fff8d8; padding: 10px 12px; border-radius: 8px; margin-bottom: 14px; }
        .small { color: #666; font-size: 0.9rem; }
        .controls { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 10px; margin: 10px 0 16px; }
        .controls label { background: #f9fafb; border-radius: 8px; padding: 10px; }
        code { background: #f1f3f5; padding: 2px 4px; border-radius: 4px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="topline">
            <div>
                <h1>LIG Full-Text AI Upload Queue</h1>
                <div class="meta">Signed in as <strong>{{.RemoteUser}}</strong> · batch <code>{{.Batch.BatchSlug}}</code></div>
            </div>
            <div class="nav">
                <a href="/cgi-bin/fulltext-status.cgi?batch={{.Batch.BatchSlug}}">Processing status</a>
                <a href="/cgi-bin/audit.cgi">Title/abstract audit</a>
            </div>
        </div>

        <div class="card">
            <div class="stats">
                <div class="stat"><strong>AI processing</strong><br>{{fulltextSummaryLabel .Summary}}</div>
                <div class="stat"><strong>Full text</strong><br>{{.Summary.AvailableCount}} uploaded/available<br>{{.Summary.PendingFetchCount}} waiting for upload · {{.Summary.NeedsManualCount}} needs manual fetch</div>
                <div class="stat"><strong>Unavailable</strong><br>{{.Summary.UnavailableCount}} unavailable<br>{{.Summary.ExtractionFailedCount}} extraction failed</div>
            </div>
        </div>

        <div class="card">
            <div class="nav">
                {{if gt .PrevArticleID 0}}<a href="/cgi-bin/fulltext-verify.cgi?batch={{.Batch.BatchSlug}}&article_id={{.PrevArticleID}}">Previous</a>{{end}}
                {{if gt .NextArticleID 0}}<a href="/cgi-bin/fulltext-verify.cgi?batch={{.Batch.BatchSlug}}&article_id={{.NextArticleID}}">Next</a>{{end}}
                <a href="/cgi-bin/fulltext-verify.cgi?batch={{.Batch.BatchSlug}}">Next needing upload/AI</a>
                <a href="/cgi-bin/fulltext-upload.cgi?batch={{.Batch.BatchSlug}}&article_id={{.Article.ArticleID}}">Upload this paper for AI</a>
            </div>

			<div class="pill">full text: {{fulltextStatusDisplay .Article.FulltextStatus}}</div>
			<div class="pill">AI analysis: {{.Article.AIAnalysisStatus}}</div>
			{{if .Article.FulltextSource}}<div class="pill">source: {{.Article.FulltextSource}}</div>{{end}}
			{{if .Article.UploadedFilename}}<div class="pill">upload: {{.Article.UploadedFilename}}</div>{{end}}
			{{if eq .Article.AIAnalysisStatus "processed"}}<div class="pill">AI terms: {{fulltextAITermList .Article}}</div>{{end}}

            <p class="article-title">{{.Article.Title}}</p>
            <p class="meta">{{.Article.JournalName}} · {{yearLabel .Article.PubYear}} · article {{.Article.ArticleID}}{{if .Article.DOI}} · <a href="https://doi.org/{{.Article.DOI}}" target="_blank" rel="noopener noreferrer">{{.Article.DOI}}</a>{{end}}</p>

            {{if eq .Article.AIAnalysisStatus "processed"}}
            <div class="current">
                AI analysis flagged: {{fulltextAITermList .Article}}{{if .Article.AIProcessedAt}} · {{formatTimestamp .Article.AIProcessedAt}}{{end}}{{if .Article.AIModel}} · {{.Article.AIModel}}{{end}}.
            </div>
            {{else if .Article.AIError}}
            <div class="current">AI analysis error: {{.Article.AIError}}</div>
            {{end}}

            <h3>Abstract</h3>
            <div class="abstract">{{if .Article.Abstract}}{{.Article.Abstract}}{{else}}No abstract available.{{end}}</div>

			<h3>Full Article Text</h3>
			{{if .Article.UploadedFilename}}<p class="small">Stored upload: {{.Article.UploadedFilename}}{{if gt .Article.UploadedSize 0}} · {{.Article.UploadedSize}} bytes{{end}}{{if .Article.UploadedAt}} · {{formatTimestamp .Article.UploadedAt}}{{end}}</p>{{end}}
			{{if .Article.FulltextPath}}<p class="small">Stored text/PDF: <a href="{{.Article.FulltextPath}}" target="_blank" rel="noopener noreferrer">{{.Article.FulltextPath}}</a></p>{{end}}
            {{if .Article.ExtractedText}}
                <div class="fulltext">{{.Article.ExtractedText}}</div>
            {{else}}
                <p class="small">No full text has been uploaded for AI processing yet. Use the upload link above to paste article text or upload a PDF/text/HTML file.</p>
            {{end}}
        </div>
    </div>
</body>
</html>`))

func main() {
	if err := cgi.Serve(http.HandlerFunc(handleFulltextAudit)); err != nil {
		panic(err)
	}
}

func handleFulltextAudit(w http.ResponseWriter, r *http.Request) {
	config := GetConfig()
	db, err := OpenDatabase(config.DBPath)
	if err != nil {
		http.Error(w, "Failed to open audit database: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	batch := r.URL.Query().Get("batch")
	if batch == "" {
		batch, err = loadCurrentFulltextBatch(db)
		if err != nil {
			http.Error(w, "Failed to determine current full-text batch: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if batch == "" {
		http.Error(w, "No full-text AI processing batch has been loaded yet.", http.StatusNotFound)
		return
	}

	meta, err := loadFulltextBatchMeta(db, batch)
	if err != nil {
		http.Error(w, "Failed to load batch metadata: "+err.Error(), http.StatusInternalServerError)
		return
	}
	summary, err := loadFulltextSummary(db, batch)
	if err != nil {
		http.Error(w, "Failed to load batch summary: "+err.Error(), http.StatusInternalServerError)
		return
	}

	articleID := 0
	if rawID := r.URL.Query().Get("article_id"); rawID != "" {
		if parsed, err := strconv.Atoi(rawID); err == nil {
			articleID = parsed
		}
	}
	if articleID == 0 {
		articleID, err = firstPendingFulltextArticleID(db, batch)
		if err != nil {
			http.Error(w, "Failed to choose next article: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if articleID == 0 {
		http.Error(w, "No sampled full-text AI processing articles found.", http.StatusNotFound)
		return
	}

	article, err := loadFulltextArticle(db, batch, articleID)
	if err != nil {
		http.Error(w, "Failed to load full-text AI processing article: "+err.Error(), http.StatusInternalServerError)
		return
	}

	prevID, nextID, err := adjacentFulltextArticleIDs(db, batch, articleID)
	if err != nil {
		http.Error(w, "Failed to load article navigation: "+err.Error(), http.StatusInternalServerError)
		return
	}

	remoteUser := os.Getenv("REMOTE_USER")
	if remoteUser == "" {
		remoteUser = "authenticated user"
	}

	data := FulltextAuditPageData{
		RemoteUser:    remoteUser,
		Batch:         meta,
		Summary:       summary,
		Article:       article,
		PrevArticleID: prevID,
		NextArticleID: nextID,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := fulltextAuditTemplate.Execute(w, data); err != nil {
		http.Error(w, "Template error: "+err.Error(), http.StatusInternalServerError)
	}
}
