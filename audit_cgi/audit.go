package main

import (
	"html/template"
	"net/http"
	"net/http/cgi"
	"os"
	"strconv"
)

type AuditPageData struct {
	RemoteUser      string
	Batch           BatchMeta
	TargetLabel     string
	Article         AuditArticle
	PrevArticleID   int
	NextArticleID   int
	TargetSummaries []TargetLabelSummary
	CurrentStatus   string
	CurrentOutcome  string
}

var auditTemplate = template.Must(template.New("audit").Funcs(templateFuncs).Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>LIG Human Audit</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; background: #f6f7f9; color: #222; }
        .container { max-width: 1200px; margin: 0 auto; padding: 24px; }
        .card { background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.08); margin-bottom: 20px; }
        .topline { display: flex; justify-content: space-between; gap: 12px; flex-wrap: wrap; align-items: center; margin-bottom: 12px; }
        .meta { color: #666; font-size: 0.95rem; }
        .pill { display: inline-block; padding: 4px 10px; border-radius: 999px; background: #eef3ff; color: #1e4fd4; font-size: 0.85rem; margin-right: 8px; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }
        .stat { background: #f9fafb; border-radius: 8px; padding: 12px; }
        a { color: #0b63ce; text-decoration: none; }
        a:hover { text-decoration: underline; }
        h1, h2, h3 { margin-top: 0; }
        .nav { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 16px; }
        .nav a { padding: 8px 12px; border-radius: 8px; background: #edf2f7; }
        .article-title { font-size: 1.4rem; margin-bottom: 12px; }
        .abstract { white-space: pre-wrap; line-height: 1.55; }
        textarea { width: 100%; min-height: 140px; font: inherit; padding: 10px; }
        .actions { display: flex; gap: 12px; flex-wrap: wrap; margin-top: 14px; }
        button { border: 0; border-radius: 8px; padding: 10px 14px; font: inherit; cursor: pointer; }
        .btn-confirm { background: #1f8f52; color: white; }
        .btn-reject { background: #c0392b; color: white; }
        .btn-stay { background: #44556b; color: white; }
        .current { background: #fff8d8; padding: 10px 12px; border-radius: 8px; margin-bottom: 14px; }
        .small { color: #666; font-size: 0.9rem; }
        code { background: #f1f3f5; padding: 2px 4px; border-radius: 4px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="topline">
            <div>
                <h1>LIG Human Audit</h1>
                <div class="meta">Signed in as <strong>{{.RemoteUser}}</strong> · batch <code>{{.Batch.SampleBatch}}</code></div>
            </div>
            <div class="nav">
                <a href="/cgi-bin/audit-status.cgi{{queryWithBatch .Batch.SampleBatch}}">Public status</a>
                <a href="/cgi-bin/fulltext-upload.cgi">Full-text AI upload</a>
                {{range .TargetSummaries}}
                <a href="/cgi-bin/audit.cgi?batch={{$.Batch.SampleBatch}}&target_label={{.TargetLabel}}">{{targetLabelDisplay .TargetLabel}}</a>
                {{end}}
            </div>
        </div>

        <div class="card">
            <div class="stats">
                {{range .TargetSummaries}}
                <div class="stat">
                    <strong>{{targetLabelDisplay .TargetLabel}}</strong><br>
                    {{.ReviewedCount}} reviewed / {{.TotalCount}} total<br>
                    {{.PendingCount}} pending<br>
                    {{targetLabelSummaryLabel .}}
                </div>
                {{end}}
            </div>
        </div>

        <div class="card">
            <div class="nav">
                {{if gt .PrevArticleID 0}}<a href="/cgi-bin/audit.cgi?batch={{.Batch.SampleBatch}}&target_label={{.TargetLabel}}&article_id={{.PrevArticleID}}">Previous</a>{{end}}
                {{if gt .NextArticleID 0}}<a href="/cgi-bin/audit.cgi?batch={{.Batch.SampleBatch}}&target_label={{.TargetLabel}}&article_id={{.NextArticleID}}">Next</a>{{end}}
                <a href="/cgi-bin/audit.cgi?batch={{.Batch.SampleBatch}}&target_label={{.TargetLabel}}">Next pending</a>
            </div>

            <div class="pill">review target: {{targetLabelDisplay .Article.TargetLabel}}</div>
            {{if .CurrentOutcome}}<div class="pill">review result: {{outcomeLabel .Article.TargetLabel .CurrentOutcome}}</div>{{end}}

            <p class="article-title">{{.Article.Title}}</p>
            <p class="meta">{{.Article.JournalName}} · {{yearLabel .Article.PubYear}} · article {{.Article.ArticleID}}{{if .Article.DOI}} · <a href="https://doi.org/{{.Article.DOI}}" target="_blank" rel="noopener noreferrer">{{.Article.DOI}}</a>{{end}}</p>
            <p class="small"><strong>Classifier flags:</strong> {{joinPhrases .Article}}</p>

            {{if .CurrentStatus}}
            <div class="current">
                Current review: <strong>{{.CurrentStatus}}</strong>{{if .Article.ReviewerUsername}} by {{.Article.ReviewerUsername}}{{end}}{{if .Article.ReviewedAt}} at {{formatTimestamp .Article.ReviewedAt}}{{end}}
            </div>
            {{end}}

            <h3>Abstract</h3>
            <div class="abstract">{{if .Article.Abstract}}{{.Article.Abstract}}{{else}}No abstract available.{{end}}</div>

            <form method="POST" action="/cgi-bin/audit-save.cgi">
                <input type="hidden" name="sample_batch" value="{{.Batch.SampleBatch}}">
                <input type="hidden" name="target_label" value="{{.TargetLabel}}">
                <input type="hidden" name="article_id" value="{{.Article.ArticleID}}">
                <h3>Reviewer Notes</h3>
                <textarea name="review_notes">{{.Article.ReviewNotes}}</textarea>
                <div class="actions">
                    <button class="btn-confirm" type="submit" name="target_confirmed" value="1">{{confirmButtonLabel .Article.TargetLabel}}</button>
                    <button class="btn-reject" type="submit" name="target_confirmed" value="0">{{rejectButtonLabel .Article.TargetLabel}}</button>
                    <button class="btn-stay" type="submit" name="action" value="stay">Save And Stay</button>
                </div>
                <p class="small">The green/red buttons save and continue to the next pending article for this target label. “Save And Stay” keeps the current article open.</p>
            </form>
        </div>
    </div>
</body>
</html>`))

func main() {
	if err := cgi.Serve(http.HandlerFunc(handleAudit)); err != nil {
		panic(err)
	}
}

func handleAudit(w http.ResponseWriter, r *http.Request) {
	config := GetConfig()
	db, err := OpenDatabase(config.DBPath)
	if err != nil {
		http.Error(w, "Failed to open audit database: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	batch := r.URL.Query().Get("batch")
	if batch == "" {
		batch, err = loadCurrentBatch(db)
		if err != nil {
			http.Error(w, "Failed to determine current batch: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if batch == "" {
		http.Error(w, "No audit batch has been loaded yet.", http.StatusNotFound)
		return
	}

	meta, err := loadBatchMeta(db, batch)
	if err != nil {
		http.Error(w, "Failed to load batch metadata: "+err.Error(), http.StatusInternalServerError)
		return
	}

	targetSummaries, err := loadTargetLabelSummaries(db, batch)
	if err != nil {
		http.Error(w, "Failed to load batch summary: "+err.Error(), http.StatusInternalServerError)
		return
	}

	targetLabel := r.URL.Query().Get("target_label")
	if targetLabel == "" {
		targetLabel = defaultTargetLabel(targetSummaries)
	}

	articleID := 0
	if rawID := r.URL.Query().Get("article_id"); rawID != "" {
		if parsed, err := strconv.Atoi(rawID); err == nil {
			articleID = parsed
		}
	}
	if articleID == 0 {
		articleID, err = firstPendingArticleID(db, batch, targetLabel)
		if err != nil {
			http.Error(w, "Failed to choose next article: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if articleID == 0 {
		http.Error(w, "No sampled articles found for this review target.", http.StatusNotFound)
		return
	}

	article, err := loadAuditArticle(db, batch, targetLabel, articleID)
	if err != nil {
		http.Error(w, "Failed to load article: "+err.Error(), http.StatusInternalServerError)
		return
	}

	prevID, nextID, err := adjacentArticleIDs(db, batch, targetLabel, articleID)
	if err != nil {
		http.Error(w, "Failed to load article navigation: "+err.Error(), http.StatusInternalServerError)
		return
	}

	data := AuditPageData{
		RemoteUser:      os.Getenv("REMOTE_USER"),
		Batch:           meta,
		TargetLabel:     targetLabel,
		Article:         article,
		PrevArticleID:   prevID,
		NextArticleID:   nextID,
		TargetSummaries: targetSummaries,
		CurrentStatus:   articleReviewStatus(article),
		CurrentOutcome:  articleOutcome(article),
	}
	if data.RemoteUser == "" {
		data.RemoteUser = "authenticated reviewer"
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := auditTemplate.Execute(w, data); err != nil {
		http.Error(w, "Template error: "+err.Error(), http.StatusInternalServerError)
	}
}
