package main

import (
	"html/template"
	"net/http"
	"net/http/cgi"
	"strconv"
)

type FulltextStatusPageData struct {
	Batch          FulltextBatchMeta
	Summary        FulltextSummary
	ReviewStatus   string
	FulltextStatus string
	Articles       []FulltextArticleRow
	Detail         *FulltextArticle
}

var fulltextStatusTemplate = template.Must(template.New("fulltext-status").Funcs(templateFuncs).Funcs(template.FuncMap{
	"fulltextAITermList":    fulltextAITermList,
	"fulltextStatusDisplay": fulltextStatusDisplay,
	"fulltextTermList":      fulltextTermList,
	"fulltextOutcome":       fulltextOutcome,
	"fulltextReviewStatus":  fulltextReviewStatus,
	"fulltextSummaryLabel":  fulltextSummaryLabel,
}).Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>LIG Full-Text Verification Status</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #f6f7f9; color: #222; margin: 0; }
        .container { max-width: 1320px; margin: 0 auto; padding: 24px; }
        .card { background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.08); margin-bottom: 20px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { border-bottom: 1px solid #e8eaed; padding: 10px; text-align: left; vertical-align: top; }
        th { background: #f8f9fb; }
        a { color: #0b63ce; text-decoration: none; }
        a:hover { text-decoration: underline; }
        .filters { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 12px; }
        .filters a { background: #edf2f7; padding: 8px 12px; border-radius: 8px; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }
        .stat { background: #f9fafb; border-radius: 8px; padding: 12px; }
        .abstract, .fulltext { white-space: pre-wrap; line-height: 1.55; }
        .fulltext { max-height: 420px; overflow: auto; border: 1px solid #dde2e6; border-radius: 8px; padding: 14px; background: #fbfcfd; }
        .small { color: #666; font-size: 0.9rem; }
        .pending { color: #8a5300; }
        .reviewed { color: #146a35; }
        code { background: #f1f3f5; padding: 2px 4px; border-radius: 4px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="card">
            <h1>LIG Full-Text Verification Status</h1>
            <p class="small">This page is public. The verification interface lives at <a href="/cgi-bin/fulltext-verify.cgi?batch={{.Batch.BatchSlug}}">/cgi-bin/fulltext-verify.cgi</a> and requires login.</p>
            <p class="small">Batch <code>{{.Batch.BatchSlug}}</code> · created {{formatTimestamp .Batch.CreatedAt}} · seed {{.Batch.Seed}}</p>
            <div class="filters">
                <a href="/cgi-bin/fulltext-status.cgi?batch={{.Batch.BatchSlug}}">All</a>
                <a href="/cgi-bin/fulltext-status.cgi?batch={{.Batch.BatchSlug}}&review_status=pending">Pending verification</a>
                <a href="/cgi-bin/fulltext-status.cgi?batch={{.Batch.BatchSlug}}&review_status=reviewed">Verified</a>
                <a href="/cgi-bin/fulltext-status.cgi?batch={{.Batch.BatchSlug}}&fulltext_status=available">Full text available</a>
                <a href="/cgi-bin/fulltext-status.cgi?batch={{.Batch.BatchSlug}}&fulltext_status=needs_manual">Needs manual</a>
                <a href="/cgi-bin/fulltext-status.cgi?batch={{.Batch.BatchSlug}}&fulltext_status=pending_fetch">Pending fetch</a>
            </div>
            <div class="stats">
                <div class="stat"><strong>Verification progress</strong><br>{{fulltextSummaryLabel .Summary}}<br>{{.Summary.PendingCount}} pending</div>
                <div class="stat"><strong>Full text</strong><br>{{.Summary.AvailableCount}} available<br>{{.Summary.PendingFetchCount}} pending fetch · {{.Summary.NeedsManualCount}} needs manual</div>
                <div class="stat"><strong>Unavailable</strong><br>{{.Summary.UnavailableCount}} unavailable<br>{{.Summary.ExtractionFailedCount}} extraction failed</div>
            </div>
        </div>

        {{if .Detail}}
        <div class="card">
            <h2>{{.Detail.Title}}</h2>
            <p class="small">{{.Detail.JournalName}} · {{yearLabel .Detail.PubYear}} · article {{.Detail.ArticleID}}{{if .Detail.DOI}} · <a href="https://doi.org/{{.Detail.DOI}}" target="_blank" rel="noopener noreferrer">{{.Detail.DOI}}</a>{{end}}</p>
            <p class="small">Verification status: <strong>{{fulltextReviewStatus .Detail}}</strong>. Verification result: <strong>{{fulltextOutcome .Detail}}</strong>{{if .Detail.ReviewerUsername}} · verifier {{.Detail.ReviewerUsername}}{{end}}{{if .Detail.ReviewedAt}} · {{formatTimestamp .Detail.ReviewedAt}}{{end}}</p>
            <p class="small">Full text: <strong>{{fulltextStatusDisplay .Detail.FulltextStatus}}</strong>{{if .Detail.FulltextSource}} · source {{.Detail.FulltextSource}}{{end}}</p>
            <p class="small">AI analysis: <strong>{{.Detail.AIAnalysisStatus}}</strong>{{if eq .Detail.AIAnalysisStatus "processed"}} · {{fulltextAITermList .Detail}}{{end}}{{if .Detail.AIError}} · {{.Detail.AIError}}{{end}}</p>
            <p class="small">Terms marked: {{fulltextTermList .Detail}}</p>
            {{if .Detail.QuotedEvidence}}<h3>Quoted Evidence</h3><div class="abstract">{{.Detail.QuotedEvidence}}</div>{{end}}
            <h3>Abstract</h3>
            <div class="abstract">{{if .Detail.Abstract}}{{.Detail.Abstract}}{{else}}No abstract available.{{end}}</div>
            {{if .Detail.ExtractedText}}<h3>Full Article Text</h3><div class="fulltext">{{.Detail.ExtractedText}}</div>{{end}}
        </div>
        {{end}}

        <div class="card">
            <table>
                <thead>
                    <tr>
                        <th>Article</th>
                        <th>Journal</th>
                        <th>Full text</th>
                        <th>AI</th>
                        <th>Verification</th>
                        <th>Result</th>
                        <th>Verifier</th>
                        <th>Updated</th>
                    </tr>
                </thead>
                <tbody>
                    {{range .Articles}}
                    <tr>
                        <td><a href="/cgi-bin/fulltext-status.cgi?batch={{.BatchSlug}}&article_id={{.ArticleID}}">{{.Title}}</a><div class="small">article {{.ArticleID}}</div></td>
                        <td>{{.JournalName}}<div class="small">{{yearLabel .PubYear}}</div></td>
                        <td>{{fulltextStatusDisplay .FulltextStatus}}{{if .FulltextSource}}<div class="small">{{.FulltextSource}}</div>{{end}}</td>
                        <td>{{.AIAnalysisStatus}}{{if eq .AIAnalysisStatus "processed"}}<div class="small">{{fulltextAITermList .}}</div>{{end}}</td>
                        <td class="{{.ReviewStatus}}">{{.ReviewStatus}}</td>
                        <td>{{.AuditOutcome}}</td>
                        <td>{{if .ReviewerUsername}}{{.ReviewerUsername}}{{else}}—{{end}}</td>
                        <td>{{if .UpdatedAt}}{{formatTimestamp .UpdatedAt}}{{else}}—{{end}}</td>
                    </tr>
                    {{end}}
                </tbody>
            </table>
        </div>
    </div>
</body>
</html>`))

func main() {
	if err := cgi.Serve(http.HandlerFunc(handleFulltextStatus)); err != nil {
		panic(err)
	}
}

func handleFulltextStatus(w http.ResponseWriter, r *http.Request) {
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
		http.Error(w, "No full-text verification batch has been loaded yet.", http.StatusNotFound)
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

	reviewStatus := r.URL.Query().Get("review_status")
	fulltextStatus := r.URL.Query().Get("fulltext_status")
	articles, err := listFulltextArticles(db, batch, reviewStatus, fulltextStatus)
	if err != nil {
		http.Error(w, "Failed to list full-text verification articles: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var detail *FulltextArticle
	if rawID := r.URL.Query().Get("article_id"); rawID != "" {
		if articleID, err := strconv.Atoi(rawID); err == nil {
			loaded, err := loadFulltextArticle(db, batch, articleID)
			if err == nil {
				detail = &loaded
			}
		}
	}

	data := FulltextStatusPageData{
		Batch:          meta,
		Summary:        summary,
		ReviewStatus:   reviewStatus,
		FulltextStatus: fulltextStatus,
		Articles:       articles,
		Detail:         detail,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := fulltextStatusTemplate.Execute(w, data); err != nil {
		http.Error(w, "Template error: "+err.Error(), http.StatusInternalServerError)
	}
}
