package main

import (
	"html/template"
	"net/http"
	"net/http/cgi"
	"strconv"
)

type StatusPageData struct {
	Batch           BatchMeta
	TargetLabel     string
	Status          string
	Articles        []ArticleRow
	TargetSummaries []TargetLabelSummary
	Detail          *AuditArticle
}

var statusTemplate = template.Must(template.New("status").Funcs(templateFuncs).Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>LIG Audit Status</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #f6f7f9; color: #222; margin: 0; }
        .container { max-width: 1300px; margin: 0 auto; padding: 24px; }
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
        .abstract { white-space: pre-wrap; line-height: 1.55; }
        .small { color: #666; font-size: 0.9rem; }
        .pending { color: #8a5300; }
        .reviewed { color: #146a35; }
    </style>
</head>
<body>
    <div class="container">
        <div class="card">
            <h1>LIG Audit Status</h1>
            <p class="small">This page is public. The editing interface lives at <a href="/cgi-bin/audit.cgi{{queryWithBatch .Batch.SampleBatch}}">/cgi-bin/audit.cgi</a> and requires login. Full-text verification status is at <a href="/cgi-bin/fulltext-status.cgi">/cgi-bin/fulltext-status.cgi</a>.</p>
            <p class="small">Batch <code>{{.Batch.SampleBatch}}</code> · created {{formatTimestamp .Batch.CreatedAt}} · seed {{.Batch.Seed}}</p>
            <div class="filters">
                <a href="/cgi-bin/audit-status.cgi?batch={{.Batch.SampleBatch}}">All</a>
                {{range .TargetSummaries}}
                <a href="/cgi-bin/audit-status.cgi?batch={{$.Batch.SampleBatch}}&target_label={{.TargetLabel}}">{{targetLabelDisplay .TargetLabel}}</a>
                {{end}}
                <a href="/cgi-bin/audit-status.cgi?batch={{.Batch.SampleBatch}}&status=pending">Pending only</a>
                <a href="/cgi-bin/audit-status.cgi?batch={{.Batch.SampleBatch}}&status=reviewed">Reviewed only</a>
            </div>
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

        {{if .Detail}}
        <div class="card">
            <h2>{{.Detail.Title}}</h2>
            <p class="small">{{.Detail.JournalName}} · {{yearLabel .Detail.PubYear}} · article {{.Detail.ArticleID}}{{if .Detail.DOI}} · <a href="https://doi.org/{{.Detail.DOI}}" target="_blank" rel="noopener noreferrer">{{.Detail.DOI}}</a>{{end}}</p>
            <p class="small">Review target: <strong>{{targetLabelDisplay .Detail.TargetLabel}}</strong>. Review status: <strong>{{articleReviewStatus .Detail}}</strong></p>
            <p class="small">Audit result: <strong>{{outcomeLabel .Detail.TargetLabel (articleOutcome .Detail)}}</strong>{{if .Detail.ReviewerUsername}} · reviewer {{.Detail.ReviewerUsername}}{{end}}{{if .Detail.ReviewedAt}} · {{formatTimestamp .Detail.ReviewedAt}}{{end}}</p>
            <p class="small">Classifier flags: {{joinPhrases .Detail}}</p>
            <div class="abstract">{{if .Detail.Abstract}}{{.Detail.Abstract}}{{else}}No abstract available.{{end}}</div>
        </div>
        {{end}}

        <div class="card">
            <table>
                <thead>
                    <tr>
                        <th>Target</th>
                        <th>Article</th>
                        <th>Journal</th>
                        <th>Status</th>
                        <th>Outcome</th>
                        <th>Reviewer</th>
                        <th>Updated</th>
                    </tr>
                </thead>
                <tbody>
                    {{range .Articles}}
                    <tr>
                        <td>{{targetLabelDisplay .TargetLabel}}</td>
                        <td><a href="/cgi-bin/audit-status.cgi?batch={{.SampleBatch}}&target_label={{.TargetLabel}}&article_id={{.ArticleID}}">{{.Title}}</a><div class="small">article {{.ArticleID}}</div></td>
                        <td>{{.JournalName}}<div class="small">{{yearLabel .PubYear}}</div></td>
                        <td class="{{.ReviewStatus}}">{{.ReviewStatus}}</td>
                        <td>{{outcomeLabel .TargetLabel .AuditOutcome}}</td>
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
	if err := cgi.Serve(http.HandlerFunc(handleAuditStatus)); err != nil {
		panic(err)
	}
}

func handleAuditStatus(w http.ResponseWriter, r *http.Request) {
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

	targetLabel := r.URL.Query().Get("target_label")
	status := r.URL.Query().Get("status")
	if status == "" {
		status = "all"
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
	articles, err := listAuditArticles(db, batch, targetLabel, status)
	if err != nil {
		http.Error(w, "Failed to list audit articles: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var detail *AuditArticle
	if rawID := r.URL.Query().Get("article_id"); rawID != "" {
		if articleID, err := strconv.Atoi(rawID); err == nil {
			detailTargetLabel := targetLabel
			if detailTargetLabel == "" {
				detailTargetLabel = defaultTargetLabel(targetSummaries)
			}
			loaded, err := loadAuditArticle(db, batch, detailTargetLabel, articleID)
			if err == nil {
				detail = &loaded
			}
		}
	}

	data := StatusPageData{
		Batch:           meta,
		TargetLabel:     targetLabel,
		Status:          status,
		Articles:        articles,
		TargetSummaries: targetSummaries,
		Detail:          detail,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := statusTemplate.Execute(w, data); err != nil {
		http.Error(w, "Template error: "+err.Error(), http.StatusInternalServerError)
	}
}
