package main

import (
	"html/template"
	"net/http"
	"net/http/cgi"
	"strconv"
)

type HumanSubjectStatusPageData struct {
	Batch      HumanSubjectBatchMeta
	Summary    HumanSubjectSummary
	Status     string
	AIDecision string
	Articles   []HumanSubjectArticleRow
	Detail     *HumanSubjectArticle
}

var humanSubjectStatusTemplate = template.Must(template.New("human-subject-status").Funcs(templateFuncs).Funcs(template.FuncMap{
	"humanSubjectBoolLabel":     humanSubjectBoolLabel,
	"humanSubjectReviewerLabel": humanSubjectReviewerLabel,
	"humanSubjectSummaryLabel":  humanSubjectSummaryLabel,
	"humanSubjectOutcome":       humanSubjectOutcome,
	"humanSubjectReviewStatus":  humanSubjectReviewStatus,
}).Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>LIG Homo Sapiens Audit Status</title>
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
            <h1>LIG Homo Sapiens Audit Status</h1>
            <p class="small">This status page uses the audit login. The editing interface lives at <a href="/cgi-bin/audit-human-subject.cgi?batch={{.Batch.BatchSlug}}">/cgi-bin/audit-human-subject.cgi</a>. Terminology audit status is at <a href="/cgi-bin/audit-status.cgi">/cgi-bin/audit-status.cgi</a>.</p>
            <p class="small">Batch <code>{{.Batch.BatchSlug}}</code> · created {{formatTimestamp .Batch.CreatedAt}} · seed {{.Batch.Seed}}</p>
            <div class="filters">
                <a href="/cgi-bin/audit-human-subject-status.cgi?batch={{.Batch.BatchSlug}}">All</a>
                <a href="/cgi-bin/audit-human-subject-status.cgi?batch={{.Batch.BatchSlug}}&ai_decision=human">Bot: about humans</a>
                <a href="/cgi-bin/audit-human-subject-status.cgi?batch={{.Batch.BatchSlug}}&ai_decision=not_human">Bot: not about humans</a>
                <a href="/cgi-bin/audit-human-subject-status.cgi?batch={{.Batch.BatchSlug}}&status=pending">Pending only</a>
                <a href="/cgi-bin/audit-human-subject-status.cgi?batch={{.Batch.BatchSlug}}&status=reviewed">Reviewed only</a>
            </div>
            <div class="stats">
                <div class="stat"><strong>Progress</strong><br>{{humanSubjectSummaryLabel .Summary}}</div>
                <div class="stat"><strong>Bot sample split</strong><br>{{.Summary.AIHumanCount}} about humans<br>{{.Summary.AINotHumanCount}} not about humans</div>
                <div class="stat"><strong>Reviewer labels</strong><br>{{.Summary.ReviewerHumanCount}} about humans<br>{{.Summary.ReviewerNotHumanCount}} not about humans</div>
            </div>
        </div>

        {{if .Detail}}
        <div class="card">
            <h2>{{.Detail.Title}}</h2>
            <p class="small">{{.Detail.JournalName}} · {{yearLabel .Detail.PubYear}} · classifier {{.Detail.ClassificationID}}{{if .Detail.DOI}} · <a href="https://doi.org/{{.Detail.DOI}}" target="_blank" rel="noopener noreferrer">{{.Detail.DOI}}</a>{{end}}</p>
            <p class="small">Bot label: <strong>{{humanSubjectBoolLabel .Detail.AIAboutHumans}}</strong>{{if .Detail.AIConfidence}} · confidence {{.Detail.AIConfidence}}{{end}}{{if .Detail.AIEvidence}} · evidence: {{.Detail.AIEvidence}}{{end}}</p>
            <p class="small">Reviewer label: <strong>{{humanSubjectReviewerLabel .Detail.ReviewerAboutHumans}}</strong>{{if .Detail.ReviewerUsername}} · reviewer {{.Detail.ReviewerUsername}}{{end}}{{if .Detail.ReviewedAt}} · {{formatTimestamp .Detail.ReviewedAt}}{{end}}</p>
            <p class="small">Audit result: <strong>{{humanSubjectOutcome .Detail}}</strong></p>
            <div class="abstract">{{if .Detail.Abstract}}{{.Detail.Abstract}}{{else}}No abstract available.{{end}}</div>
        </div>
        {{end}}

        <div class="card">
            <table>
                <thead>
                    <tr>
                        <th>Classifier</th>
                        <th>Article</th>
                        <th>Journal</th>
                        <th>Bot</th>
                        <th>Status</th>
                        <th>Reviewer</th>
                        <th>Outcome</th>
                        <th>Updated</th>
                    </tr>
                </thead>
                <tbody>
                    {{range .Articles}}
                    <tr>
                        <td>{{.ClassificationID}}</td>
                        <td><a href="/cgi-bin/audit-human-subject-status.cgi?batch={{.BatchSlug}}&classification_id={{.ClassificationID}}">{{.Title}}</a>{{if .DOI}}<div class="small">{{.DOI}}</div>{{end}}</td>
                        <td>{{.JournalName}}<div class="small">{{yearLabel .PubYear}}</div></td>
                        <td>{{humanSubjectBoolLabel .AIAboutHumans}}{{if .AIConfidence}}<div class="small">{{.AIConfidence}}</div>{{end}}</td>
                        <td class="{{.ReviewStatus}}">{{.ReviewStatus}}</td>
                        <td>{{humanSubjectReviewerLabel .ReviewerAboutHumans}}{{if .ReviewerUsername}}<div class="small">{{.ReviewerUsername}}</div>{{end}}</td>
                        <td>{{.AuditOutcome}}</td>
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
	if err := cgi.Serve(http.HandlerFunc(handleHumanSubjectStatus)); err != nil {
		panic(err)
	}
}

func handleHumanSubjectStatus(w http.ResponseWriter, r *http.Request) {
	config := GetConfig()
	db, err := OpenDatabase(config.DBPath)
	if err != nil {
		http.Error(w, "Failed to open audit database: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	batch := r.URL.Query().Get("batch")
	if batch == "" {
		batch, err = loadCurrentHumanSubjectBatch(db)
		if err != nil {
			http.Error(w, "Failed to determine current Homo sapiens audit batch: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if batch == "" {
		http.Error(w, "No Homo sapiens audit batch has been loaded yet.", http.StatusNotFound)
		return
	}

	status := r.URL.Query().Get("status")
	if status == "" {
		status = "all"
	}
	aiDecision := r.URL.Query().Get("ai_decision")

	meta, err := loadHumanSubjectBatchMeta(db, batch)
	if err != nil {
		http.Error(w, "Failed to load batch metadata: "+err.Error(), http.StatusInternalServerError)
		return
	}
	summary, err := loadHumanSubjectSummary(db, batch)
	if err != nil {
		http.Error(w, "Failed to load batch summary: "+err.Error(), http.StatusInternalServerError)
		return
	}
	articles, err := listHumanSubjectArticles(db, batch, status, aiDecision)
	if err != nil {
		http.Error(w, "Failed to list Homo sapiens audit articles: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var detail *HumanSubjectArticle
	if rawID := r.URL.Query().Get("classification_id"); rawID != "" {
		if classificationID, err := strconv.Atoi(rawID); err == nil {
			loaded, err := loadHumanSubjectArticle(db, batch, classificationID)
			if err == nil {
				detail = &loaded
			}
		}
	}

	data := HumanSubjectStatusPageData{
		Batch:      meta,
		Summary:    summary,
		Status:     status,
		AIDecision: aiDecision,
		Articles:   articles,
		Detail:     detail,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := humanSubjectStatusTemplate.Execute(w, data); err != nil {
		http.Error(w, "Template error: "+err.Error(), http.StatusInternalServerError)
	}
}
