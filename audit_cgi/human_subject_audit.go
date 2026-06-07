package main

import (
	"html/template"
	"net/http"
	"net/http/cgi"
	"os"
	"strconv"
)

type HumanSubjectAuditPageData struct {
	RemoteUser           string
	Batch                HumanSubjectBatchMeta
	Summary              HumanSubjectSummary
	Article              HumanSubjectArticle
	PrevClassificationID int
	NextClassificationID int
	CurrentStatus        string
	CurrentOutcome       string
}

var humanSubjectAuditTemplate = template.Must(template.New("human-subject-audit").Funcs(templateFuncs).Funcs(template.FuncMap{
	"humanSubjectBoolLabel":     humanSubjectBoolLabel,
	"humanSubjectReviewerLabel": humanSubjectReviewerLabel,
	"humanSubjectSummaryLabel":  humanSubjectSummaryLabel,
}).Parse(`<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>LIG Homo Sapiens Audit</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; background: #f6f7f9; color: #222; }
        .container { max-width: 1200px; margin: 0 auto; padding: 24px; }
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
        .article-title { font-size: 1.4rem; margin-bottom: 12px; }
        .abstract { white-space: pre-wrap; line-height: 1.55; }
        textarea { width: 100%; min-height: 120px; font: inherit; padding: 10px; box-sizing: border-box; }
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
                <h1>LIG Homo Sapiens Audit</h1>
                <div class="meta">Signed in as <strong>{{.RemoteUser}}</strong> · batch <code>{{.Batch.BatchSlug}}</code></div>
            </div>
            <div class="nav">
                <a href="/cgi-bin/audit-human-subject-status.cgi?batch={{.Batch.BatchSlug}}">Status</a>
                <a href="/cgi-bin/audit.cgi">Terminology audit</a>
                <a href="/cgi-bin/fulltext-upload.cgi">Full-text AI upload</a>
            </div>
        </div>

        <div class="card">
            <div class="stats">
                <div class="stat"><strong>Progress</strong><br>{{humanSubjectSummaryLabel .Summary}}</div>
                <div class="stat"><strong>Bot sample split</strong><br>{{.Summary.AIHumanCount}} about humans<br>{{.Summary.AINotHumanCount}} not about humans</div>
                <div class="stat"><strong>Reviewer labels</strong><br>{{.Summary.ReviewerHumanCount}} about humans<br>{{.Summary.ReviewerNotHumanCount}} not about humans</div>
            </div>
        </div>

        <div class="card">
            <div class="nav">
                {{if gt .PrevClassificationID 0}}<a href="/cgi-bin/audit-human-subject.cgi?batch={{.Batch.BatchSlug}}&classification_id={{.PrevClassificationID}}">Previous</a>{{end}}
                {{if gt .NextClassificationID 0}}<a href="/cgi-bin/audit-human-subject.cgi?batch={{.Batch.BatchSlug}}&classification_id={{.NextClassificationID}}">Next</a>{{end}}
                <a href="/cgi-bin/audit-human-subject.cgi?batch={{.Batch.BatchSlug}}">Next pending</a>
            </div>

            <div class="pill">bot: {{humanSubjectBoolLabel .Article.AIAboutHumans}}</div>
            {{if .Article.AIConfidence}}<div class="pill">confidence: {{.Article.AIConfidence}}</div>{{end}}
            {{if .CurrentOutcome}}<div class="pill">audit result: {{.CurrentOutcome}}</div>{{end}}

            <p class="article-title">{{.Article.Title}}</p>
            <p class="meta">{{.Article.JournalName}} · {{yearLabel .Article.PubYear}} · classifier {{.Article.ClassificationID}}{{if .Article.DOI}} · <a href="https://doi.org/{{.Article.DOI}}" target="_blank" rel="noopener noreferrer">{{.Article.DOI}}</a>{{end}}</p>
            {{if .Article.AIEvidence}}<p class="small"><strong>Bot evidence:</strong> {{.Article.AIEvidence}}</p>{{end}}
            {{if .Article.AIModel}}<p class="small"><strong>Bot model:</strong> {{.Article.AIModel}}</p>{{end}}

            {{if .CurrentStatus}}
            <div class="current">
                Current review: <strong>{{.CurrentStatus}}</strong>{{if .Article.ReviewerUsername}} by {{.Article.ReviewerUsername}}{{end}}{{if .Article.ReviewedAt}} at {{formatTimestamp .Article.ReviewedAt}}{{end}}.
                Reviewer label: <strong>{{humanSubjectReviewerLabel .Article.ReviewerAboutHumans}}</strong>
            </div>
            {{end}}

            <h3>Abstract</h3>
            <div class="abstract">{{if .Article.Abstract}}{{.Article.Abstract}}{{else}}No abstract available.{{end}}</div>

            <form method="POST" action="/cgi-bin/audit-human-subject-save.cgi">
                <input type="hidden" name="batch" value="{{.Batch.BatchSlug}}">
                <input type="hidden" name="classification_id" value="{{.Article.ClassificationID}}">
                <h3>Reviewer Notes</h3>
                <textarea name="review_notes">{{.Article.ReviewNotes}}</textarea>
                <div class="actions">
                    <button class="btn-confirm" type="submit" name="reviewer_about_humans" value="1">Paper Is About Humans</button>
                    <button class="btn-reject" type="submit" name="reviewer_about_humans" value="0">Paper Is Not About Humans</button>
                    <button class="btn-stay" type="submit" name="action" value="stay">Save And Stay</button>
                </div>
                <p class="small">Choose the human label from the title and abstract. The interface derives whether the bot was correct from your label.</p>
            </form>
        </div>
    </div>
</body>
</html>`))

func main() {
	if err := cgi.Serve(http.HandlerFunc(handleHumanSubjectAudit)); err != nil {
		panic(err)
	}
}

func handleHumanSubjectAudit(w http.ResponseWriter, r *http.Request) {
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

	classificationID := 0
	if rawID := r.URL.Query().Get("classification_id"); rawID != "" {
		if parsed, err := strconv.Atoi(rawID); err == nil {
			classificationID = parsed
		}
	}
	if classificationID == 0 {
		classificationID, err = firstPendingHumanSubjectClassificationID(db, batch)
		if err != nil {
			http.Error(w, "Failed to choose next classification: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if classificationID == 0 {
		http.Error(w, "No sampled Homo sapiens audit articles found.", http.StatusNotFound)
		return
	}

	article, err := loadHumanSubjectArticle(db, batch, classificationID)
	if err != nil {
		http.Error(w, "Failed to load Homo sapiens audit article: "+err.Error(), http.StatusInternalServerError)
		return
	}

	prevID, nextID, err := adjacentHumanSubjectClassificationIDs(db, batch, classificationID)
	if err != nil {
		http.Error(w, "Failed to load article navigation: "+err.Error(), http.StatusInternalServerError)
		return
	}

	remoteUser := os.Getenv("REMOTE_USER")
	if remoteUser == "" {
		remoteUser = "authenticated reviewer"
	}

	data := HumanSubjectAuditPageData{
		RemoteUser:           remoteUser,
		Batch:                meta,
		Summary:              summary,
		Article:              article,
		PrevClassificationID: prevID,
		NextClassificationID: nextID,
		CurrentStatus:        humanSubjectReviewStatus(article),
		CurrentOutcome:       humanSubjectOutcome(article),
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := humanSubjectAuditTemplate.Execute(w, data); err != nil {
		http.Error(w, "Template error: "+err.Error(), http.StatusInternalServerError)
	}
}
