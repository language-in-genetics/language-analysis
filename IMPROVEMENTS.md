# IMPROVEMENTS.md

_Analysis date: 2026-07-11_

## What this project is

**Word Frequency Analysis / Language in Genetics** is a research pipeline that studies racial/ethnic terminology usage in genetics journals. It ingests CrossRef article metadata into PostgreSQL (`crossref` DB), runs OpenAI batch classification over the corpus (`extractor/` Python), generates static HTML dashboards, and supports a human audit workflow (`audit/`, `audit_cgi/`) with an authenticated review UI on `merah` plus nightly SQLite→PostgreSQL import. Go tools (`cmd/crossrefimport`, `cmd/pgjsontool`) handle bulk and incremental import. The repo is active — the latest commits (June–July 2026) added a Homo sapiens classifier pass and moved LIG full-text uploads out of SQLite. State on the analysis date is a working but sprawling multi-language codebase with meaningful uncommitted work in flight.

## Bugs & Fixes

- **`open()` without context manager for API key files** — `batchcheck.py:24`, `batchfetch.py:63`, `bulkquery.py:440`, `human_subject_batchfetch.py:134`, `human_subject_bulkquery.py:429` all do `open(args.openai_api_key).read().strip()`, leaking the file handle. `process_fulltext_analysis.py:539` already uses the better `Path(...).read_text()` idiom — standardize on that everywhere.
- **Bare-file API-key reads have no error handling** — a missing or empty key file yields an opaque traceback mid-run inside a cron job. Wrap with a clear "key file not found / empty" message so `cronscript.sh` failures are diagnosable from the log.
- Confirm the modified-but-uncommitted `extractor/*.py` (batchfetch, bulkquery, generate_dashboard, human_subject_*) changes are complete and intended before they drift further from HEAD (see Housekeeping).

## Unfinished / in-flight work (git status)

Working tree is dirty with a coherent but uncommitted feature:

- Untracked: `extractor/author_persistence_analysis.py`, `extractor/author_persistence_stats.py` (2066 lines), `extractor/test_author_persistence_stats.py`, and `paper/draft-sections/author-persistence-methods-results.md`.
- Modified: `.gitignore`, `README.md`, `cronscript.sh`, `extractor/batchfetch.py`, `extractor/bulkquery.py`, `extractor/generate_dashboard.py`, `extractor/human_subject_batchfetch.py`, `extractor/human_subject_bulkquery.py`, `paper/README.md`.

This is a substantial "author persistence" analysis + paper section that has never been committed. Land it (with its test) before it rots — a 2000-line untracked module is a data-loss risk.

## Security

- **No committed secrets found** — good. API keys are read from external files (`/home/languageingenetics/.openai.lig.key`) and DB creds come from the environment. Keep it that way.
- Ensure the audit CGI review UI on `merah` enforces auth on every endpoint (recent commit "Protect public full-text status page" suggests a page was briefly exposed). Audit the `audit_cgi/` entry points for any status/report route that leaks data or DOIs without authentication.

## Testing

- Only two test files exist: `test_retraction_stats.py` and the untracked `test_author_persistence_stats.py`. The core pipeline (`bulkquery.py`, `batchfetch.py`, dashboard generation, audit import) has no automated tests.
- Add tests around the classification result parsing and the SQLite→PostgreSQL audit import — these are the parts where silent data corruption is most costly for a research result.
- Wire a `make test` target (Makefile exists) that runs the Python tests via `uv run` and `go test ./...`.

## Documentation

- `SETUP.md` and `README.md` still instruct `python -m venv .venv` + `pip install -r ../requirements.txt`, but `cronscript.sh` already uses `uv run`. The docs contradict the production runner — update them to the uv workflow.
- Document the `author_persistence_*` scripts once committed (what question they answer, inputs, outputs).
- `PERFORMANCE_OPTIMIZATIONS.md` and the several `database/*_redesign.md` design docs are good; add a top-level index in README linking the incremental-import design docs so they are discoverable.

## Housekeeping / Modernization

- **Migrate off `requirements.txt` to uv.** The repo already runs via `uv run` in `cronscript.sh`, but there is no `pyproject.toml` — only a two-line `requirements.txt` (`openai`, `psycopg2-binary`). Create a `pyproject.toml`, run `uv add openai psycopg2-binary` (and any others the scripts import), commit the resulting `uv.lock`, and delete `requirements.txt`. Then update `SETUP.md`/`README.md` accordingly. This matches the owner's stated preference and removes the venv/pip instructions entirely.
- Consider consolidating the near-duplicate `retraction_statistics.py`/`retraction_stats.py` and `random_sample.py`/`quick_random_sample.py` pairs — the parallel `*_stats.py` vs `*_analysis.py` naming suggests copy-paste divergence.
- The `human_subject_*` scripts are near-clones of their non-human-subject counterparts. Factor the shared batch/query logic into a module in `internal/` (Go) or a shared Python module to cut the maintenance surface.

## Quick Wins

1. `git add -A` and commit the author-persistence feature + test (protect 2000+ untracked lines).
2. Replace the five `open(key).read()` calls with `Path(key).read_text().strip()`.
3. Add `pyproject.toml` + `uv.lock`, drop `requirements.txt`, fix SETUP.md.
4. Add a `make test` target running the existing Python and Go tests.
