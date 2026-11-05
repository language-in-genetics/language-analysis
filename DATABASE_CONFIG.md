# Production database configuration

Prepared 5 November 2026; live Crossref remains on PG16 pending the verified
cutover. These changes do not enable paused processing.

| Setting | Production location / value |
| --- | --- |
| Host / account | Raksasa / `languageingenetics` |
| Checkout | `/home/languageingenetics/Word-Frequency-Analysis-` |
| Database / schema / service role | `crossref` / `languageingenetics` / `languageingenetics` |
| Current / prepared port | PG16/5432 / PG18/5433 |
| Local service endpoint | Unix socket `/var/run/postgresql`, existing peer authentication |
| Shared Python connection | `extractor/db_connection.py:connect_pg18()` |
| Main wrapper | `cronscript.sh`; account cron entries are currently commented out |

Python connection precedence is explicit DSN, explicit keyword arguments, then
PGDATABASE/PGHOST/PGPORT/PGUSER. Missing database/host/port defaults become
crossref, the local socket and 5433. The connected database, user, port and
server major are checked before returning the connection to processing code.
LIG_EXPECTED_DATABASE allows an explicitly selected isolated rehearsal database.
`cd extractor && uv run db_connection.py` checks identity without submitting or
fetching a batch, importing results, generating a site or publishing.

The verified local socket route needs no password. Existing TCP/workstation
usage may use that caller's `.pgpass`; credentials and availability on Udara
were not inspected or changed. Use explicit host=raksasa port=5433 dbname=crossref
user=languageingenetics when such a route is deliberately used after cutover.
Do not add secrets to Git. Never infer that an old documentation example proves
a currently active off-host writer.

The Raksasa batch-check and full processing cron entries were paused on
20 August 2026 after exhausting the 2026 corpus. Preserve that pause. No new
OpenAI work, full corpus import or cron reactivation is migration acceptance.
The Merah audit workflow uses a separate SQLite review cache; manual Raksasa
importers transfer its reviews into PostgreSQL. Preserve all caches, histories,
batch IDs and receipts, including pending external work.

Historical Go importers and administrator shell tools remain manual utilities.
They must use explicit PGPORT=5433 and an identity preflight if deliberately
invoked; they were not run or revived here. The old database and focused
research recovery archives retain original IDs. After new PG18 writes, rollback
requires reconciliation before reopening PG16 for writes.
