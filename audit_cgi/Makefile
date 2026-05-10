SHARED = common.go fulltext_common.go

all: audit.cgi audit-save.cgi audit-status.cgi fulltext-audit.cgi fulltext-save.cgi fulltext-status.cgi fulltext-upload.cgi

audit.cgi: audit.go common.go
	go build -o audit.cgi audit.go common.go

audit-save.cgi: audit_save.go common.go
	go build -o audit-save.cgi audit_save.go common.go

audit-status.cgi: audit_status.go common.go
	go build -o audit-status.cgi audit_status.go common.go

fulltext-audit.cgi: fulltext_audit.go $(SHARED)
	go build -o fulltext-audit.cgi fulltext_audit.go $(SHARED)

fulltext-save.cgi: fulltext_save.go $(SHARED)
	go build -o fulltext-save.cgi fulltext_save.go $(SHARED)

fulltext-status.cgi: fulltext_status.go $(SHARED)
	go build -o fulltext-status.cgi fulltext_status.go $(SHARED)

fulltext-upload.cgi: fulltext_upload.go $(SHARED)
	go build -o fulltext-upload.cgi fulltext_upload.go $(SHARED)

clean:
	rm -f audit.cgi audit-save.cgi audit-status.cgi fulltext-audit.cgi fulltext-save.cgi fulltext-status.cgi fulltext-upload.cgi
