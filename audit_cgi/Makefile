all: audit.cgi audit-save.cgi audit-status.cgi

audit.cgi: audit.go common.go
	go build -o audit.cgi audit.go common.go

audit-save.cgi: audit_save.go common.go
	go build -o audit-save.cgi audit_save.go common.go

audit-status.cgi: audit_status.go common.go
	go build -o audit-status.cgi audit_status.go common.go

clean:
	rm -f audit.cgi audit-save.cgi audit-status.cgi
