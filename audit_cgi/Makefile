FULLTEXT_FILES = common.go fulltext_common.go
HUMAN_SUBJECT_FILES = common.go human_subject_common.go

all: audit.cgi audit-save.cgi audit-status.cgi audit-human-subject.cgi audit-human-subject-save.cgi audit-human-subject-status.cgi fulltext-audit.cgi fulltext-verify.cgi fulltext-save.cgi fulltext-status.cgi fulltext-upload.cgi

audit.cgi: audit.go common.go
	go build -o audit.cgi audit.go common.go

audit-save.cgi: audit_save.go common.go
	go build -o audit-save.cgi audit_save.go common.go

audit-status.cgi: audit_status.go common.go
	go build -o audit-status.cgi audit_status.go common.go

fulltext-audit.cgi: fulltext_audit.go $(FULLTEXT_FILES)
	go build -o fulltext-audit.cgi fulltext_audit.go $(FULLTEXT_FILES)

fulltext-verify.cgi: fulltext_audit.go $(FULLTEXT_FILES)
	go build -o fulltext-verify.cgi fulltext_audit.go $(FULLTEXT_FILES)

fulltext-save.cgi: fulltext_save.go $(FULLTEXT_FILES)
	go build -o fulltext-save.cgi fulltext_save.go $(FULLTEXT_FILES)

fulltext-status.cgi: fulltext_status.go $(FULLTEXT_FILES)
	go build -o fulltext-status.cgi fulltext_status.go $(FULLTEXT_FILES)

fulltext-upload.cgi: fulltext_upload.go $(FULLTEXT_FILES)
	go build -o fulltext-upload.cgi fulltext_upload.go $(FULLTEXT_FILES)

audit-human-subject.cgi: human_subject_audit.go $(HUMAN_SUBJECT_FILES)
	go build -o audit-human-subject.cgi human_subject_audit.go $(HUMAN_SUBJECT_FILES)

audit-human-subject-save.cgi: human_subject_save.go $(HUMAN_SUBJECT_FILES)
	go build -o audit-human-subject-save.cgi human_subject_save.go $(HUMAN_SUBJECT_FILES)

audit-human-subject-status.cgi: human_subject_status.go $(HUMAN_SUBJECT_FILES)
	go build -o audit-human-subject-status.cgi human_subject_status.go $(HUMAN_SUBJECT_FILES)

clean:
	rm -f audit.cgi audit-save.cgi audit-status.cgi audit-human-subject.cgi audit-human-subject-save.cgi audit-human-subject-status.cgi fulltext-audit.cgi fulltext-verify.cgi fulltext-save.cgi fulltext-status.cgi fulltext-upload.cgi
