package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
)

func TestParseConnectionsExcelFileReadsRowsAndGroupPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "connections.xlsx")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create excel: %v", err)
	}
	writer, err := newXLSXExportFileWriter(file, 0)
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}
	if err := writer.SetColumns([]string{"name", "type", "host", "port", "group"}); err != nil {
		t.Fatalf("set columns: %v", err)
	}
	if err := writer.ConsumeRow(map[string]interface{}{
		"name": "finance-db", "type": "mysql", "host": "10.0.0.8", "port": "3306", "group": "prod/finance",
	}); err != nil {
		t.Fatalf("write row: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	parsed, err := parseConnectionsExcelFile(path)
	if err != nil {
		t.Fatalf("parse excel: %v", err)
	}
	if len(parsed.Inputs) != 1 {
		t.Fatalf("inputs = %d, want 1", len(parsed.Inputs))
	}
	got := parsed.Inputs[0]
	if got.Name != "finance-db" || got.Config.Host != "10.0.0.8" || got.Config.Port != 3306 {
		t.Fatalf("unexpected connection: %+v", got)
	}
	if len(parsed.Groups) != 1 || parsed.Groups[0].GroupPath != "prod/finance" {
		t.Fatalf("unexpected groups: %+v", parsed.Groups)
	}
}

func TestParseConnectionsExcelFileRejectsMissingRequiredHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.xlsx")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create excel: %v", err)
	}
	writer, err := newXLSXExportFileWriter(file, 0)
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}
	if err := writer.SetColumns([]string{"name", "host", "port"}); err != nil {
		t.Fatalf("set columns: %v", err)
	}
	if err := writer.ConsumeRow(map[string]interface{}{"name": "db", "host": "127.0.0.1", "port": "3306"}); err != nil {
		t.Fatalf("write row: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	_ = file.Close()

	if _, err := parseConnectionsExcelFile(path); err == nil {
		t.Fatal("expected missing type header to fail")
	}
}

func TestParseConnectionsExcelFileAcceptsSQLiteWithoutHostPort(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sqlite.xlsx")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create excel: %v", err)
	}
	writer, err := newXLSXExportFileWriter(file, 0)
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}
	if err := writer.SetColumns([]string{"name", "type", "password", "environment"}); err != nil {
		t.Fatalf("set columns: %v", err)
	}
	if err := writer.ConsumeRow(map[string]interface{}{
		"name": "local-db", "type": "sqlite", "password": "secret", "environment": "local",
	}); err != nil {
		t.Fatalf("write row: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	parsed, err := parseConnectionsExcelFile(path)
	if err != nil {
		t.Fatalf("parse excel: %v", err)
	}
	if len(parsed.Inputs) != 1 {
		t.Fatalf("inputs = %d, want 1", len(parsed.Inputs))
	}
	got := parsed.Inputs[0]
	if got.Name != "local-db" || got.Config.Type != "sqlite" || got.Config.Password != "secret" || got.EnvironmentType != "local" {
		t.Fatalf("unexpected connection: %+v", got)
	}
}

func TestWriteConnectionsExcelBytesRoundTripLocalizedTemplate(t *testing.T) {
	content, err := writeConnectionsExcelBytes(defaultAppText, []connectionExcelExportRow{{
		Name:        "finance-db",
		Type:        "mysql",
		Environment: "production",
		Host:        "10.0.0.8",
		Port:        3306,
		User:        "root",
		Password:    "s3cret",
		Database:    "app",
		ReadOnly:    true,
		UseSSL:      true,
		SSLMode:     "required",
	}})
	if err != nil {
		t.Fatalf("write excel: %v", err)
	}
	path := filepath.Join(t.TempDir(), "template.xlsx")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	workbook, err := excelize.OpenFile(path)
	if err != nil {
		t.Fatalf("open excel: %v", err)
	}
	defer workbook.Close()
	header, err := workbook.GetCellValue(connectionExcelDataSheet, "A1")
	if err != nil {
		t.Fatalf("read header: %v", err)
	}
	if header == "name" || header == "" {
		t.Fatalf("expected localized name header, got %q", header)
	}
	typeHeader, err := workbook.GetCellValue(connectionExcelDataSheet, "B1")
	if err != nil {
		t.Fatalf("read type header: %v", err)
	}
	if typeHeader == "type" || typeHeader == "" {
		t.Fatalf("expected localized type header, got %q", typeHeader)
	}
	validations, err := workbook.GetDataValidations(connectionExcelDataSheet)
	if err != nil {
		t.Fatalf("read validations: %v", err)
	}
	if len(validations) == 0 {
		t.Fatal("expected dropdown validations")
	}
	foundTypeList := false
	for _, validation := range validations {
		if strings.Contains(validation.Sqref, "B2:") && strings.Contains(validation.Formula1, connectionExcelListSheet) {
			foundTypeList = true
		}
	}
	if !foundTypeList {
		t.Fatalf("expected type dropdown to reference %s, got %+v", connectionExcelListSheet, validations)
	}

	parsed, err := parseConnectionsExcelFile(path)
	if err != nil {
		t.Fatalf("parse exported excel: %v", err)
	}
	if len(parsed.Inputs) != 1 {
		t.Fatalf("inputs = %d, want 1", len(parsed.Inputs))
	}
	got := parsed.Inputs[0]
	if got.Name != "finance-db" || got.Config.Type != "mysql" || got.Config.Password != "s3cret" || got.EnvironmentType != "production" {
		t.Fatalf("unexpected round-trip connection: %+v", got)
	}
	if !got.Config.ReadOnly || !got.Config.UseSSL || got.Config.SSLMode != "required" {
		t.Fatalf("unexpected flags: %+v", got.Config)
	}
}

func TestXLSXExportWriterRequiresSetColumnsBeforeRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.xlsx")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create excel: %v", err)
	}
	writer, err := newXLSXExportFileWriter(file, 0)
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}
	if err := writer.ConsumeRow(map[string]interface{}{"name": "db"}); err == nil {
		t.Fatal("expected uninitialized sheet to fail")
	}
	if err := writer.SetColumns(connectionExcelExportColumns); err != nil {
		t.Fatalf("set columns: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close initialized sheet: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}
}
