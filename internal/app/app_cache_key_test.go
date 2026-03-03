package app

import (
	"testing"

	"GoNavi-Wails/internal/connection"
)

func TestGetCacheKey_IgnoreTimeout(t *testing.T) {
	base := connection.ConnectionConfig{
		Type:     "duckdb",
		Host:     `C:\data\songs.duckdb`,
		Timeout:  30,
		UseProxy: false,
		UseSSH:   false,
	}
	modified := base
	modified.Timeout = 120

	left := getCacheKey(base)
	right := getCacheKey(modified)
	if left != right {
		t.Fatalf("expected same cache key when only timeout differs, got %s vs %s", left, right)
	}
}

func TestGetCacheKey_DuckDBHostAndDatabaseEquivalent(t *testing.T) {
	withHost := connection.ConnectionConfig{
		Type: "duckdb",
		Host: `D:\music\songs.duckdb`,
	}
	withDatabase := connection.ConnectionConfig{
		Type:     "duckdb",
		Database: `D:\music\songs.duckdb`,
	}

	left := getCacheKey(withHost)
	right := getCacheKey(withDatabase)
	if left != right {
		t.Fatalf("expected same cache key for duckdb host/database path, got %s vs %s", left, right)
	}
}

func TestGetCacheKey_KeepDatabaseIsolation(t *testing.T) {
	a := connection.ConnectionConfig{
		Type:     "mysql",
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "root",
		Database: "db_a",
		Timeout:  30,
	}
	b := a
	b.Database = "db_b"
	b.Timeout = 5

	left := getCacheKey(a)
	right := getCacheKey(b)
	if left == right {
		t.Fatalf("expected different cache key for different database targets")
	}
}
