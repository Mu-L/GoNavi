package db

import (
	"reflect"
	"testing"

	"GoNavi-Wails/internal/connection"
)

func TestResolvePostgresConnectDatabases_ExplicitDatabase(t *testing.T) {
	cfg := connection.ConnectionConfig{
		Type:     "postgres",
		Database: "analytics",
		User:     "app_user",
	}

	got := resolvePostgresConnectDatabases(cfg)
	want := []string{"analytics"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected databases, got=%v want=%v", got, want)
	}
}

func TestResolvePostgresConnectDatabases_FallbackOrder(t *testing.T) {
	cfg := connection.ConnectionConfig{
		Type: "postgres",
		User: "app_user",
	}

	got := resolvePostgresConnectDatabases(cfg)
	want := []string{"postgres", "template1", "app_user"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected databases, got=%v want=%v", got, want)
	}
}

func TestResolvePostgresConnectDatabases_DeduplicateUserDefault(t *testing.T) {
	cfg := connection.ConnectionConfig{
		Type: "postgres",
		User: "postgres",
	}

	got := resolvePostgresConnectDatabases(cfg)
	want := []string{"postgres", "template1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected databases, got=%v want=%v", got, want)
	}
}
