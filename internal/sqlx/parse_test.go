package sqlx

import (
	"errors"
	"strings"
	"testing"
)

func TestParseRoutingShapes(t *testing.T) {
	cases := []struct {
		sql     string
		kind    Kind
		table   string
		keys    []string
		all     bool
		perRow  bool
		locking bool
		flags   Flags
		auto    *bool
		names   string
		db      string
		global  bool
		explain bool
	}{
		{sql: "SELECT * FROM users WHERE user_id = 42", kind: KindSelect, table: "users", keys: []string{"42"}},
		{sql: "select * from Users where User_ID=42", kind: KindSelect, table: "users", keys: []string{"42"}},
		{sql: "SELECT * FROM users WHERE user_id = '42'", kind: KindSelect, table: "users", keys: []string{"42"}},
		{sql: "SELECT * FROM users WHERE status = 1 AND user_id = 7", kind: KindSelect, table: "users", keys: []string{"7"}},
		{sql: "SELECT * FROM users WHERE user_id = 7 AND (status = 1 OR status = 2)", kind: KindSelect, table: "users", keys: []string{"7"}},
		{sql: "SELECT * FROM users WHERE user_id = 7 OR status = 1", kind: KindSelect, table: "users", all: true},
		{sql: "SELECT * FROM users WHERE user_id IN (1, 2, 3)", kind: KindSelect, table: "users", keys: []string{"1", "2", "3"}},
		{sql: "SELECT * FROM users WHERE id = 1", kind: KindSelect, table: "users", all: true},
		{sql: "SELECT * FROM app.users WHERE users.user_id = 3", kind: KindSelect, table: "users", keys: []string{"3"}},
		{sql: "SELECT * FROM users WHERE name = 'user_id = 1' AND user_id = 8", kind: KindSelect, table: "users", keys: []string{"8"}},
		{sql: "SELECT 1", kind: KindSelect, all: true},
		{sql: "SELECT COUNT(*) FROM users WHERE user_id = 4", kind: KindSelect, table: "users", keys: []string{"4"}, flags: Flags{Aggregate: true}},
		{sql: "SELECT * FROM users WHERE user_id = 4 ORDER BY id LIMIT 2", kind: KindSelect, table: "users", keys: []string{"4"}, flags: Flags{OrderBy: true, Limit: true}},
		{sql: "SELECT * FROM users WHERE user_id = 4 FOR UPDATE", kind: KindSelect, table: "users", keys: []string{"4"}, locking: true},
		{sql: "-- comment\nSELECT * FROM users WHERE user_id = 9", kind: KindSelect, table: "users", keys: []string{"9"}},
		{sql: "INSERT INTO users (id, user_id, name) VALUES (1, 9, 'a')", kind: KindInsert, table: "users", keys: []string{"9"}, perRow: true},
		{sql: "INSERT INTO users (id, user_id) VALUES (1, 9), (2, 9)", kind: KindInsert, table: "users", keys: []string{"9", "9"}, perRow: true},
		{sql: "INSERT INTO users SET id=1, user_id=11, name='z'", kind: KindInsert, table: "users", keys: []string{"11"}, perRow: true},
		{sql: "INSERT IGNORE INTO users (user_id) VALUES (12) ON DUPLICATE KEY UPDATE name='x'", kind: KindInsert, table: "users", keys: []string{"12"}, perRow: true},
		{sql: "UPDATE users SET name='b' WHERE user_id=9", kind: KindUpdate, table: "users", keys: []string{"9"}},
		{sql: "DELETE FROM users WHERE user_id=9", kind: KindDelete, table: "users", keys: []string{"9"}},
		{sql: "BEGIN", kind: KindBegin},
		{sql: "START TRANSACTION", kind: KindBegin},
		{sql: "COMMIT", kind: KindCommit},
		{sql: "ROLLBACK", kind: KindRollback},
		{sql: "SET NAMES utf8mb4", kind: KindSet, names: "utf8mb4"},
		{sql: "SET autocommit=0", kind: KindSet, auto: boolPtr(false)},
		{sql: "SET @@session.autocommit = 1", kind: KindSet, auto: boolPtr(true)},
		{sql: "USE app", kind: KindUse, db: "app"},
		{sql: "SHOW VARIABLES", kind: KindShow},
		{sql: "EXPLAIN SELECT * FROM users WHERE user_id = 1", kind: KindSelect, table: "users", keys: []string{"1"}, explain: true},
		{sql: "CREATE TABLE users (id BIGINT)", kind: KindDDL},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			a, err := Parse(tc.sql, "user_id")
			if err != nil {
				t.Fatal(err)
			}
			if a.Kind != tc.kind {
				t.Fatalf("kind %s want %s", a.Kind, tc.kind)
			}
			if tc.table != "" {
				if len(a.Tables) != 1 || a.Tables[0] != tc.table {
					t.Fatalf("tables %#v", a.Tables)
				}
			}
			if a.Locking != tc.locking {
				t.Fatalf("locking %v", a.Locking)
			}
			if a.Flags != tc.flags {
				t.Fatalf("flags %+v want %+v", a.Flags, tc.flags)
			}
			if a.Explain != tc.explain {
				t.Fatalf("explain %v", a.Explain)
			}
			if a.NamesCharset != tc.names {
				t.Fatalf("names %q", a.NamesCharset)
			}
			if a.UseDB != tc.db {
				t.Fatalf("db %q", a.UseDB)
			}
			if tc.auto != nil && (a.Autocommit == nil || *a.Autocommit != *tc.auto) {
				t.Fatalf("autocommit %#v", a.Autocommit)
			}
			if a.Kind == KindSelect || a.Kind == KindUpdate || a.Kind == KindDelete || a.Kind == KindInsert {
				m, err := a.Match("user_id", nil)
				if err != nil {
					t.Fatal(err)
				}
				if m.All != tc.all {
					t.Fatalf("all %v keys %v", m.All, m.Keys)
				}
				if !tc.all && strings.Join(m.Keys, ",") != strings.Join(tc.keys, ",") {
					t.Fatalf("keys %v want %v", m.Keys, tc.keys)
				}
				if m.PerRow != tc.perRow {
					t.Fatalf("perRow %v", m.PerRow)
				}
			}
		})
	}
}

func TestParsePlaceholderAndRejects(t *testing.T) {
	a, err := Parse("SELECT * FROM users WHERE user_id = ? AND status = ?", "user_id")
	if err != nil {
		t.Fatal(err)
	}
	m, err := a.Match("user_id", []any{int64(15), "open"})
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Keys) != 1 || m.Keys[0] != "15" {
		t.Fatalf("keys %v", m.Keys)
	}

	a, err = Parse("INSERT INTO users (id, user_id, name) VALUES (?, ?, ?)", "user_id")
	if err != nil {
		t.Fatal(err)
	}
	m, err = a.Match("user_id", []any{int64(1), "21", "ada"})
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Keys) != 1 || m.Keys[0] != "21" {
		t.Fatalf("insert keys %v", m.Keys)
	}

	if _, err := Parse("SELECT 1; SELECT 2", "user_id"); err == nil || !strings.Contains(err.Error(), "multi-statements") {
		t.Fatalf("multi: %v", err)
	}
	_, err = Parse("SELECT * FROM users u JOIN orders o ON o.user_id = u.user_id", "user_id")
	var unsupported *UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("join: %v", err)
	}
	_, err = Parse("SELECT * FROM users WHERE user_id IN (SELECT user_id FROM orders)", "user_id")
	if !errors.As(err, &unsupported) {
		t.Fatalf("subquery: %v", err)
	}
	a, err = Parse("DELETE FROM users", "user_id")
	if err != nil {
		t.Fatal(err)
	}
	m, err = a.Match("user_id", nil)
	if err != nil || !m.All {
		t.Fatalf("delete all: %+v %v", m, err)
	}
	a, err = Parse("INSERT INTO users (id, name) VALUES (1, 'a')", "user_id")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Match("user_id", nil); err == nil {
		t.Fatal("expected missing shard key")
	}
	a, err = Parse("SELECT * FROM users WHERE user_id = 1 AND user_id = 2", "user_id")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Match("user_id", nil); err == nil {
		t.Fatal("expected contradiction")
	}
}

func TestCanonicalNegativeAndPadding(t *testing.T) {
	a, err := Parse("SELECT * FROM users WHERE user_id = 0042", "user_id")
	if err != nil {
		t.Fatal(err)
	}
	m, err := a.Match("user_id", nil)
	if err != nil {
		t.Fatal(err)
	}
	if m.Keys[0] != "42" {
		t.Fatalf("key %q", m.Keys[0])
	}
	a, err = Parse("SELECT * FROM users WHERE user_id = -7", "user_id")
	if err != nil {
		t.Fatal(err)
	}
	m, err = a.Match("user_id", nil)
	if err != nil {
		t.Fatal(err)
	}
	if m.Keys[0] != "-7" {
		t.Fatalf("key %q", m.Keys[0])
	}
}

func boolPtr(v bool) *bool { return &v }
