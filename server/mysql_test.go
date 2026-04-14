package server

import (
	"crypto/tls"
	"database/sql"
	"os"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// getEnvOrDefault returns the value of the environment variable named by key,
// or defaultVal if the variable is not set or empty.
func getEnvOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// openTestDB opens a MySQL connection for integration tests.
//
// Configuration is read from environment variables (defaults shown):
//
//	MYSQL_USER              (default: im)
//	MYSQL_PASSWD            (default: "")
//	MYSQL_ADDR              (default: 127.0.0.1:3306)
//	MYSQL_DBNAME            (default: gobelieve)
//	MYSQL_TLS_SKIP_VERIFY   set to "1" to skip server certificate verification
//
// Example – override at runtime without changing source:
//
//	MYSQL_ADDR=127.0.0.1:3306 MYSQL_PASSWD=secret go test ./server/ -run TestOpenTestDB -v
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	var tlsCfg *tls.Config
	if os.Getenv("MYSQL_TLS_SKIP_VERIFY") == "1" {
		tlsCfg = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // test-only
	}

	cfg := mysql.Config{
		User:                 getEnvOrDefault("MYSQL_USER", "im"),
		Passwd:               getEnvOrDefault("MYSQL_PASSWD", ""),
		Net:                  "tcp",
		Addr:                 getEnvOrDefault("MYSQL_ADDR", "127.0.0.1:3306"),
		DBName:               getEnvOrDefault("MYSQL_DBNAME", "gobelieve"),
		AllowNativePasswords: true,
		TLS:                  tlsCfg,
	}
	dsn := cfg.FormatDSN()
	t.Log("dsn:", dsn)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("db.Ping: %v", err)
	}
	return db
}

// TestOpenTestDB verifies that openTestDB can successfully connect to the database.
func TestOpenTestDB(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("db.Ping after open: %v", err)
	}
	t.Log("Successfully connected to the test database")
}
