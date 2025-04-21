package pgsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // sql driver
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/k3s-io/kine/pkg/util"
	"github.com/sirupsen/logrus"
)

const (
	defaultDSN = "postgres://postgres:postgres@localhost/"
)

var (
	schema = []string{
		`CREATE TABLE IF NOT EXISTS kine (
			id BIGSERIAL PRIMARY KEY,
			name text COLLATE "C",              -- COLLATE "C" is important for index usage with LIKE/comparison operators
			created INTEGER,
			deleted INTEGER,
			create_revision BIGINT,
			prev_revision BIGINT,
			lease INTEGER,
			value bytea,
			old_value bytea
		);`,
		// Supports AfterSQL query WHERE/ORDER BY and name lookups.
		`CREATE INDEX IF NOT EXISTS kine_name_id_index ON kine (name,id)`,
		// Supports CompactSQL subquery filter.
		`CREATE INDEX IF NOT EXISTS kine_prev_revision_index ON kine (prev_revision)`,
		// Unique constraint vital for correctness; also supports lookups.
		`CREATE UNIQUE INDEX IF NOT EXISTS kine_name_prev_revision_uindex ON kine (name, prev_revision)`,
		// Supports List/Count queries using DISTINCT ON ORDER BY clauses.
		`CREATE INDEX IF NOT EXISTS kine_list_query_index on kine(name, id DESC, deleted)`,
		// Supports CompactSQL deletion of rows marked as deleted.
		`CREATE INDEX IF NOT EXISTS kine_deleted_rows_id_idx ON kine (id) WHERE deleted != 0`,
	}

	schemaMigrations = []string{
		// Migration 0: Ensure large enough integer types
		`ALTER TABLE kine ALTER COLUMN id SET DATA TYPE BIGINT, ALTER COLUMN create_revision SET DATA TYPE BIGINT, ALTER COLUMN prev_revision SET DATA TYPE BIGINT; ALTER SEQUENCE kine_id_seq AS BIGINT`,
		// Migration 1: Ensure "C" collation for name column
		`ALTER TABLE kine ALTER COLUMN name SET DATA TYPE TEXT COLLATE "C" USING name::TEXT COLLATE "C"`,
		// Migration 2: Remove redundant kine_name_index
		`DROP INDEX IF EXISTS kine_name_index;`,
		// Migration 3: Remove redundant kine_id_deleted_index
		`DROP INDEX IF EXISTS kine_id_deleted_index;`,
	}
	createDB = `CREATE DATABASE "%s";`

	// Optimized Query Templates using CTEs to improve performance.
	globalRevsCTE = `
		WITH global_revs AS (
			SELECT
				MAX(k.id) AS current_rev,
				MAX(CASE WHEN k.name = 'compact_rev_key' THEN k.prev_revision ELSE 0 END) AS compact_rev
			FROM kine k
		)
	`
	listSQLBaseCTE = globalRevsCTE + `,
		latest_keys AS (
			SELECT DISTINCT ON (kv.name)
				kv.id AS theid, kv.name AS thename, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value
			FROM kine AS kv
			WHERE
				kv.name LIKE ?
				%s
			ORDER BY kv.name, kv.id DESC
		)
		SELECT
			gr.current_rev,
			gr.compact_rev,
			lk.*
		FROM latest_keys AS lk
		CROSS JOIN global_revs AS gr
		WHERE
			lk.deleted = 0 OR ?
		ORDER BY lk.thename ASC
	`
	countSQLBaseCTE = globalRevsCTE + `,
		latest_keys_for_count AS (
			SELECT DISTINCT ON (kv.name)
				kv.id AS theid, kv.deleted
			FROM kine AS kv
			WHERE
				kv.name LIKE ?
				%s
			ORDER BY kv.name, kv.id DESC
		)
		SELECT
			gr.current_rev,
			COUNT(lk.theid)
		FROM latest_keys_for_count AS lk
		CROSS JOIN global_revs AS gr
		WHERE
			lk.deleted = 0 OR ?
	`
	afterSQLBaseCTE = globalRevsCTE + `
		SELECT
			gr.current_rev,
			gr.compact_rev,
			kv.id AS theid, kv.name AS thename, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value
		FROM kine AS kv
		CROSS JOIN global_revs AS gr
		WHERE
			kv.name LIKE ? AND
			kv.id > ?
		ORDER BY kv.id ASC
	`
)

// New creates a new PostgreSQL backend driver implementation.
func New(ctx context.Context, cfg *drivers.Config) (bool, server.Backend, error) {
	parsedDSN, err := prepareDSN(cfg.DataSourceName, cfg.BackendTLSConfig)
	if err != nil {
		return false, nil, err
	}

	if err := createDBIfNotExist(parsedDSN); err != nil {
		logrus.Warnf("Failed to ensure database existence, proceeding anyway: %v", err)
	}

	dialect, err := generic.Open(ctx, "pgx", parsedDSN, cfg.ConnectionPoolConfig, "$", true, cfg.MetricsRegisterer)
	if err != nil {
		return false, nil, err
	}

	// Assign optimized PostgreSQL-specific SQL queries
	dialect.GetCurrentSQL = q(fmt.Sprintf(listSQLBaseCTE, "AND kv.name > ?"))
	dialect.ListRevisionStartSQL = q(fmt.Sprintf(listSQLBaseCTE, "AND kv.id <= ?"))
	dialect.GetRevisionAfterSQL = q(fmt.Sprintf(listSQLBaseCTE, "AND kv.name > ? AND kv.id <= ?"))
	dialect.CountCurrentSQL = q(fmt.Sprintf(countSQLBaseCTE, "AND kv.name > ?"))
	dialect.CountRevisionSQL = q(fmt.Sprintf(countSQLBaseCTE, "AND kv.name > ? AND kv.id <= ?"))
	dialect.AfterSQL = q(afterSQLBaseCTE)
	dialect.GetSizeSQL = `SELECT pg_total_relation_size('kine')`
	dialect.CompactSQL = `
		DELETE FROM kine AS kv
		USING	(
			SELECT kp.prev_revision AS id FROM kine AS kp
			WHERE kp.name != 'compact_rev_key' AND kp.prev_revision != 0 AND kp.id <= $1
			UNION
			SELECT kd.id AS id FROM kine AS kd WHERE kd.deleted != 0 AND kd.id <= $2
		) AS ks
		WHERE kv.id = ks.id`

	// Configure driver-specific callbacks
	dialect.FillRetryDuration = time.Millisecond + 5
	dialect.InsertRetry = func(err error) bool {
		// Handle potential PK conflicts on insert, possibly due to gap fill races.
		if err, ok := err.(*pgconn.PgError); ok && err.Code == pgerrcode.UniqueViolation && err.ConstraintName == "kine_pkey" {
			return true
		}
		return false
	}
	dialect.TranslateErr = func(err error) error {
		if err, ok := err.(*pgconn.PgError); ok && err.Code == pgerrcode.UniqueViolation {
			return server.ErrKeyExists
		}
		return err
	}
	dialect.ErrCode = func(err error) string {
		if err == nil {
			return ""
		}
		if err, ok := err.(*pgconn.PgError); ok {
			return err.Code
		}
		return err.Error()
	}

	// Finalize setup and return backend
	if err := setup(dialect.DB); err != nil {
		dialect.DB.Close()
		return false, nil, err
	}

	dialect.Migrate(context.Background())
	return true, logstructured.New(sqllog.New(dialect)), nil
}

// setup ensures the database schema and indexes are created and up-to-date.
func setup(db *sql.DB) error {
	logrus.Infof("Configuring database table schema and indexes, this may take a moment...")
	var version string
	collationSupported := true
	if err := db.QueryRow("select version()").Scan(&version); err == nil && strings.Contains(strings.ToLower(version), "cockroachdb") {
		collationSupported = false
	}

	for _, stmt := range schema {
		logrus.Tracef("SETUP EXEC : %v", util.Stripped(stmt))
		if !collationSupported {
			stmt = strings.ReplaceAll(stmt, ` COLLATE "C"`, "")
		}
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	// Run enabled schema migrations based on KINE_SCHEMA_MIGRATION environment variable.
	schemaVersion, _ := strconv.ParseUint(os.Getenv("KINE_SCHEMA_MIGRATION"), 10, 64)
	for i, stmt := range schemaMigrations {
		// KINE_SCHEMA_MIGRATION=N means migrations 0..N-1 are done, run migration i if i < N
		if uint64(i) >= schemaVersion {
			continue // Skip migrations at or beyond the specified version
		}
		if !collationSupported {
			stmt = strings.ReplaceAll(stmt, ` COLLATE "C"`, "")
		}
		if stmt == "" {
			continue
		}
		logrus.Tracef("SETUP EXEC MIGRATION %d: %v", i, util.Stripped(stmt))
		if _, err := db.Exec(stmt); err != nil {
			var pgErr *pgconn.PgError
			// Ignore errors indicating the migration was likely already applied
			if errors.As(err, &pgErr) &&
				(pgErr.Code == pgerrcode.DuplicateColumn ||
					pgErr.Code == pgerrcode.DuplicateObject ||
					pgErr.Code == pgerrcode.DuplicateTable ||
					pgErr.Code == pgerrcode.DuplicateIndex ||
					pgErr.Code == pgerrcode.UndefinedObject || // Ignore if trying to DROP something not there
					pgErr.Code == pgerrcode.UndefinedTable) {
				logrus.Warnf("Ignoring error during migration %d, likely safe: %v", i, err)
				// Continue to next migration
			} else {
				return err // Return on actual migration error
			}
		}
	}

	logrus.Infof("Database tables and indexes are up to date")
	return nil
}

// createDBIfNotExist attempts to create the target database if it doesn't exist.
func createDBIfNotExist(dataSourceName string) error {
	u, err := util.ParseURL(dataSourceName)
	if err != nil {
		return err
	}

	dbNameParts := strings.SplitN(u.Path, "/", 2)
	if len(dbNameParts) < 2 || dbNameParts[1] == "" {
		return nil // No db name specified
	}
	dbName := dbNameParts[1]

	if dbName == "postgres" { // Don't try to create default db
		return nil
	}

	// Connect to 'postgres' db to check/create
	postgresU := *u
	postgresU.Path = "/postgres"
	db, err := sql.Open("pgx", postgresU.String())
	if err != nil {
		logrus.Warnf("failed to ensure existence of database %s: unable to connect to default postgres database: %v", dbName, err)
		return nil
	}
	defer db.Close()

	var exists bool
	err = db.QueryRow("SELECT 1 FROM pg_database WHERE datname = $1", dbName).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		logrus.Warnf("failed to check existence of database %s, going to attempt create: %v", dbName, err)
	}

	if !exists {
		safeDbName := strings.ReplaceAll(dbName, `"`, `""`)
		stmt := fmt.Sprintf(createDB, safeDbName)
		logrus.Tracef("SETUP EXEC : %v", util.Stripped(stmt))
		if _, err = db.Exec(stmt); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.DuplicateDatabase {
				// Race condition or check failed, DB already exists.
			} else {
				logrus.Warnf("failed to create database %s: %v", dbName, err)
			}
		} else {
			logrus.Tracef("created database: %s", dbName)
		}
	}
	return nil
}

// q replaces SQL '?' placeholders with PostgreSQL '$n' placeholders sequentially.
func q(sql string) string {
	regex := regexp.MustCompile(`\?`)
	n := 0
	return regex.ReplaceAllStringFunc(sql, func(string) string {
		n++
		return "$" + strconv.Itoa(n)
	})
}

// prepareDSN modifies the DSN for pgx compatibility and injects TLS parameters.
func prepareDSN(dataSourceName string, tlsInfo tls.Config) (string, error) {
	if len(dataSourceName) == 0 {
		dataSourceName = defaultDSN
	} else if !strings.Contains(dataSourceName, "://") {
		dataSourceName = "postgres://" + dataSourceName
	}
	u, err := util.ParseURL(dataSourceName)
	if err != nil {
		return "", err
	}
	if len(u.Path) == 0 || u.Path == "/" {
		u.Path = "/kubernetes" // Default database name
	}
	u.Path = strings.ToLower(u.Path) // Use lowercase db name

	queryMap, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", err
	}
	params := url.Values{}
	sslmode := ""
	if _, ok := queryMap["sslcert"]; tlsInfo.CertFile != "" && !ok {
		params.Add("sslcert", tlsInfo.CertFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslkey"]; tlsInfo.KeyFile != "" && !ok {
		params.Add("sslkey", tlsInfo.KeyFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslrootcert"]; tlsInfo.CAFile != "" && !ok {
		params.Add("sslrootcert", tlsInfo.CAFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslmode"]; !ok && sslmode != "" {
		params.Add("sslmode", sslmode)
	}
	for k, v := range queryMap {
		params.Add(k, v[0])
	}
	u.RawQuery = params.Encode()
	return u.String(), nil
}

// init registers the "postgres" and "postgresql" driver names with Kine.
func init() {
	drivers.Register("postgres", New)
	drivers.Register("postgresql", New)
}
