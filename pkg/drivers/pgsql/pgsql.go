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
	// === Schema Definition ===
	// Defines the target schema state, including optimized indexes.
	// RECOMMENDATION: Consider adding the following partial index to potentially
	// improve Compaction performance by optimizing deletion of deleted rows:
	// CREATE INDEX IF NOT EXISTS kine_deleted_rows_id_idx ON kine (id) WHERE deleted != 0;
	schema = []string{
		// Main table definition
		`CREATE TABLE IF NOT EXISTS kine (
			id BIGSERIAL PRIMARY KEY,             -- Implicit kine_pkey index on (id)
			name text COLLATE "C",              -- COLLATE "C" is important for index usage with LIKE/comparison operators
			created INTEGER,
			deleted INTEGER,                  -- Used as a boolean (0 or 1)
			create_revision BIGINT,
			prev_revision BIGINT,
			lease INTEGER,
			value bytea,
			old_value bytea
		);`,

		// === Optimized Index Set ===
		// Supports AfterSQL (Watch) query WHERE/ORDER BY clauses and name lookups.
		`CREATE INDEX IF NOT EXISTS kine_name_id_index ON kine (name,id)`,
		// Supports CompactSQL subquery filter WHERE kp.prev_revision != 0.
		`CREATE INDEX IF NOT EXISTS kine_prev_revision_index ON kine (prev_revision)`,
		// Unique constraint vital for Kine's data model correctness. Also supports lookups.
		`CREATE UNIQUE INDEX IF NOT EXISTS kine_name_prev_revision_uindex ON kine (name, prev_revision)`,
		// Supports List/Count queries using DISTINCT ON, covering ORDER BY and potentially filtering.
		`CREATE INDEX IF NOT EXISTS kine_list_query_index on kine(name, id DESC, deleted)`,
	}

	// === Schema Migrations ===
	// Contains SQL statements to upgrade schemas from older Kine versions.
	// Controlled by the KINE_SCHEMA_MIGRATION environment variable: if set to N,
	// Kine assumes migrations 0 through N-1 are already applied.
	schemaMigrations = []string{
		// Migration 0: Ensure large enough integer types for revisions and IDs
		`ALTER TABLE kine ALTER COLUMN id SET DATA TYPE BIGINT, ALTER COLUMN create_revision SET DATA TYPE BIGINT, ALTER COLUMN prev_revision SET DATA TYPE BIGINT; ALTER SEQUENCE kine_id_seq AS BIGINT`,
		// Migration 1: Ensure "C" collation for name column for predictable sorting and index usage
		`ALTER TABLE kine ALTER COLUMN name SET DATA TYPE TEXT COLLATE "C" USING name::TEXT COLLATE "C"`,
		// Migration 2: Remove redundant index kine_name_index (covered by other multi-column indexes)
		`DROP INDEX IF EXISTS kine_name_index;`,
		// Migration 3: Remove redundant/suboptimal index kine_id_deleted_index (PK covers ID lookup, partial index better for compaction)
		`DROP INDEX IF EXISTS kine_id_deleted_index;`,
	}
	createDB = `CREATE DATABASE "%s";`

	// === Optimized Query Templates using Common Table Expressions (CTEs) ===
	// CTEs are used to calculate global aggregates (current revision, compact revision)
	// only ONCE per query, significantly improving performance for List, Count, and
	// After (Watch) operations compared to executing aggregates per-row.
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
				kv.name LIKE ? -- $1: prefix
				%s             -- Placeholder for additional WHERE clauses
			ORDER BY kv.name, kv.id DESC -- Required for DISTINCT ON semantic (latest ID per name)
		)
		SELECT
			gr.current_rev,
			gr.compact_rev,
			lk.*
		FROM latest_keys AS lk
		CROSS JOIN global_revs AS gr
		WHERE
			lk.deleted = 0 OR ?     -- $N: includeDeleted
		ORDER BY lk.thename ASC      -- Final ordering by name
	`
	countSQLBaseCTE = globalRevsCTE + `,
		latest_keys_for_count AS (
			SELECT DISTINCT ON (kv.name)
				kv.id AS theid, kv.deleted
			FROM kine AS kv
			WHERE
				kv.name LIKE ? -- $1: prefix
				%s             -- Placeholder for additional WHERE clauses
			ORDER BY kv.name, kv.id DESC -- Required for DISTINCT ON
		)
		SELECT
			gr.current_rev,
			COUNT(lk.theid)
		FROM latest_keys_for_count AS lk
		CROSS JOIN global_revs AS gr
		WHERE
			lk.deleted = 0 OR ?      -- $N: includeDeleted
	`
	afterSQLBaseCTE = globalRevsCTE + `
		SELECT
			gr.current_rev,
			gr.compact_rev,
			kv.id AS theid, kv.name AS thename, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value
		FROM kine AS kv
		CROSS JOIN global_revs AS gr
		WHERE
			kv.name LIKE ? AND       -- $1: prefix
			kv.id > ?                -- $2: revision
		ORDER BY kv.id ASC           -- Order by ID for watch stream
	`
)

// New creates a new PostgreSQL backend driver implementation.
func New(ctx context.Context, cfg *drivers.Config) (bool, server.Backend, error) {
	parsedDSN, err := prepareDSN(cfg.DataSourceName, cfg.BackendTLSConfig)
	if err != nil {
		return false, nil, fmt.Errorf("failed to prepare DSN: %w", err)
	}

	if err := createDBIfNotExist(parsedDSN); err != nil {
		logrus.Warnf("Failed to ensure database existence, proceeding anyway: %v", err)
	}

	dialect, err := generic.Open(ctx, "pgx", parsedDSN, cfg.ConnectionPoolConfig, "$", true, cfg.MetricsRegisterer)
	if err != nil {
		return false, nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// === PostgreSQL Specific SQL Query Assignments ===
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
	// === END SQL Query Assignments ===

	// --- Driver Specific Callbacks ---
	dialect.FillRetryDuration = time.Millisecond + 5

	// InsertRetry handles potential PK conflicts on insert, possibly due to gap fill races.
	dialect.InsertRetry = func(err error) bool {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation && pgErr.ConstraintName == "kine_pkey" {
			logrus.Warnf("Retrying insert due to potential PK conflict (gap fill race?): %v", err)
			return true
		}
		return false
	}

	// TranslateErr maps common DB errors to Kine errors.
	dialect.TranslateErr = func(err error) error {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
			if pgErr.ConstraintName == "kine_name_prev_revision_uindex" {
				// This specific constraint violation means the key/revision pair already exists.
				return server.ErrKeyExists
			}
		}
		return err // Return original error if not translated
	}

	// ErrCode extracts the PG error code, primarily for metrics.
	dialect.ErrCode = func(err error) string {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			return pgErr.Code
		}
		if err == context.Canceled || err == context.DeadlineExceeded {
			return err.Error()
		}
		if err == sql.ErrNoRows {
			return "SQL_NO_ROWS"
		}
		if err == nil {
			return ""
		}
		return "UNKNOWN" // Fallback for other error types
	}

	// --- Final Steps ---
	if err := setup(dialect.DB); err != nil {
		dialect.DB.Close()
		return false, nil, fmt.Errorf("failed to setup database schema: %w", err)
	}

	dialect.Migrate(context.Background()) // Run generic data migration logic if applicable
	return true, logstructured.New(sqllog.New(dialect)), nil
}

// setup ensures the database schema and indexes are created and up-to-date.
func setup(db *sql.DB) (err error) { // Named return error for easier defer handling
	logrus.Infof("Configuring database table schema and indexes (PostgreSQL)...")
	var version string
	collationSupported := true
	// Check for CockroachDB which doesn't support COLLATE "C"
	if errCheck := db.QueryRowContext(context.Background(), "select version()").Scan(&version); errCheck == nil {
		if strings.Contains(strings.ToLower(version), "cockroachdb") {
			logrus.Info("Detected CockroachDB - Disabling COLLATE \"C\" clauses.")
			collationSupported = false
		}
	} else if errCheck != sql.ErrNoRows {
		logrus.Warnf("Could not query database version, assuming collations supported: %v", errCheck)
	}

	// Use a transaction for schema setup to ensure atomicity
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for schema setup: %w", err)
	}
	defer func() {
		if err != nil { // Rollback only if an error occurred during setup
			if rbErr := tx.Rollback(); rbErr != nil {
				logrus.Errorf("Failed to rollback schema transaction: %v (original error: %v)", rbErr, err)
			}
		}
	}()

	// Apply base schema (table and optimized indexes)
	for i, stmt := range schema {
		finalStmt := stmt
		if !collationSupported {
			finalStmt = strings.ReplaceAll(finalStmt, ` COLLATE "C"`, "")
		}
		logrus.Tracef("SETUP TX EXEC Schema %d: %s", i, util.Stripped(finalStmt))
		if _, err = tx.Exec(finalStmt); err != nil {
			return fmt.Errorf("failed executing schema statement %d: %w\nStatement: %s", i, err, util.Stripped(finalStmt))
		}
	}

	// Determine target schema version based on environment variable
	// KINE_SCHEMA_MIGRATION=N means migrations 0..N-1 are assumed complete.
	schemaVersionStr := os.Getenv("KINE_SCHEMA_MIGRATION")
	targetSchemaVersion, parseErr := strconv.ParseUint(schemaVersionStr, 10, 64)
	applyAllMigrations := false
	if schemaVersionStr == "" {
		// If KINE_SCHEMA_MIGRATION is not set, attempt to apply all defined migrations.
		applyAllMigrations = true
		logrus.Debugf("KINE_SCHEMA_MIGRATION not set, attempting to apply all %d migrations if needed.", len(schemaMigrations))
	} else if parseErr != nil {
		logrus.Warnf("Invalid KINE_SCHEMA_MIGRATION value '%s', skipping schema migrations: %v", schemaVersionStr, parseErr)
		targetSchemaVersion = 0 // Prevent running migrations if value is invalid
	}

	// Apply schema migrations sequentially
	for i, stmt := range schemaMigrations {
		// Run migration 'i' if applying all, or if target version requires it (i < targetSchemaVersion).
		if !applyAllMigrations && uint64(i) >= targetSchemaVersion {
			logrus.Debugf("Skipping schema migration %d as target schema version is %d", i, targetSchemaVersion)
			continue
		}

		if stmt == "" { continue } // Skip empty slots

		finalStmt := stmt
		if !collationSupported {
			finalStmt = strings.ReplaceAll(finalStmt, ` COLLATE "C"`, "")
		}
		logrus.Tracef("SETUP TX EXEC Migration %d: %s", i, util.Stripped(finalStmt))
		if _, err = tx.Exec(finalStmt); err != nil {
			var pgErr *pgconn.PgError
			// Ignore "already exists" or "does not exist" (for DROP) errors during migrations
			if errors.As(err, &pgErr) &&
				(pgErr.Code == pgerrcode.DuplicateColumn ||
					pgErr.Code == pgerrcode.DuplicateObject ||
					pgErr.Code == pgerrcode.DuplicateTable ||
					pgErr.Code == pgerrcode.DuplicateIndex ||
					pgErr.Code == pgerrcode.UndefinedObject || // Ignore if trying to DROP something not there
					pgErr.Code == pgerrcode.UndefinedTable) {
				logrus.Warnf("Ignoring error during migration %d, likely safe (e.g., already applied or object missing): %v", i, err)
				err = nil // Reset error to continue
			} else {
				// Return wrapped error for actual migration failures
				return fmt.Errorf("failed executing schema migration %d: %w\nStatement: %s", i, err, util.Stripped(finalStmt))
			}
		}
	}

	// Commit transaction if everything succeeded
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit schema setup transaction: %w", err)
	}

	logrus.Infof("Database tables and indexes are up to date")
	return nil // Success
}

// createDBIfNotExist attempts to create the target database if it doesn't exist.
func createDBIfNotExist(dataSourceName string) error {
	u, err := util.ParseURL(dataSourceName)
	if err != nil {
		return fmt.Errorf("failed to parse DSN for DB check: %w", err)
	}
	// Extract database name from path
	dbName := strings.TrimPrefix(u.Path, "/")
	// Don't try to create empty or default db names
	if dbName == "" || dbName == "postgres" {
		logrus.Debugf("Target database is '%s', skipping existence check/creation.", dbName)
		return nil
	}

	// Connect to the default 'postgres' database to run check/create commands
	postgresU := *u
	postgresU.Path = "/postgres"
	postgresDSN := postgresU.String()

	db, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		logrus.Warnf("failed to connect to 'postgres' database to check existence of '%s': %v", dbName, err)
		return nil // Allow Kine startup to proceed, connection to target DB might still work
	}
	defer db.Close()

	// Check if target database exists
	var exists int
	err = db.QueryRowContext(context.Background(), "SELECT 1 FROM pg_database WHERE datname = $1", dbName).Scan(&exists)
	if err == nil && exists == 1 {
		logrus.Debugf("Database '%s' already exists.", dbName)
		return nil // Already exists, nothing to do
	}
	// Log if check query failed, but proceed to attempt creation
	if err != nil && err != sql.ErrNoRows {
		logrus.Warnf("failed to query existence of database '%s': %v. Attempting creation.", dbName, err)
	}

	// Attempt to create database if it doesn't exist or check failed
	safeDbName := strings.ReplaceAll(dbName, `"`, `""`) // Basic defense against injection in db name
	stmt := fmt.Sprintf(`CREATE DATABASE "%s"`, safeDbName)
	logrus.Infof("Attempting to create database '%s'...", dbName)
	logrus.Tracef("SETUP EXEC (DB Create): %s", stmt)
	_, err = db.ExecContext(context.Background(), stmt)
	if err != nil {
		var pgErr *pgconn.PgError
		// Ignore error if database already exists (race condition or check failed)
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.DuplicateDatabase {
			logrus.Infof("Database '%s' already exists (creation attempt confirmed).", dbName)
			return nil
		}
		// Log other creation errors as warnings, but allow Kine startup to proceed
		logrus.Warnf("failed to create database '%s': %v", dbName, err)
		return nil
	}

	logrus.Infof("Successfully created database '%s'.", dbName)
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
		// Default to postgres scheme if missing
		dataSourceName = "postgres://" + dataSourceName
	}

	u, err := util.ParseURL(dataSourceName)
	if err != nil {
		return "", fmt.Errorf("parsing DSN failed: %w", err)
	}

	// Default database name if path is empty or just "/"
	if len(u.Path) <= 1 {
		u.Path = "/kubernetes"
	}
	// Use lowercase db name for consistency (PostgreSQL folds unquoted identifiers to lowercase)
	u.Path = strings.ToLower(u.Path)

	params, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", fmt.Errorf("parsing DSN query parameters failed: %w", err)
	}

	// Inject TLS parameters from config if not already present in DSN
	sslmode := params.Get("sslmode")
	targetSSLMode := ""
	// Assume verify-full if any TLS certificate material is provided
	if tlsInfo.CertFile != "" || tlsInfo.KeyFile != "" || tlsInfo.CAFile != "" {
		targetSSLMode = "verify-full"
	}

	if tlsInfo.CertFile != "" && params.Get("sslcert") == "" {
		params.Set("sslcert", tlsInfo.CertFile)
	}
	if tlsInfo.KeyFile != "" && params.Get("sslkey") == "" {
		params.Set("sslkey", tlsInfo.KeyFile)
	}
	if tlsInfo.CAFile != "" && params.Get("sslrootcert") == "" {
		params.Set("sslrootcert", tlsInfo.CAFile)
	}

	// Set sslmode based on TLS config if not explicitly set in DSN
	if sslmode == "" && targetSSLMode != "" {
		params.Set("sslmode", targetSSLMode)
		logrus.Infof("Setting PostgreSQL SSL mode to '%s' based on provided TLS configuration.", targetSSLMode)
	} else if sslmode != "" && targetSSLMode != "" && sslmode != targetSSLMode {
		// Warn if DSN sslmode conflicts with what TLS config implies
		logrus.Warnf("DSN specifies sslmode='%s' but provided TLS configuration implies '%s'. Using DSN value '%s'.", sslmode, targetSSLMode, sslmode)
	} else if sslmode == "" {
		logrus.Debug("No TLS configuration provided and no sslmode specified in DSN. Using default SSL mode (typically 'prefer').")
	}

	u.RawQuery = params.Encode()
	return u.String(), nil
}

// init registers the "postgres" and "postgresql" driver names with Kine.
func init() {
	drivers.Register("postgres", New)
	drivers.Register("postgresql", New)
}
