package postgresql

import (
	"context"
	"database/sql"

	"github.com/crossplane-contrib/provider-sql/pkg/clients/xsql"
	"github.com/lib/pq"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
)

const (
	// https://www.postgresql.org/docs/current/errcodes-appendix.html
	// These are not available as part of the pq library.
	pqInvalidCatalog = pq.ErrorCode("3D000")

	sslModeKey = "sslmode"
)

type postgresDB struct {
	dsn      string
	endpoint string
	port     string
	sslmode  string
}

// New returns a new PostgreSQL database client. The default database name is
// an empty string. The underlying pq library will default to either using the
// value of PGDATABASE, or if unset, the hardcoded string 'postgres'.
// The sslmode defines the mode used to set up the connection for the provider.
func New(creds map[string][]byte, database string, sslmode string) xsql.DB {
	// TODO(negz): Support alternative connection secret formats?
	endpoint := string(creds[xpv1.ResourceCredentialsSecretEndpointKey])
	port := string(creds[xpv1.ResourceCredentialsSecretPortKey])

	dsn := "postgres://" +
		string(creds[xpv1.ResourceCredentialsSecretUserKey]) + ":" +
		string(creds[xpv1.ResourceCredentialsSecretPasswordKey]) + "@" +
		endpoint + ":" +
		port + "/" +
		database

	if sslmode != "" {
		dsn = dsn + "?sslmode=" + sslmode
	}

	return postgresDB{
		dsn:      dsn,
		endpoint: endpoint,
		port:     port,
		sslmode:  sslmode,
	}
}

// ExecTx executes an array of queries, committing if all are successful and
// rolling back immediately on failure.
func (c postgresDB) ExecTx(ctx context.Context, ql []xsql.Query) error {
	d, err := sql.Open("postgres", c.dsn)
	if err != nil {
		return err
	}

	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// Rollback or Commit based on error state. Defer close in defer to make
	// sure the connection is always closed.
	defer func() {
		defer d.Close() //nolint:errcheck
		if err != nil {
			tx.Rollback() //nolint:errcheck
			return
		}
		err = tx.Commit()
	}()

	for _, q := range ql {
		if _, err = tx.Exec(q.String, q.Parameters...); err != nil {
			return err
		}
	}
	return err
}

// Exec the supplied query.
func (c postgresDB) Exec(ctx context.Context, q xsql.Query) error {
	d, err := sql.Open("postgres", c.dsn)
	if err != nil {
		return err
	}
	defer d.Close() //nolint:errcheck

	_, err = d.ExecContext(ctx, q.String, q.Parameters...)
	return err
}

// Query the supplied query.
func (c postgresDB) Query(ctx context.Context, q xsql.Query) (*sql.Rows, error) {
	d, err := sql.Open("postgres", c.dsn)
	if err != nil {
		return nil, err
	}
	defer d.Close() //nolint:errcheck

	rows, err := d.QueryContext(ctx, q.String, q.Parameters...)
	return rows, err
}

// Scan the results of the supplied query into the supplied destination.
func (c postgresDB) Scan(ctx context.Context, q xsql.Query, dest ...interface{}) error {
	db, err := sql.Open("postgres", c.dsn)
	if err != nil {
		return err
	}
	defer db.Close() //nolint:errcheck

	return db.QueryRowContext(ctx, q.String, q.Parameters...).Scan(dest...)
}

// GetConnectionDetails returns the connection details for a user of this DB
func (c postgresDB) GetConnectionDetails(username, password string) managed.ConnectionDetails {
	return managed.ConnectionDetails{
		xpv1.ResourceCredentialsSecretUserKey:     []byte(username),
		xpv1.ResourceCredentialsSecretPasswordKey: []byte(password),
		xpv1.ResourceCredentialsSecretEndpointKey: []byte(c.endpoint),
		xpv1.ResourceCredentialsSecretPortKey:     []byte(c.port),
		sslModeKey:                                []byte(c.sslmode),
	}
}

// IsInvalidCatalog returns true if passed a pq error indicating
// that the database does not exist.
func IsInvalidCatalog(err error) bool {
	if pqe, ok := err.(*pq.Error); ok {
		return pqe.Code == pqInvalidCatalog
	}
	return false
}
