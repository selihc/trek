package postgresql

import (
	"context"
	"database/sql"
	"errors"

	"selihc.com/trek"
)

type sqlWrapper struct {
	db  trek.StdlibDB
	log trek.Logf
}

func newSQLWrapper(log trek.Logf, sqlIshDB trek.StdlibDB) *sqlWrapper {
	return &sqlWrapper{
		db:  sqlIshDB,
		log: log,
	}
}

func (w *sqlWrapper) Query(ctx context.Context, scanner trek.ScanFn, query string, args ...interface{}) error {
	w.log("executing sql statement: %q with args %+v", query, args)
	rows, err := w.db.QueryContext(ctx, query, args...)
	if err != nil {
		w.log("got error %q while executing %q with args %+v", err.Error(), query, args)
		return err
	}
	defer rows.Close()

	for rows.Next() {
		err = scanner(rows)
		if err != nil {
			return err
		}
	}

	err = rows.Err()
	if err != nil {
		return err
	}

	return nil
}

func (w *sqlWrapper) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	w.log("executing sql statement: %q with args %+v", query, args)
	return w.db.QueryRowContext(ctx, query, args...)
}

func (w *sqlWrapper) Exec(ctx context.Context, query string, args ...interface{}) error {
	_, err := w.db.ExecContext(ctx, query, args...)
	if err != nil {
		w.log("error in sql exec: %s", err)
		return err
	}

	return err
}

func (w *sqlWrapper) Transact(ctx context.Context, txFn trek.TxFn) error {
	return errors.New("cannot execute nested transaction")
}
