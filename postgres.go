package datapasta

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

// NewPostgres returns a pgdb that can generate a Database for datapasta Upload and Download functions.
func NewPostgres(ctx context.Context, c Postgreser) (pgdb, error) {
	client := postgresQueries{db: c}
	sqlcPKs, err := client.GetPrimaryKeys(ctx)
	if err != nil {
		return pgdb{}, err
	}

	sqlcFKs, err := client.GetForeignKeys(ctx)
	if err != nil {
		return pgdb{}, err
	}

	pkGroups := make(map[string]getPrimaryKeysRow, len(sqlcPKs))
	for _, pk := range sqlcPKs {
		pkGroups[pk.TableName] = pk
	}

	builder := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
	fks := make([]ForeignKey, 0, len(sqlcFKs))
	for _, fk := range sqlcFKs {
		fks = append(fks, ForeignKey(fk))
	}

	return pgdb{
		fks:      fks,
		pkGroups: pkGroups,
		builder:  builder,
	}, nil
}

type pgdb struct {
	// figured out from schema
	pkGroups map[string]getPrimaryKeysRow
	fks      []ForeignKey

	// squirrel instance to help with stuff
	builder squirrel.StatementBuilderType
}

// NewClient creates a pgtx that can be used as a Database for Upload and Download.
// it is recommended you pass an open transaction, so you can control committing or rolling it back.
func (db pgdb) NewClient(ctx context.Context, tx Postgreser) (pgtx, error) {
	return pgtx{
		pgdb:           db,
		tx:             postgresQueries{tx},
		ctx:            ctx,
		found:          map[string][]any{},
		foundWithoutPK: map[any]bool{},
	}, nil
}

func (db pgdb) ForeignKeys() []ForeignKey {
	return db.fks
}

type pgtx struct {
	pgdb
	ctx context.Context

	// as a destination, we need a tx
	tx postgresQueries

	// as a source, we must not return already-found objects
	found          map[string][]any
	foundWithoutPK map[any]bool
}

func (db pgtx) Insert(rows ...map[string]any) error {
	for _, row := range rows {
		table := row[DumpTableKey].(string)

		pk := ""
		if pkg, found := db.pkGroups[table]; found {
			pk = pkg.ColumnName
		}

		builder := db.builder.Insert(`"` + table + `"`)
		if pk != "" {
			builder = builder.Suffix("RETURNING " + pk)
			delete(row, pk)
		}

		keys := make([]string, 0, len(row))
		vals := make([]any, 0, len(row))
		for k, v := range row {
			if k == DumpTableKey {
				continue
			}
			vals = append(vals, v)
			keys = append(keys, fmt.Sprintf(`"%s"`, k))
		}

		sql, args, err := builder.Columns(keys...).Values(vals...).ToSql()
		if err != nil {
			return fmt.Errorf(`build: %w, args: %s, sql: %s`, err, args, sql)
		}

		if pk != "" {
			newPK := any(nil)
			if err := db.tx.db.QueryRow(db.ctx, sql, args...).Scan(&newPK); err != nil {
				for i, k := range keys {
					log.Printf(`cloning error dump: %s, %T,%#v`, k, vals[i], vals[i])
				}
				return fmt.Errorf(`query: %w, args: %s, sql: %s`, err, args, sql)
			}
			row[pk] = newPK
			continue
		}

		if _, err := db.tx.db.Exec(db.ctx, sql, args...); err != nil {
			for i, k := range keys {
				log.Printf(`cloning error dump: %s, %T,%#v`, k, vals[i], vals[i])
			}
			return fmt.Errorf(`query: %w, args: %s, sql: %s`, err, args, sql)
		}
	}
	return nil
}

func (db pgtx) SelectMatchingRows(tname string, conds map[string][]any) ([]map[string]any, error) {
	// build a query to select * where each of the conditions is met
	or := squirrel.Or{}
	for col, vals := range conds {
		or = append(or, squirrel.Eq{col: vals})
	}
	eq := squirrel.Sqlizer(or)
	if pk, ok := db.pkGroups[tname]; ok {
		eq = squirrel.And{eq, squirrel.NotEq{pk.ColumnName: db.found[tname]}}
	}
	sql, args, err := db.builder.Select("*").From(`"` + tname + `"`).Where(eq).ToSql()
	if err != nil {
		return nil, err
	}

	rows, err := db.tx.db.Query(db.ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	foundInThisScan := make(DatabaseDump, 0)
	desc := rows.FieldDescriptions()
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}

		res := map[string]any{DumpTableKey: tname}
		for n, field := range desc {
			res[string(field.Name)] = vals[n]
			if b, ok := vals[n].([16]byte); ok {
				res[string(field.Name)] = pgtype.UUID{Bytes: b, Status: pgtype.Present}
			}

			if pg, ok := vals[n].(interface {
				EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error)
			}); ok {
				out, err := pg.EncodeText(nil, nil)
				if err != nil {
					return nil, err
				}
				res[string(field.Name)] = string(out)
			}
		}

		if pk, ok := db.pkGroups[tname]; ok {
			db.found[tname] = append(db.found[tname], res[pk.ColumnName])
		} else {
			k, _ := json.Marshal(res)
			resStr := string(k)
			if _, found := db.foundWithoutPK[resStr]; found {
				continue
			}
			db.foundWithoutPK[resStr] = true
		}
		foundInThisScan = append(foundInThisScan, res)
	}
	return foundInThisScan, nil
}

// Postgreser does postgres things.
// github.com/jackc/pgx/v4/pgxpool.Pool is one such implementation of postgres.
type Postgreser interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
}

type postgresQueries struct {
	db Postgreser
}

const getForeignKeys = `-- name: GetForeignKeys :many
SELECT 
	(select  r.relname from pg_catalog.pg_class r where r.oid = c.confrelid)::text as base_table,
    a.attname::text as base_col,
   	(select r.relname from pg_catalog.pg_class r where r.oid = c.conrelid)::text as referencing_table,
   	UNNEST((select array_agg(attname) from pg_catalog.pg_attribute where attrelid = c.conrelid and array[attnum] <@ c.conkey))::text as referencing_col
FROM pg_catalog.pg_constraint c join pg_catalog.pg_attribute a on c.confrelid=a.attrelid and a.attnum = ANY(confkey)
`

type getForeignKeysRow struct {
	BaseTable        string `json:"base_table"`
	BaseCol          string `json:"base_col"`
	ReferencingTable string `json:"referencing_table"`
	ReferencingCol   string `json:"referencing_col"`
}

func (q *postgresQueries) GetForeignKeys(ctx context.Context) ([]getForeignKeysRow, error) {
	rows, err := q.db.Query(ctx, getForeignKeys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []getForeignKeysRow
	for rows.Next() {
		var i getForeignKeysRow
		if err := rows.Scan(
			&i.BaseTable,
			&i.BaseCol,
			&i.ReferencingTable,
			&i.ReferencingCol,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getPrimaryKeys = `-- name: GetPrimaryKeys :many
select
    t.relname::text  as table_name,
    (ARRAY_AGG(a.attname::text))[1]::text AS column_name
from
          pg_catalog.pg_class c
     join pg_catalog.pg_namespace n on n.oid        = c.relnamespace
     join pg_catalog.pg_index i     on i.indexrelid = c.oid AND i.indisprimary
     join pg_catalog.pg_class t     on i.indrelid   = t.oid
     JOIN   pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
where
        c.relkind = 'i'
    and n.nspname not in ('pg_catalog', 'pg_toast')
    and pg_catalog.pg_table_is_visible(c.oid)
GROUP BY t.relname
HAVING COUNT(*) = 1
`

type getPrimaryKeysRow struct {
	TableName  string `json:"table_name"`
	ColumnName string `json:"column_name"`
}

func (q *postgresQueries) GetPrimaryKeys(ctx context.Context) ([]getPrimaryKeysRow, error) {
	rows, err := q.db.Query(ctx, getPrimaryKeys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []getPrimaryKeysRow
	for rows.Next() {
		var i getPrimaryKeysRow
		if err := rows.Scan(&i.TableName, &i.ColumnName); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
