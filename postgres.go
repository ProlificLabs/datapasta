package datapasta

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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

	fks := make([]ForeignKey, 0, len(sqlcFKs))
	for _, fk := range sqlcFKs {
		fks = append(fks, ForeignKey(fk))
	}

	builder := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
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

func (db pgdb) ForeignKeys() []ForeignKey {
	return db.fks
}

func (db pgdb) PrimaryKeys() map[string]string {
	out := make(map[string]string)
	for _, r := range db.pkGroups {
		out[r.TableName] = r.ColumnName
	}
	return out
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

// NewBatchClient creates a batching client that can be used as a Database for Upload and Download.
// it is recommended you pass an open transaction, so you can control committing or rolling it back.
// This client is optimized for Postgres to use a temporary table "datapasta_clone" which allows
// the entire upload to be done without any round trips. This table is dropped on commit or rollback.
func (db pgdb) NewBatchClient(ctx context.Context, tx Postgreser) (pgbatchtx, error) {
	child := pgtx{
		pgdb:           db,
		tx:             postgresQueries{tx},
		ctx:            ctx,
		found:          map[string][]any{},
		foundWithoutPK: map[any]bool{},
	}
	return pgbatchtx{pgtx: child}, nil
}

type pgbatchtx struct {
	pgtx
}

func (db pgbatchtx) SelectMatchingRows(tname string, conds map[string][]any) ([]map[string]any, error) {
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

func (db pgbatchtx) InsertRecord(row map[string]any) (any, error) {
	keys := make([]string, 0, len(row))
	vals := make([]any, 0, len(row))
	table := row[DumpTableKey].(string)
	builder := db.builder.Insert(`"` + table + `"`).Suffix("RETURNING id")
	for k, v := range row {
		if v == nil {
			continue
		}
		if k == DumpTableKey {
			continue
		}
		keys = append(keys, fmt.Sprintf(`"%s"`, k))
		vals = append(vals, v)
	}

	builder = builder.Columns(keys...).Values(vals...)
	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, err
	}
	var id any
	if err := db.tx.db.QueryRow(db.ctx, sql, args...).Scan(&id); err != nil {
		return nil, err
	}
	return id, nil
}

func (db pgbatchtx) Update(id RecordID, cols map[string]any) error {
	table := id.Table
	builder := db.builder.Update(`"` + table + `"`)
	builder = builder.SetMap(cols).Where(squirrel.Eq{"id": id.PrimaryKey})
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}
	cmd, err := db.tx.db.Exec(db.ctx, sql, args...)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() != 1 {
		return fmt.Errorf("update affected %d rows, expected 1", cmd.RowsAffected())
	}
	return nil
}

func (db pgbatchtx) Delete(id RecordID) error {
	table := id.Table
	builder := db.builder.Delete(`"` + table + `"`).Where(squirrel.Eq{"id": id.PrimaryKey})
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}
	cmd, err := db.tx.db.Exec(db.ctx, sql, args...)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() > 1 {
		return fmt.Errorf("delete affected %d rows, expected 0 or 1", cmd.RowsAffected())
	}
	return nil
}

func (db pgbatchtx) Mapping() ([]Mapping, error) {
	rows, err := db.tx.GetMapping(db.ctx)
	if err != nil {
		return nil, err
	}
	mapps := make([]Mapping, 0, len(rows))
	for _, r := range rows {
		mapps = append(mapps, Mapping{RecordID: RecordID{Table: r.TableName, PrimaryKey: r.CloneID}, OriginalID: r.OriginalID})
	}
	return mapps, nil
}

func (db pgbatchtx) Insert(rows ...map[string]any) error {
	if _, err := db.tx.db.Exec(db.ctx, "CREATE TEMPORARY TABLE IF NOT EXISTS datapasta_clone(table_name text, original_id integer, clone_id integer) ON COMMIT DROP"); err != nil {
		return err
	}

	if _, err := db.tx.db.Exec(db.ctx, "CREATE INDEX ON datapasta_clone(table_name,original_id, clone_id)"); err != nil {
		return err
	}

	start := time.Now()

	batch := &pgx.Batch{}
	followup := &pgx.Batch{}
	for _, row := range rows {
		table := row[DumpTableKey].(string)

		pk := ""
		if pkg, found := db.pkGroups[table]; found {
			pk = pkg.ColumnName
		}

		builder := db.builder.Insert(`"` + table + `"`)
		oldPK := row[pk]
		if pk != "" {
			builder = builder.Suffix("RETURNING " + pk + " as id")
			builder = builder.Prefix("WITH inserted_row AS (")
			builder = builder.Suffix(") INSERT INTO datapasta_clone (table_name, original_id, clone_id) SELECT ?, ?, id FROM inserted_row", table, oldPK)
			//delete(row, pk)
		}

		keys := make([]string, 0, len(row))
		vals := make([]any, 0, len(row))
		for k, v := range row {
			if v == nil {
				continue
			}
			if k == DumpTableKey {
				continue
			}
			deferred := false
			foundForeign := false
			for _, fk := range db.fks {

				if fk.ReferencingCol == k && fk.ReferencingTable == table {
					foundForeign = true
					findInMap := squirrel.Expr("COALESCE((SELECT clone_id FROM datapasta_clone WHERE original_id = ? AND table_name = ?::text), ?)", v, fk.BaseTable, v)

					if fk.BaseTable == table {
						// self-referential columns become NULL and are updated in a second pass by PK
						if pk == "" {
							return fmt.Errorf("can't have self-referencing tables without primary key")
						}
						deferred = true
						builder := db.builder.Update(`"`+table+`"`).Set(k, findInMap).Where(pk+"=(SELECT clone_id FROM datapasta_clone WHERE original_id = ? AND table_name = ?::text)", oldPK, fk.BaseTable)
						sql, args, err := builder.ToSql()
						if err != nil {
							return fmt.Errorf(`build: %w, args: %s, sql: %s`, err, args, sql)
						}
						followup.Queue(sql, args...)
					} else {
						v = findInMap
					}

					break
				}
			}
			if deferred {
				continue
			}
			if foundForeign || k != pk {
				keys = append(keys, fmt.Sprintf(`"%s"`, k))
				vals = append(vals, v)
			}
		}

		builder = builder.Columns(keys...).Values(vals...)
		sql, args, err := builder.ToSql()
		if err != nil {
			return fmt.Errorf(`build: %w, args: %s, sql: %s`, err, args, sql)
		}

		batch.Queue(sql, args...)
	}

	prepped := time.Now()
	LogFunc("batchrows:%d, followups:%d", batch.Len(), followup.Len())

	res := db.tx.db.SendBatch(db.ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		_, err := res.Exec()
		if err != nil {
			return fmt.Errorf(`batch query %d error: %w`, i, err)
		}
	}
	if err := res.Close(); err != nil {
		return fmt.Errorf("failed to execute batch upload: %w", err)
	}

	fks := db.tx.db.SendBatch(db.ctx, followup)
	for i := 0; i < followup.Len(); i++ {
		_, err := fks.Exec()
		if err != nil {
			return fmt.Errorf(`batch foreign key %d error: %w`, i, err)
		}
	}
	fks.Close()

	LogFunc("prepping: %s, batching: %s", prepped.Sub(start), time.Since(prepped))

	if err := res.Close(); err != nil {
		return fmt.Errorf("failed to execute batch followup queries: %w", err)
	}
	return nil
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

const getMapping = `
	SELECT table_name, original_id, clone_id FROM datapasta_clone
`

type getMappingRow struct {
	TableName           string
	OriginalID, CloneID int32
}

func (q *postgresQueries) GetMapping(ctx context.Context) ([]getMappingRow, error) {
	rows, err := q.db.Query(ctx, getMapping)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []getMappingRow
	for rows.Next() {
		var i getMappingRow
		if err := rows.Scan(
			&i.TableName,
			&i.OriginalID,
			&i.CloneID,
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
