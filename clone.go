package datapasta

import (
	"context"
	"fmt"
	"strings"
)

type (
	// DatabaseDump is the output of a Download call, containing every record that was downloaded.
	// It is safe to transport as JSON.
	DatabaseDump []map[string]any

	// Opt is a functional option that can be passed to Download.
	Opt func(*downloadOpts)
)

// DontRecurse includes records from `table`, but does not recurse into references to it.
func DontRecurse(table string) Opt {
	return func(m *downloadOpts) {
		m.dontRecurse[table] = true
	}
}

// DontInclude does not recurse into records from `table`, but still includes referenced records.
func DontInclude(table string) Opt {
	return func(m *downloadOpts) {
		m.dontInclude[table] = true
	}
}

// LimitSize causes the clone to fail if more than `limit` records have been collected.
// You should use an estimate of a higher bound for how many records you expect to be exported.
// The default limit is 0, and 0 is treated as having no limit.
func LimitSize(limit int) Opt {
	return func(m *downloadOpts) {
		m.limit = limit
	}
}

const (
	// DumpTableKey is a special field present in every row of an export.
	// It can be used to determine which table the row is from.
	// Note that the export may have rows from a table interleaved with rows from other tables.
	DumpTableKey = "%_tablename"
)

type downloadOpts struct {
	dontInclude map[string]bool
	dontRecurse map[string]bool
	limit       int
}

// Download recursively downloads a dump of the database from a given starting point.
// the 2nd return is a trace that can help debug or understand what happened.
func Download(ctx context.Context, db Database, startTable, startColumn string, startId any, opts ...Opt) (DatabaseDump, []string, error) {
	options := downloadOpts{
		dontInclude: map[string]bool{},
		dontRecurse: map[string]bool{},
	}
	for _, o := range opts {
		o(&options)
	}

	type searchParams struct {
		TableName  string
		ColumnName string
		Value      any
	}

	lookupQueue := []searchParams{{TableName: startTable, ColumnName: startColumn, Value: startId}}
	lookupStatus := map[searchParams]bool{lookupQueue[0]: false}
	cloneInOrder := make(DatabaseDump, 0)
	fks := db.ForeignKeys()
	debugging := []string{}

	var recurse func(int) error
	recurse = func(i int) error {
		if options.limit != 0 && len(cloneInOrder) >= options.limit {
			debugging = append(debugging, "hit maximum recursion")
			return fmt.Errorf("%d export limit exceeded", options.limit)
		}

		if lookupStatus[lookupQueue[i]] {
			return nil
		}
		tname := lookupQueue[i].TableName
		conditions := make(map[string][]any, 1)
		ors := make([]string, 0)
		for _, l := range lookupQueue[i:] {
			if l.TableName != tname || lookupStatus[l] {
				continue
			}
			conditions[l.ColumnName] = append(conditions[l.ColumnName], l.Value)
			lookupStatus[l] = true
			ors = append(ors, fmt.Sprintf(`%s=%v`, l.ColumnName, l.Value))
		}

		// ask the DB implementation for matching rows
		foundInThisScan, err := db.SelectMatchingRows(tname, conditions)
		if err != nil {
			return err
		}

		debugging = append(debugging, fmt.Sprintf("select `%s` where `%s`: %d rows", tname, strings.Join(ors, " or "), len(foundInThisScan)))

		for _, res := range foundInThisScan {
			res[DumpTableKey] = tname

			for _, fk := range fks {
				if fk.BaseTable != tname || options.dontRecurse[fk.BaseTable] || options.dontInclude[fk.ReferencingTable] {
					continue
				}
				// foreign keys pointing to this record can come later
				lookup := searchParams{TableName: fk.ReferencingTable, ColumnName: fk.ReferencingCol, Value: res[fk.BaseCol]}
				if _, ok := lookupStatus[lookup]; !ok {
					lookupQueue = append(lookupQueue, lookup)
					lookupStatus[lookup] = false
				}
			}
			for _, fk := range fks {
				if fk.ReferencingTable != tname || res[fk.ReferencingCol] == nil || options.dontInclude[fk.BaseTable] {
					continue
				}
				// foreign keys referenced by this record must be grabbed before this record
				lookup := searchParams{TableName: fk.BaseTable, ColumnName: fk.BaseCol, Value: res[fk.ReferencingCol]}

				// if its not in there, or if we haven't collected it yet
				if !lookupStatus[lookup] {
					// immediately recurse
					lookupQueue = append(lookupQueue, lookup)
					if err := recurse(len(lookupQueue) - 1); err != nil {
						return err
					}
				}
			}
		}
		cloneInOrder = append(cloneInOrder, foundInThisScan...)
		return nil
	}

	// we use a buffer of search queries so we can batch them
	// but we still need to "try" every one, even though some will be batched by earlier calls
	for i := 0; i < len(lookupQueue); i++ {
		if err := recurse(i); err != nil {
			return nil, debugging, err
		}
	}

	return cloneInOrder, debugging, nil
}

// Upload uploads, in naive order, every record in a dump.
// It mutates the elements of `dump`, so you can track changes (for example new primary keys).
func Upload(ctx context.Context, db Database, dump DatabaseDump) error {
	return db.Insert(dump...)
}
