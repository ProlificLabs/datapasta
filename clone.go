package datapasta

import (
	"context"
	"fmt"
	"log"
	"strings"
)

// Database is the abstraction between the cloning tool and the database.
// The NewPostgres.NewClient method gives you an implementation for Postgres.
type Database interface {
	// get foriegn key mapping
	ForeignKeys() []ForeignKey

	// SelectMatchingRows must return unseen records.
	// a Database can't be reused between clones, because it must do internal deduping.
	// `conds` will be a map of columns and the values they can have.
	SelectMatchingRows(tname string, conds map[string][]any) ([]map[string]any, error)

	// Insert uploads a batch of records.
	// any changes to the records (such as newly generated primary keys) should mutate the record map directly.
	// a Destination can't generally be reused between clones, as it may be inside a transaction.
	// it's recommended that callers use a Database that wraps a transaction.
	Insert(records ...map[string]any) error
}

type (
	DatabaseDump []map[string]any
	Opt          func(map[string]bool)
)

type ForeignKey struct {
	BaseTable        string `json:"base_table"`
	BaseCol          string `json:"base_col"`
	ReferencingTable string `json:"referencing_table"`
	ReferencingCol   string `json:"referencing_col"`
}

func DontRecurse(table string) Opt {
	return func(m map[string]bool) {
		m["dontrecurse."+table] = true
	}
}

func DontInclude(table string) Opt {
	return func(m map[string]bool) {
		m["dontinclude."+table] = true
	}
}

const (
	// we store table and primary key names in the dump, using these keys
	// because it makes it much easier to transport and clone.
	// we *could* stop tracking primary key, but it saves some repeated work on the upload.
	DumpTableKey = "%_tablename"
)

const MAX_LEN = 50000

// DownloadWith recursively downloads a dump of the database from a given starting point.
// the 2nd return is a trace that can help debug or understand what happened.
func DownloadWith(ctx context.Context, db Database, startTable, startColumn string, startId any, opts ...Opt) (DatabaseDump, []string, error) {
	flags := map[string]bool{}
	for _, o := range opts {
		o(flags)
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
				if fk.BaseTable != tname || flags["dontrecurse."+fk.BaseTable] {
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
				if fk.ReferencingTable != tname || res[fk.ReferencingCol] == nil || flags["dontinclude."+fk.BaseTable] {
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
		if len(lookupQueue) >= MAX_LEN {
			debugging = append(debugging, "hit maximum recursion")
			return nil, debugging, nil
		}
	}
	return cloneInOrder, debugging, nil
}

// UploadWith uploads, in naive order, every record in a dump.
// It mutates the elements of `dump`, so you can track changes (for example new primary keys).
func UploadWith(ctx context.Context, db Database, dump DatabaseDump) error {
	// keep track of old columns and their new values
	changes := map[string]map[any]any{}

	for _, fk := range db.ForeignKeys() {
		// make sure we track changes on any column that is referenced
		changes[fk.BaseTable+`.`+fk.BaseCol] = map[any]any{}
	}

	for _, row := range dump {
		table := row[DumpTableKey].(string)
		for k, v := range row {
			for _, fk := range db.ForeignKeys() {
				if fk.ReferencingTable != table || fk.ReferencingCol != k || v == nil || changes[fk.BaseTable+`.`+fk.BaseCol] == nil {
					continue
				}

				newID, ok := changes[fk.BaseTable+`.`+fk.BaseCol][v]
				if !ok {
					log.Printf("unable to find mapped id for %s[%s]=%v in %s", table, k, v, fk.BaseTable)
				} else {
					row[k] = newID
				}
			}
		}

		copy := make(map[string]any, len(row))
		for k, v := range row {
			// does anyone care about this value?
			if changes[table+`.`+k] == nil {
				continue
			}
			copy[k] = v
		}

		if err := db.Insert(row); err != nil {
			return err
		}

		for k, v := range row {
			if changes[table+"."+k] == nil {
				continue
			}
			changes[table+"."+k][copy[k]] = v
		}
	}

	return nil
}
