package sqlclone

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	_ "github.com/lib/pq"
)

type DatabaseDump map[string][]map[string]string
type Mapping map[string]map[string]string

// gets all related rows of the starting points as specified in the download options from source database
// returns collected data as a DatabaseDump of the structure: map[string][]map[string]string
func Download(db database, options *downloadOptions) (DatabaseDump, error) {
	references, err := db.getReferences()
	if err != nil {
		return nil, err
	}

	dumpCache := make(map[string]bool, 0)
	queryCache := make(map[string]bool, 0)
	databaseDump := make(DatabaseDump)
	for _, sp := range options.start_points {
		databaseDump, dumpCache, queryCache, err = getDataRecursively(db, references, databaseDump, dumpCache, queryCache, options.dont_recurse, sp.table, sp.column, sp.value)
	}
	return databaseDump, err
}

func getDataRecursively(db database, references References, databaseDump DatabaseDump, dumpCache map[string]bool, queryCache map[string]bool, dont_recurse []string, table_name string, col string, val string) (DatabaseDump, map[string]bool, map[string]bool, error) {
	rows, err := db.getRows(table_name, col, val)
	queryCache[query_hash(table_name, col, val)] = true

	if err != nil {
		return nil, nil, nil, err
	}
	for _, r := range rows {
		rowSummary := row_hash(table_name, r)

		_, ok := dumpCache[rowSummary]
		if !ok {
			databaseDump[table_name] = append(databaseDump[table_name], r)
			dumpCache[rowSummary] = true
			var df = getReferencesFromTable(references, table_name)
			for _, d := range df {
				_, ok := queryCache[query_hash(d.referenced_table_name, d.referenced_column_name, r[d.column_name])]
				if !ok {
					getDataRecursively(db, references, databaseDump, dumpCache, queryCache, dont_recurse, d.referenced_table_name, d.referenced_column_name, r[d.column_name])
				}
			}

			var dr = getReferencesToTable(references, table_name)
			for _, d := range dr {
				_, ok := queryCache[query_hash(d.table_name, d.column_name, val)]
				if !ok && !sliceContains(dont_recurse, d.table_name) {
					getDataRecursively(db, references, databaseDump, dumpCache, queryCache, dont_recurse, d.table_name, d.column_name, r[d.referenced_column_name])
				}
			}
		}
	}
	return databaseDump, dumpCache, queryCache, nil
}

func Upload(db database, data DatabaseDump) (Mapping, error) {
	tx, err := db.newTransaction()
	if err != nil {
		return nil, err
	}

	mapping, err := upload(tx, db, data)
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("could not complete insertion: %s", err)
	}

	// if all inserts succeeded, commit the transaction
	err = tx.Commit()
	if err != nil {
		return mapping, fmt.Errorf("commit failed: %q", err)
	}
	return mapping, nil
}

// inserts all downloaded rows in the DatabaseDump into target database
// returns a map of the structure map[string]map[string]string showing which identifiers in source database correspond to which identifiers in target database
func upload(tx *sql.Tx, db database, data DatabaseDump) (Mapping, error) {
	order, err := db.getDependencyOrder()
	if err != nil {
		return nil, err
	}

	references, err := db.getReferences()
	if err != nil {
		return nil, err
	}

	primaryKeys, err := db.getPrimaryKeys()
	if err != nil {
		return nil, err
	}

	mapping := make(Mapping)
	for _, t := range order {
		ok, c := isTableSelfReferencing(references, t)
		if ok {
			sort.Slice(data[t], func(i, j int) bool {
				v1 := fmt.Sprintf("%v", data[t][i][c])
				v2 := fmt.Sprintf("%v", data[t][j][c])
				return v1 < v2
			})
		}
		for _, r := range data[t] {
			mapping, err = uploadRow(db, tx, primaryKeys, references, mapping, t, r)
			if err != nil {
				return nil, fmt.Errorf("upload could not be completed: %q", err)
			}
		}
	}
	return mapping, nil
}

// insert a row into the target database and update the mapping if necessary
func uploadRow(db database, tx *sql.Tx, primaryKeys map[string][]string, references References, mapping Mapping, table_name string, data map[string]string) (Mapping, error) {
	columns := make([]string, 0)
	primary_key := ""
	for key := range data {
		if !sliceContains(primaryKeys[table_name], key) {
			columns = append(columns, key)
		} else {
			primary_key = key // column that has an auto value
		}
	}

	values := make([]string, 0)
	for _, key := range columns {
		d, exists := getReference(references[table_name], key)
		if exists && data[key] != "<nil>" {
			// column contains a value that references another table
			// --> we need to use the updated value in the reference map
			ids, exists := mapping[d.referenced_table_name]
			if exists {
				values = append(values, ids[fmt.Sprintf("%v", data[key])])
			} else {
				// should never be the case as we put the new ids into mapping, but just in case this would use the old value
				// todo: should we abort with error message?
				values = append(values, data[key])
			}
		} else {
			values = append(values, data[key])
		}
	}

	lastInsertId, err := db.insertRow(tx, table_name, columns, values, primary_key)
	if err != nil {
		return mapping, err
	}

	if lastInsertId != -1 {
		// update mapping
		ids, exists := mapping[table_name]
		if exists {
			ids[fmt.Sprintf("%v", data["id"])] = fmt.Sprintf("%d", lastInsertId)
		} else {
			// first entry
			mapping[table_name] = map[string]string{fmt.Sprintf("%v", data["id"]): fmt.Sprintf("%d", lastInsertId)}
		}
	}

	return mapping, nil
}

// ----------------------------
// ----- HELPER FUNCTIONS -----
// ----------------------------
func row_hash(table_name string, data map[string]string) string {
	h := table_name
	sortedKeys := make([]string, 0)
	for k := range data {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	values := make([]string, 0)
	values = append(values, table_name)
	for _, k := range sortedKeys {
		h += data[k]
		values = append(values, data[k])
	}

	hasher := sha256.New()
	hasher.Write([]byte(strings.Join(values, "|")))
	hash := hex.EncodeToString(hasher.Sum(nil))
	return hash
}

func query_hash(table_name string, colum_name string, value string) string {
	values := make([]string, 0)
	values = append(values, table_name)
	values = append(values, colum_name)
	values = append(values, value)

	hasher := sha256.New()
	hasher.Write([]byte(strings.Join(values, "|")))
	hash := hex.EncodeToString(hasher.Sum(nil))
	return hash
}

func sliceContains(mySlice []string, searchString string) bool {
	for _, s := range mySlice {
		if s == searchString {
			return true
		}
	}
	return false
}
