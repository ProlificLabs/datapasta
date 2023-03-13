package sqlclone

import (
	"database/sql"
	"fmt"
)

type References map[string][]TableReference
type postgresDB struct {
	*sql.DB
}

type database interface {
	getRows(string, string, string) ([]map[string]string, error)
	insertRow(*sql.Tx, string, []string, []string, string) (int, error)
	getTables() ([]string, error)
	getReferences() (References, error)
	getPrimaryKeys() (map[string][]string, error)
	getDependencyOrder() ([]string, error)
	newTransaction() (*sql.Tx, error)
}

func NewPostgresAdapter(host string, port int, user string, pw string, db_name string) (database, error) {
	postgres_db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, pw, db_name))
	if err != nil {
		return nil, fmt.Errorf("could not connect to PostgreSQL database")
	}
	return postgresDB{postgres_db}, nil
}

func (db postgresDB) newTransaction() (*sql.Tx, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("could not start transaction %s", err)
	}
	return tx, nil
}

// get rows from a table where a column has a certain value
func (db postgresDB) getRows(table_name string, col string, val string) ([]map[string]string, error) {
	ret := make([]map[string]string, 0)
	if val != "<nil>" {
		query := "SELECT * FROM " + table_name + " WHERE \"" + col + "\"='" + val + "'"
		fmt.Println(query)
		rows, err := db.Query(query)
		if err != nil {
			return nil, fmt.Errorf("query %s could not be executed: %s", query, err)
		}
		defer rows.Close()

		columns, _ := rows.Columns()
		values := make([]sql.NullString, len(columns))
		for rows.Next() {
			scanArgs := make([]interface{}, len(columns))
			for i := range values {
				scanArgs[i] = &values[i]
			}
			if err := rows.Scan(scanArgs...); err != nil {
				return nil, fmt.Errorf("values could not be extracted from row: %s", err)
			}

			resultMap := make(map[string]string)
			for i, value := range values {
				if !value.Valid { // value was NULL
					resultMap[columns[i]] = "<nil>"
				} else {
					resultMap[columns[i]] = value.String
				}
			}
			ret = append(ret, resultMap)
		}
	}
	return ret, nil
}

// insert a row with given column names and values into a database.
// if the table has a column with an automatically generated value,
// return that value after insertion, return -1 otherwise
func (db postgresDB) insertRow(tx *sql.Tx, table_name string, columns []string, values []string, primary_key string) (int, error) {
	cols := ""
	vals := ""
	for i, c := range columns {
		if c != primary_key {
			cols += "\"" + c + "\", "
			if values[i] != "<nil>" {
				vals += "'" + values[i] + "', "
			} else {
				vals += "NULL, "
			}
		}
	}
	cols = cols[:len(cols)-2]
	vals = vals[:len(vals)-2]

	query := "INSERT INTO " + table_name + " (" + cols + ") VALUES (" + vals + ")"

	lastInsertId := -1
	if primary_key != "" {
		query += " RETURNING " + primary_key
		fmt.Println(query)
		err := tx.QueryRow(query).Scan(&lastInsertId)
		if err != nil {
			return -1, fmt.Errorf("query %s could not be executed: %s", query, err)
		}
	} else {
		fmt.Println(query)
		_, err := tx.Exec(query)
		if err != nil {
			return -1, fmt.Errorf("query %s could not be executed: %s", query, err)
		}
	}
	return lastInsertId, nil
}

// get list of tables in the database
func (db postgresDB) getTables() ([]string, error) {
	var query = "" +
		"SELECT table_name  " +
		"FROM information_schema.tables " +
		"WHERE table_schema = 'public'"

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query %s could not be executed: %s", query, err)
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, fmt.Errorf("error extracting table name from result set: %s", err)
		}
		tables = append(tables, t)
	}
	return tables, nil
}

// get all references from all tables
func (db postgresDB) getReferences() (References, error) {
	var query = "" +
		"SELECT " +
		"conrelid::regclass table_name, " +
		"a1.attname column_name, " +
		"confrelid::regclass referenced_table, " +
		"a2.attname referenced_column_name " +
		"FROM (" +
		"select conrelid::regclass, confrelid::regclass, col, fcol " +
		"from pg_constraint, " +
		"lateral unnest(conkey) col, " +
		"lateral unnest(confkey) fcol " +
		"where contype = 'f'" +
		") s " +
		"JOIN pg_attribute a1 ON a1.attrelid = conrelid AND a1.attnum = col " +
		"JOIN pg_attribute a2 ON a2.attrelid = confrelid AND a2.attnum = fcol;"

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query %s could not be executed: %s", query, err)
	}
	defer rows.Close()

	references := make(References)
	for rows.Next() {
		var t, tc, rt, rtc string
		if err := rows.Scan(&t, &tc, &rt, &rtc); err != nil {
			return nil, fmt.Errorf("error extracting table reference from result set: %s", err)
		}
		references[t] = append(references[t], TableReference{table_name: t, column_name: tc, referenced_table_name: rt, referenced_column_name: rtc})
	}
	return references, nil
}

// get all tables that have primary keys and their primary keys
func (db postgresDB) getPrimaryKeys() (map[string][]string, error) {
	var query = "" +
		"SELECT tc.table_name, kc.column_name " +
		"FROM " +
		"information_schema.table_constraints tc, " +
		"information_schema.key_column_usage kc " +
		"WHERE " +
		"tc.constraint_type = 'PRIMARY KEY' " +
		"AND tc.table_schema = 'public' " +
		"AND kc.table_name = tc.table_name and kc.table_schema = tc.table_schema " +
		"AND kc.constraint_name = tc.constraint_name"

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query %s could not be executed: %s", query, err)
	}
	defer rows.Close()

	primary_keys := make(map[string][]string, 0)
	for rows.Next() {
		var t, c string
		if err := rows.Scan(&t, &c); err != nil {
			return nil, fmt.Errorf("error extracting primary key from result set: %s", err)
		}
		primary_keys[t] = append(primary_keys[t], c)
	}
	return primary_keys, nil
}

// returns the list of tables after a topological sort following Kahn's algorithm.
// this list will be used to perform cloning so that data is inserted into the target database
// before it is needed by referencing rows later on
func (db postgresDB) getDependencyOrder() ([]string, error) {
	references, err := db.getReferences()
	if err != nil {
		return nil, err
	}

	tables, err := db.getTables()
	if err != nil {
		return nil, err
	}

	visited := make([]string, 0)
	order := make([]string, 0)
	S := make([]string, 0)
	out_degrees := make(map[string]int, 0)

	for _, table := range tables {
		ref_tables := getReferencesFromTable(references, table)
		out_degrees[table] = len(ref_tables)

		self_referencing, _ := isTableSelfReferencing(references, table)
		if self_referencing {
			out_degrees[table]--
		}

		if out_degrees[table] == 0 {
			S = append(S, table)
		}
	}

	for len(S) != 0 {
		table := S[len(S)-1]
		order = append(order, table)
		visited = append(visited, table)

		S = S[:len(S)-1] // remove table from S
		edges := getReferencesToTable(references, table)
		for _, r := range edges {
			out_degrees[r.table_name]--
			if out_degrees[r.table_name] == 0 && !sliceContains(visited, r.table_name) {
				S = append(S, r.table_name)
			}
		}
	}
	return order, nil
}

func isTableSelfReferencing(references References, table_name string) (bool, string) {
	from := getReferencesFromTable(references, table_name)
	for _, d := range from {
		if d.referenced_table_name == table_name {
			return true, d.referenced_column_name
		}
	}
	return false, ""
}

func getReferencesToTable(references References, table_name string) []TableReference {
	var ret = make([]TableReference, 0)
	for _, value := range references {
		for _, d := range value {
			if d.referenced_table_name == table_name {
				ret = append(ret, d)
			}
		}
	}
	return ret
}

func getReferencesFromTable(references References, table_name string) []TableReference {
	return references[table_name]
}

func getReference(input []TableReference, searchString string) (TableReference, bool) {
	var ret TableReference
	for _, d := range input {
		if d.column_name == searchString {
			return d, true
		}
	}
	return ret, false
}
