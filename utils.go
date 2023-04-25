package datapasta

import (
	"testing"
)

// TestDatabaseImplementation is a utility to test an implemention. it just makes sure that 1 row gets cloned correctly.
// the row you choose as the starting point must have be referenced as a foreign key by some other table.
// ci is not set up, but this function is one of many tests that run against local postgres for development.
func TestDatabaseImplementation(t *testing.T, db Database, startTable, startCol string, startVal any) {
	// find some interesting columns
	cols := make(map[string]bool, 0)
	for _, fk := range db.ForeignKeys() {
		if fk.BaseTable != startTable {
			continue
		}
		cols[fk.BaseCol] = true
	}

	found, err := db.SelectMatchingRows(startTable, map[string][]any{startCol: {startVal}})
	if err != nil {
		t.Fatalf("error getting rows: %s", err.Error())
		return
	}

	t.Logf("clone test: first scan found: %v", found)

	old := make(map[string]any, len(found[0]))
	for k, v := range found[0] {
		old[k] = v
	}

	fkm := NewForeignKeyMapper(db)
	if err := db.Insert(fkm, found[0]); err != nil {
		t.Fatalf("error inserting row: %s", err.Error())
		return
	}

	foundDifferences := 0
	for k, v := range found[0] {
		if !cols[k] {
			continue
		}
		t.Logf("clone test: col %s, old %v, new %v", k, v, old[k])
		if old[k] != v {
			foundDifferences++
		}
	}
	t.Logf("clone test: %d differences found", foundDifferences)

	// search again and expect zero results
	found2, err := db.SelectMatchingRows(startTable, map[string][]any{startCol: {startVal}})
	if err != nil {
		t.Fatalf("error getting rows again: %s", err.Error())
		return
	}
	if len(found2) > 0 {
		t.Fatalf("duplicate call returned results: %v", found2)
		return
	}
}

