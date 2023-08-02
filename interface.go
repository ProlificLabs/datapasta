package datapasta

import (
	"fmt"
	"log"
)

// Database is the abstraction between the cloning tool and the database.
// The NewPostgres.NewClient method gives you an implementation for Postgres.
type Database interface {
	// SelectMatchingRows must return unseen records.
	// a Database can't be reused between clones, because it must do internal deduping.
	// `conds` will be a map of columns and the values they can have.
	SelectMatchingRows(tname string, conds map[string][]any) ([]map[string]any, error)

	// insert one record, returning the new id
	InsertRecord(record map[string]any) (any, error)

	// apply the updates from the cols to the row
	Update(id RecordID, cols map[string]any) error

	// delete the row
	Delete(id RecordID) error

	// Insert uploads a batch of records.
	// a Destination can't generally be reused between clones, as it may be inside a transaction.
	// it's recommended that callers use a Database that wraps a transaction.
	//
	// the records will have primary keys which must be handled.
	// the Database is responsible for exposing the resulting primary key mapping in some manner.
	Insert(records ...map[string]any) error

	// Mapping must return whatever mapping has been created by prior Inserts.
	// the implementation may internally choose to track this in the database or in memory.
	Mapping() ([]Mapping, error)

	// get foriegn key mapping
	ForeignKeys() []ForeignKey

	// get primary key mapping
	PrimaryKeys() map[string]string
}

// ForeignKey contains every RERENCING column and the BASE column it refers to.
// This is used to recurse the database as a graph.
// Database implementations must provide a complete list of references.
type ForeignKey struct {
	BaseTable        string `json:"base_table"`
	BaseCol          string `json:"base_col"`
	ReferencingTable string `json:"referencing_table"`
	ReferencingCol   string `json:"referencing_col"`
}

type RecordID struct {
	Table      string
	PrimaryKey any
}

func (r RecordID) String() string {
	return fmt.Sprintf(`%s(%v)`, r.Table, r.PrimaryKey)
}

func GetRowIdentifier(pks map[string]string, row map[string]any) RecordID {
	table := row[DumpTableKey].(string)
	pk, ok := row[pks[table]]
	if !ok {
		panic("unable to get row identifier")
	}
	return RecordID{Table: table, PrimaryKey: pk}
}

type Mapping struct {
	RecordID
	OriginalID any
}

var LogFunc = log.Printf
