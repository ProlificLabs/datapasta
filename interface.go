package datapasta

// Database is the abstraction between the cloning tool and the database.
// The NewPostgres.NewClient method gives you an implementation for Postgres.
type Database interface {
	
	// SelectMatchingRows must return unseen records.
	// a Database can't be reused between clones, because it must do internal deduping.
	// `conds` will be a map of columns and the values they can have.
	SelectMatchingRows(tname string, conds map[string][]any) ([]map[string]any, error)
	
	// Insert uploads a batch of records.
	// any changes to the records (such as newly generated primary keys) should mutate the record map directly.
	// a Destination can't generally be reused between clones, as it may be inside a transaction.
	// it's recommended that callers use a Database that wraps a transaction.
	Insert(records ...map[string]any) error
	
	// get foriegn key mapping
	ForeignKeys() []ForeignKey
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