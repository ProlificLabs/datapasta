package sqlclone

type TableReference struct {
	table_name             string
	column_name            string
	referenced_table_name  string
	referenced_column_name string
}

// Constructor function
func NewTableReference(t string, c string, rt string, rc string) *TableReference {
	ref := &TableReference{
		table_name:             t,
		column_name:            c,
		referenced_table_name:  rt,
		referenced_column_name: rc,
	}

	return ref
}
