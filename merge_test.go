package datapasta

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindNewRows(t *testing.T) {
	main := DatabaseDump{
		{
			DumpTableKey: "person",
			"id":         10,
		},
	}
	branch := DatabaseDump{
		{
			DumpTableKey: "person",
			"id":         10,
		},
		{
			DumpTableKey: "person",
			"id":         11,
		},
	}
	pks := map[string]string{"person": "id"}

	newRows := FindMissingRows(pks, branch, main)

	ok := assert.New(t)
	ok.Len(newRows, 1)
	ok.Equal(11, newRows[0]["id"])
}

func TestReversePrimaryKeyMapping(t *testing.T) {
	branch := DatabaseDump{
		{
			DumpTableKey: "person",
			"id":         10,
		},
		{
			DumpTableKey: "person",
			"id":         11,
		},
	}
	mapp := []Mapping{
		{TableName: "person", OriginalID: 8, NewID: 11},
	}
	pks := map[string]string{"person": "id"}

	ReversePrimaryKeyMapping(pks, mapp, branch)

	ok := assert.New(t)
	ok.Equal(10, branch[0]["id"])
	ok.Equal(8, branch[1]["id"])
}

func TestFindModifiedRows(t *testing.T) {
	pks := map[string]string{"person": "id"}

	main := DatabaseDump{
		{
			DumpTableKey: "person",
			"id":         10,
			"name":       "alice",
		},
	}
	branch := DatabaseDump{
		{
			DumpTableKey: "person",
			"id":         10,
			"name":       "alicia",
		},
	}

	mods := FindModifiedRows(pks, branch, main)

	change := RecordID{Table: "person", PrimaryKey: 10}

	ok := assert.New(t)
	ok.Len(mods, 1)
	ok.Len(mods[change], 1)
	ok.Equal("alicia", mods[change]["name"])
}

func TestReverseForeignKeyMapping(t *testing.T) {
	main := DatabaseDump{
		{
			DumpTableKey: "person",
			"country":    20,
		},
		{
			DumpTableKey: "country",
			"id":         10,
		},
	}

	fks := []ForeignKey{{ReferencingTable: "person", ReferencingCol: "country", BaseTable: "country", BaseCol: "id"}}
	mapp := []Mapping{{TableName: "country", OriginalID: 15, NewID: 20}}

	ReverseForeignKeyMapping(fks, mapp, main)

	ok := assert.New(t)
	ok.Equal(15, main[0]["country"])
}

func TestGenerateMergeStrategy(t *testing.T) {
	base := DatabaseDump{
		{
			DumpTableKey: "person",
			"id":         10,
			"name":       "left_alone",
		},
		{
			DumpTableKey: "person",
			"id":         11,
			"name":       "name_changed_in_main",
		},
		{
			DumpTableKey: "person",
			"id":         12,
			"name":       "name_changed_in_branch",
		},
		{
			DumpTableKey: "person",
			"id":         13,
			"name":       "deleted_in_main",
		},
		{
			DumpTableKey: "person",
			"id":         14,
			"name":       "deleted_in_branch",
		},

		// conflicts
		{
			DumpTableKey: "person",
			"id":         17,
			"name":       "deleted_main_updated_branch",
		},
		{
			DumpTableKey: "person",
			"id":         18,
			"name":       "deleted_branch_updated_main",
		},
		{
			DumpTableKey: "person",
			"id":         19,
			"name":       "deleted_both", // not a conflict
		},
		{
			DumpTableKey: "person",
			"id":         20,
			"name":       "updated_both",
		},
	}
	main := DatabaseDump{
		{
			DumpTableKey: "person",
			"id":         10,
			"name":       "left_alone",
		},
		{
			DumpTableKey: "person",
			"id":         11,
			"name":       "name_changed_in_main_completed",
		},
		{
			DumpTableKey: "person",
			"id":         12,
			"name":       "name_changed_in_branch",
		},
		{
			DumpTableKey: "person",
			"id":         14,
			"name":       "deleted_in_branch",
		},
		{
			DumpTableKey: "person",
			"id":         15,
			"name":       "created_in_main",
		},

		// conflicts
		{
			DumpTableKey: "person",
			"id":         18,
			"name":       "deleted_branch_updated_main_complete",
		},

		{
			DumpTableKey: "person",
			"id":         20,
			"name":       "updated_both_complete_main",
		},
	}
	branch := DatabaseDump{
		{
			DumpTableKey: "person",
			"id":         10,
			"name":       "left_alone",
		},
		{
			DumpTableKey: "person",
			"id":         11,
			"name":       "name_changed_in_main",
		},
		{
			DumpTableKey: "person",
			"id":         12,
			"name":       "name_changed_in_branch_completed",
		},
		{
			DumpTableKey: "person",
			"id":         13,
			"name":       "deleted_in_main",
		},
		{
			DumpTableKey: "person",
			"id":         16,
			"name":       "created_in_branch",
		},

		// conflicts
		{
			DumpTableKey: "person",
			"id":         17,
			"name":       "deleted_main_updated_branch_complete",
		},
		{
			DumpTableKey: "person",
			"id":         20,
			"name":       "updated_both_complete_branch",
		},
	}
	pks := map[string]string{"person": "id"}

	actions := GenerateMergeStrategy(pks, base, main, branch)

	for _, ma := range actions {
		t.Logf("%#v", ma)
	}

	ok := assert.New(t)
	ok.Len(actions, 7)

	// creation is not included in the merge
	ok.Equal("create", actions[0].Action)
	ok.Equal(16, actions[0].ID.PrimaryKey)

	ok.Equal("update", actions[1].Action)
	ok.Equal(12, actions[1].ID.PrimaryKey)

	ok.Contains(actions, MergeAction{ID: RecordID{Table: "person", PrimaryKey: 20}, Action: "conflict", Data: map[string]interface{}{"name": "updated_both_complete_branch"}})
	ok.Contains(actions, MergeAction{ID: RecordID{Table: "person", PrimaryKey: 12}, Action: "update", Data: map[string]interface{}{"name": "name_changed_in_branch_completed"}})
	ok.Contains(actions, MergeAction{ID: RecordID{Table: "person", PrimaryKey: 20}, Action: "conflict", Data: map[string]interface{}{"name": "updated_both_complete_branch"}})

	ok.Equal("conflict", actions[5].Action)
	ok.Equal(18, actions[5].ID.PrimaryKey)

	ok.Equal("delete", actions[6].Action)
	ok.Equal(19, actions[6].ID.PrimaryKey)
}

func TestGenerateMergeStrategyWithMapping(t *testing.T) {
	base := DatabaseDump{
		{
			DumpTableKey: "person",
			"id":         10,
			"friend":     9,
		},
		{
			DumpTableKey: "person",
			"id":         11,
			"friend":     10,
		},
	}
	main := DatabaseDump{
		{
			DumpTableKey: "person",
			"id":         10,
			"friend":     9,
		},
		{
			DumpTableKey: "person",
			"id":         11,
			"friend":     10,
		},
	}
	branch := DatabaseDump{
		{
			DumpTableKey: "person",
			"id":         20,
			"friend":     19,
		},
		{
			DumpTableKey: "person",
			"id":         22,
		},
		{
			DumpTableKey: "person",
			"id":         21,
			"friend":     22,
		},
	}
	pks := map[string]string{"person": "id"}
	fks := []ForeignKey{
		{
			BaseTable:        "person",
			BaseCol:          "id",
			ReferencingTable: "person",
			ReferencingCol:   "friend",
		},
	}
	mapping := []Mapping{
		{
			TableName:  "person",
			OriginalID: 9,
			NewID:      19,
		},
		{
			TableName:  "person",
			OriginalID: 10,
			NewID:      20,
		},
		{
			TableName:  "person",
			OriginalID: 11,
			NewID:      21,
		},
	}

	ReversePrimaryKeyMapping(pks, mapping, branch)
	ReverseForeignKeyMapping(fks, mapping, branch)
	mas := GenerateMergeStrategy(pks, base, main, branch)

	ok := assert.New(t)
	ok.Len(mas, 2)

	ok.Equal("create", mas[0].Action)
	ok.Equal(22, mas[0].ID.PrimaryKey)

	ok.Equal("update", mas[1].Action)
	ok.Equal(11, mas[1].ID.PrimaryKey)
	ok.Len(mas[1].Data, 1)
	ok.Equal(22, mas[1].Data["friend"])
}

func TestApplyMergeStrategy(t *testing.T) {
	ok := assert.New(t)

	mapp := []Mapping{
		{"user", 1, 3},
		{"user", 2, 4},
	}
	db := &mergeDB{T: t, id: 5, data: map[any]map[string]any{}}
	db.data[1] = map[string]any{"name": "alica", "friend": 2}
	db.data[2] = map[string]any{"name": "bob"}

	// "alice" (1) is friends with bob (2)
	// "alica" is cloned to 3
	// "bob" is cloned to 4
	// "alica" renames to "alicia" and becomes friends with "jeff" (5)
	// "bob" is deleted

	mas := []MergeAction{}
	mas = append(mas, MergeAction{
		ID:     RecordID{"user", 1},
		Action: "update",
		Data:   map[string]any{"name": "alicia", "friend": 5},
	})
	mas = append(mas, MergeAction{
		ID:     RecordID{"user", 5},
		Action: "create",
		Data:   map[string]any{"name": "jeff"},
	})
	mas = append(mas, MergeAction{
		ID:     RecordID{"user", 2},
		Action: "delete",
	})

	ok.NoError(ApplyMergeStrategy(db, mapp, mas))

	ok.Equal("alicia", db.data[1]["name"])
	ok.Equal("jeff", db.data[db.data[1]["friend"]]["name"])
	ok.NotContains(db.data, "bob")
}

type mergeDB struct {
	*testing.T
	id   int
	data map[any]map[string]any
}

// get foriegn key mapping
func (d *mergeDB) ForeignKeys() []ForeignKey {
	d.Log(`ForeignKeys`)
	return []ForeignKey{
		{
			BaseTable: "user", BaseCol: "id",
			ReferencingTable: "user", ReferencingCol: "friend",
		},
	}
}

func (d *mergeDB) InsertRecord(i map[string]any) (any, error) {
	d.Log(`InsertRecord`, i)
	d.id++
	d.data[d.id] = i
	return d.id, nil
}

func (d *mergeDB) Update(id RecordID, cols map[string]any) error {
	if _, ok := d.data[id.PrimaryKey]; !ok {
		return fmt.Errorf(`cant update nonexistant row %s`, id)
	}
	for k, v := range cols {
		d.data[id.PrimaryKey][k] = v
	}
	d.Log(`Update`, id, cols)
	return nil
}

// delete the row
func (d *mergeDB) Delete(id RecordID) error {
	if _, ok := d.data[id.PrimaryKey]; !ok {
		return fmt.Errorf(`cant delete nonexistant row %s`, id)
	}
	d.Log(`Delete`, id)
	return nil
}

// stubbed for interface

func (d mergeDB) Mapping() ([]Mapping, error)            { return nil, nil }
func (d mergeDB) PrimaryKeys() map[string]string         { return nil }
func (d mergeDB) Insert(records ...map[string]any) error { return nil }
func (d mergeDB) SelectMatchingRows(tname string, conds map[string][]any) ([]map[string]any, error) {
	return nil, nil
}

var _ Database = new(mergeDB)
