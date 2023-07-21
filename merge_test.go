package datapasta

import (
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
