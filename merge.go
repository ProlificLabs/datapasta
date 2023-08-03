package datapasta

import (
	"fmt"
)

type MergeAction struct {
	ID     RecordID
	Action string
	Data   map[string]any
}

func (ma MergeAction) String() string {
	if ma.Action == "delete" {
		return fmt.Sprintf(`%s %s`, ma.Action, ma.ID)
	}
	return fmt.Sprintf(`%s %s %d columns`, ma.Action, ma.ID, len(ma.Data))
}

func FindRow(table, pk string, id any, dump DatabaseDump) map[string]any {
	if id == nil {
		return nil
	}
	needle := RecordID{Table: table, PrimaryKey: id}
	for _, d := range dump {
		test := RecordID{Table: d[DumpTableKey].(string), PrimaryKey: d[pk]}
		if test.String() == needle.String() {
			return d
		}
	}
	return nil
}

func FindMapping(id RecordID, mapp []Mapping) Mapping {
	if id.PrimaryKey == nil {
		return Mapping{RecordID: id, OriginalID: id.PrimaryKey}
	}
	for _, m := range mapp {
		if m.RecordID.String() == id.String() {
			return m
		}
	}
	LogFunc("no mapping found for %s (%T %v)", id.Table, id.PrimaryKey, id.PrimaryKey)
	return Mapping{RecordID: id, OriginalID: id.PrimaryKey}
}

// reverse all the primary keys of a dump
func ReversePrimaryKeyMapping(pks map[string]string, mapp []Mapping, dump DatabaseDump) {
	for _, row := range dump {
		table := row[DumpTableKey].(string)
		pk, hasPk := pks[table]
		if !hasPk {
			LogFunc("no pk for %s", table)
			continue
		}
		m := FindMapping(RecordID{Table: table, PrimaryKey: row[pk]}, mapp)
		row[pk] = m.OriginalID
	}
}

// reverse all the foreign keys of an indivdual row
func ReverseForeignKeyMappingRow(fks []ForeignKey, mapp []Mapping, row map[string]any) {
	update := func(row map[string]any, col, otherTable string) {
		target := RecordID{Table: otherTable, PrimaryKey: row[col]}
		m := FindMapping(target, mapp)
		row[col] = m.OriginalID
	}

	table := row[DumpTableKey].(string)
	for _, fk := range fks {
		if fk.ReferencingTable != table {
			continue
		}
		update(row, fk.ReferencingCol, fk.BaseTable)
	}
}

// reverse all the foreign keys of a dump
func ReverseForeignKeyMapping(fks []ForeignKey, mapp []Mapping, rows DatabaseDump) {
	for _, row := range rows {
		ReverseForeignKeyMappingRow(fks, mapp, row)
	}
}

// find rows in "from" that are missing in "in"
func FindMissingRows(pks map[string]string, from, in DatabaseDump) DatabaseDump {
	out := make(DatabaseDump, 0)
	for _, row := range from {
		table := row[DumpTableKey].(string)
		pk, hasPk := pks[table]
		if !hasPk {
			continue
		}
		match := FindRow(table, pk, row[pk], in)
		if match != nil {
			continue
		}

		out = append(out, row)
	}
	return out
}

// return a map of updates or deletes that would make "in" equal "from"
// the map key is the table and column that changed
// and the value is the new value
func FindModifiedRows(pks map[string]string, from, in DatabaseDump) map[RecordID]map[string]any {
	all := make(map[RecordID]map[string]any)
	for _, row := range from {
		table := row[DumpTableKey].(string)
		pk, hasPk := pks[table]
		if !hasPk {
			continue
		}
		match := FindRow(table, pk, row[pk], in)
		if match == nil {
			continue
		}

		changes := make(map[string]any)
		for k, v := range match {
			if fmt.Sprintf(`%v`, v) != fmt.Sprintf(`%v`, row[k]) {
				changes[k] = row[k]
			}
		}

		if len(changes) == 0 {
			continue
		}
		all[RecordID{Table: table, PrimaryKey: row[pk]}] = changes
	}
	return all
}

func ApplyMergeStrategy(db Database, mapp []Mapping, mas []MergeAction) error {
	fks := db.ForeignKeys()

	for _, ma := range mas {
		if ma.Action != "create" {
			continue
		}
		ma.Data[DumpTableKey] = ma.ID.Table
		ReverseForeignKeyMappingRow(fks, mapp, ma.Data)
		id, err := db.InsertRecord(ma.Data)
		if err != nil {
			return fmt.Errorf(`creating %s: %s`, ma.ID, err.Error())
		}
		mapp = append(mapp, Mapping{RecordID: ma.ID, OriginalID: id})
	}

	// do all the creates *while* updating the mapping
	// do all the updates
	for _, ma := range mas {
		if ma.Action != "update" {
			continue
		}
		ma.Data[DumpTableKey] = ma.ID.Table
		ReverseForeignKeyMappingRow(fks, mapp, ma.Data)
		delete(ma.Data, DumpTableKey)
		if err := db.Update(ma.ID, ma.Data); err != nil {
			return fmt.Errorf(`updating %s: %s`, ma.ID, err.Error())
		}
	}

	// do the all deletes
	for _, ma := range mas {
		if ma.Action != "delete" {
			continue
		}
		if err := db.Delete(ma.ID); err != nil {
			return fmt.Errorf(`deleting %s: %s`, ma.ID, err.Error())
		}
	}

	return nil
}

// GenerateMergeStrategy returns every update or delete needed to merge branch into main
// note that conflicts will be intermingled in updates and deletes
func GenerateMergeStrategy(pks map[string]string, base, main, branch DatabaseDump) []MergeAction {
	out := make([]MergeAction, 0)

	deletedInMain := make(map[string]bool)
	for _, deleted := range FindMissingRows(pks, base, main) {
		deletedInMain[GetRowIdentifier(pks, deleted).String()] = true
	}
	editedInMain := make(map[string]bool)
	for id := range FindModifiedRows(pks, main, base) {
		editedInMain[id.String()] = true
	}

	created := FindMissingRows(pks, branch, base)
	for _, m := range created {
		id := GetRowIdentifier(pks, m)
		delete(m, pks[id.Table])
		out = append(out, MergeAction{id, "create", m})
	}

	changes := FindModifiedRows(pks, branch, base)
	for id, c := range changes {
		if editedInMain[id.String()] {
			out = append(out, MergeAction{id, "conflicting_double_update", c})
			continue
		}
		if deletedInMain[id.String()] {
			out = append(out, MergeAction{id, "conflicting_update_deleted", c})
			continue
		}
		out = append(out, MergeAction{id, "update", c})
	}

	deleted := FindMissingRows(pks, base, branch)
	for _, m := range deleted {
		id := GetRowIdentifier(pks, m)
		if editedInMain[id.String()] {
			out = append(out, MergeAction{id, "conflict_delete_updated", m})
			continue
		}
		out = append(out, MergeAction{id, "delete", nil})
	}

	return out
}
