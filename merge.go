package datapasta

import (
	"fmt"
	"log"
	"reflect"
)

type MergeAction struct {
	ID     RecordID
	Action string
	Data   map[string]any
}

func (ma MergeAction) String() string {
	return fmt.Sprintf(`%s %s %#v`, ma.Action, ma.ID, ma.Data)
}

func FindRow(table, pk string, id any, dump DatabaseDump) map[string]any {
	if id == nil {
		return nil
	}
	for _, d := range dump {
		if d[DumpTableKey] != table {
			continue
		}
		if d[pk] == id {
			return d
		}
	}
	return nil
}

func FindMapping(table string, id any, mapp []Mapping) Mapping {
	for _, m := range mapp {
		if m.TableName != table {
			continue
		}
		if m.NewID == id {
			log.Printf(`%s: %T %#v == %T %#v`, table, m.NewID, m.NewID, id, id)
			return m
		}
	}
	log.Printf("no mapping found for %s (%T %v)", table, id, id)
	return Mapping{TableName: table, OriginalID: id, NewID: id}
}

// reverse all the primary keys of a dump
func ReversePrimaryKeyMapping(pks map[string]string, mapp []Mapping, dump DatabaseDump) {
	for _, row := range dump {
		table := row[DumpTableKey].(string)
		pk, hasPk := pks[table]
		if !hasPk {
			log.Println("no pk for", table)
			continue
		}
		m := FindMapping(table, row[pk], mapp)
		row[pk] = m.OriginalID
	}
}

// reverse all the foreign keys of an indivdual row
func ReverseForeignKeyMappingRow(fks []ForeignKey, mapp []Mapping, row map[string]any) {
	update := func(row map[string]any, col, otherTable string) {
		for _, m := range mapp {
			if m.TableName != otherTable {
				continue
			}
			if m.NewID != row[col] {
				continue
			}
			row[col] = m.OriginalID
		}
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
			if !reflect.DeepEqual(v, row[k]) {
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
		mapp = append(mapp, Mapping{TableName: ma.ID.Table, NewID: ma.ID.PrimaryKey, OriginalID: id})
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

	deletedInMain := make(map[RecordID]bool)
	for _, deleted := range FindMissingRows(pks, base, main) {
		deletedInMain[GetRowIdentifier(pks, deleted)] = true
	}
	editedInMain := make(map[RecordID]bool)
	for id := range FindModifiedRows(pks, main, base) {
		editedInMain[id] = true
	}

	created := FindMissingRows(pks, branch, base)
	for _, m := range created {
		id := GetRowIdentifier(pks, m)
		delete(m, pks[id.Table])
		out = append(out, MergeAction{id, "create", m})
	}

	changes := FindModifiedRows(pks, branch, base)
	for id, c := range changes {
		if editedInMain[id] || deletedInMain[id] {
			out = append(out, MergeAction{id, "conflict", c})
			continue
		}
		out = append(out, MergeAction{id, "update", c})
	}

	deleted := FindMissingRows(pks, base, branch)
	for _, m := range deleted {
		id := GetRowIdentifier(pks, m)
		if editedInMain[id] {
			out = append(out, MergeAction{id, "conflict", m})
			continue
		}
		out = append(out, MergeAction{id, "delete", nil})
	}

	return out
}
