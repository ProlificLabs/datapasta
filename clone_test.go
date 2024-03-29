package datapasta_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ProlificLabs/datapasta"
	"github.com/stretchr/testify/assert"
)

func TestDownloadUpload(t *testing.T) {
	db, assert := testDB{T: t}, assert.New(t)
	res, _, err := datapasta.Download(context.Background(), db, "company", "id", 10)
	assert.NoError(err)
	t.Log(res)

	assert.Equal(10, res[0]["id"])
	assert.Equal("produces socks", res[1]["desc"])
	assert.Equal("socks", res[2]["name"])
	assert.Equal("socks are cool", res[3]["detail"])

	// users are expected to do some cleanup, so test that it works
	for _, row := range res {
		cleanup(row)
	}

	assert.NoError(datapasta.Upload(context.Background(), db, res))

	assert.Equal(11, res[0]["id"])
	assert.Equal(12, res[1]["id"])
	assert.Equal(13, res[2]["id"])
	assert.Equal(11, res[3]["company_id"])
}

func cleanup(row map[string]any) {
	if row[datapasta.DumpTableKey] == "company" {
		row["api_key"] = "obfuscated"
	}
}

type testDB struct{ *testing.T }

// return unseen records
func (d testDB) SelectMatchingRows(tname string, conds map[string][]any) ([]map[string]any, error) {
	d.Logf("SELECT FROM %s WHERE %#v", tname, conds)

	switch tname {
	case "company":
		if conds["id"][0] == 10 {
			return []map[string]any{{"id": 10, "api_key": "secret_api_key"}}, nil
		}
	case "product":
		if conds["factory_id"] != nil {
			// we revisit this table because its a dependency of factory as well
			return nil, nil
		}
		if conds["company_id"][0] == 10 {
			return []map[string]any{{"id": 5, "name": "socks", "company_id": 10, "factory_id": 23}}, nil
		}
	case "factory":
		if conds["id"][0] == 23 {
			return []map[string]any{{"id": 23, "desc": "produces socks"}}, nil
		}
	case "company_details":
		if conds["company_id"][0] == 10 {
			return []map[string]any{{"company_id": 10, "detail": "socks are cool"}}, nil
		}
	}

	return nil, fmt.Errorf("no mock for %s where %#v", tname, conds)
}

func (d testDB) PrimaryKeys() map[string]string {
	return nil
}

func (d testDB) InsertRecord(map[string]any) (any, error) { return nil, nil }

// apply the updates from the cols to the row
func (d testDB) Update(id datapasta.RecordID, cols map[string]any) error { return nil }

// delete the row
func (d testDB) Delete(id datapasta.RecordID) error { return nil }

func (d testDB) Mapping() ([]datapasta.Mapping, error) { return nil, nil }

// upload a batch of records
func (d testDB) Insert(records ...map[string]any) error {
	for _, m := range records {
		d.Logf("inserting %#v", m)

		if m[datapasta.DumpTableKey] == "company" && m["id"] == 10 {
			if m["api_key"] != "obfuscated" {
				d.Errorf("didn't obfuscated company 9's api key, got %s", m["api_key"])
			}
			m["id"] = 11
			continue
		}
		if m[datapasta.DumpTableKey] == "factory" && m["id"] == 23 {
			m["id"] = 12
			continue
		}
		if m[datapasta.DumpTableKey] == "product" && m["id"] == 5 {
			m["id"] = 13
			continue
		}
		if m[datapasta.DumpTableKey] == "company_details" && m["company_id"] == 10 {
			m["company_id"] = 11
			continue
		}
		return fmt.Errorf("unexpected insert: %#v", m)
	}
	return nil
}

// get foriegn key mapping
func (d testDB) ForeignKeys() []datapasta.ForeignKey {
	return []datapasta.ForeignKey{
		{
			BaseTable: "company", BaseCol: "id",
			ReferencingTable: "product", ReferencingCol: "company_id",
		},
		{
			BaseTable: "factory", BaseCol: "id",
			ReferencingTable: "product", ReferencingCol: "factory_id",
		},
		{
			BaseTable: "company", BaseCol: "id",
			ReferencingTable: "company_details", ReferencingCol: "company_id",
		},
	}
}

var _ datapasta.Database = testDB{}
