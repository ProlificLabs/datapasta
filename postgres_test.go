package datapasta

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
)

const largeCompany = 2127511261
const testCompany = 1296515245

func TestWithLocalPostgres(t *testing.T) {
	t.Skipf("test is used for development against real pulley schema")

	company := testCompany

	ok := assert.New(t)
	conn, err := pgxpool.Connect(context.Background(), `postgresql://postgres:postgres@localhost:5432/postgres`)
	ok.NoError(err)
	db, err := NewPostgres(context.Background(), conn)
	ok.NoError(err)

	tx, err := conn.BeginTx(context.Background(), pgx.TxOptions{})
	ok.NoError(err)
	defer tx.Rollback(context.Background())

	cli, err := db.NewBatchClient(context.Background(), tx)
	ok.NoError(err)

	exportOpts := []Opt{
		DontInclude("user"),
		DontInclude("firm"),
		DontRecurse("stakeholder"),
		DontInclude("vest_event"),
		DontInclude("sandbox_clone"),
		DontInclude("sandbox"),
	}

	startDL := time.Now()
	res, _, err := Download(context.Background(), cli, "company", "id", company, exportOpts...)
	ok.NoError(err)
	ok.NotEmpty(res)
	download := time.Since(startDL)

	for _, row := range res {
		CleanupRow(row)
	}
	
	in, _ := json.Marshal(res)
	out := make([]map[string]any, 0, len(res))
	json.Unmarshal(in, &out)
	
	// t.Logf("full dump: %s", string(in))
	// for _, l := range debug {
	// 	t.Logf("debug: %s", l)
	// }

	fkm := NewForeignKeyMapper(cli)
	start := time.Now()
	ok.NoError(cli.Insert(fkm, out...))
	upload := time.Since(start)

	var newID int64
	switch any(cli).(type) {
	case pgbatchtx:
		ok.NoError(tx.QueryRow(context.Background(), "SELECT clone_id FROM datapasta_clone WHERE original_id = $1 AND table_name = 'company'", company).Scan(&newID))
	case pgtx:
		newID = int64(out[0]["id"].(int32))
	}

	t.Logf("new id: %d", newID)

	newRes, _, err := Download(context.Background(), cli, "company", "id", newID, exportOpts...)
	ok.NoError(err)
	ok.Len(newRes, len(res))


	t.Logf("durations: download(%s), upload(%s)", download, upload)
}


// postgres rows need some pulley-specific cleanup
func CleanupRow(obj map[string]any) {
	if obj[DumpTableKey] == "security" {
		obj["change_email_token"] = nil
	}
	if obj[DumpTableKey] == "task" {
		obj["access_code"] = nil
	}
	if obj[DumpTableKey] == "company" {
		obj["stripe_customer_id"] = nil
	}
	for col, raw := range obj {
		switch val := raw.(type) {
		case time.Time:
			if val.IsZero() || val.Year() <= 1 {
				obj[col] = time.Now() // there's a few invalid timestamps
			}
		}
	}
}
