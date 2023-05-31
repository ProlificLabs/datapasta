package datapasta

import (
	"context"
	"encoding/json"
	"log"
	"strings"
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
	log.Println("starting to clone company", company)

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
		log.Println("cloning", row[DumpTableKey], row["id"])
	}

	in, _ := json.Marshal(res)
	out := make([]map[string]any, 0, len(res))
	json.Unmarshal(in, &out)

	fkm := NewForeignKeyMapper(cli)
	start := time.Now()

	log.Println("starting to insert company", company)
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

	log.Println("starting to download company", newID)
	newRes, deb, err := Download(context.Background(), cli, "company", "id", newID, exportOpts...)
	ok.NoError(err)

	for _, l := range deb {
		if !strings.HasSuffix(l, " 0 rows") {
			t.Logf("debug: %s ... %s", l[:20], l[len(l)-20:])
		}
	}

	for _, out := range newRes {
		if out[DumpTableKey] == "company" {
			t.Logf("found cloned company %v", out["id"])
		}
	}

	ok.Equalf(len(res), len(newRes), "expected clone to have the same size export")

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
