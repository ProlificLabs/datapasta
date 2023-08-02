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

const runPulleyTests = false

// This file houses tests that run against Pulley schema.
// TODO: have a docker database to run tests in a schema in datapasta.

const largeCompany = 2127511261
const testCompany = 1296515245

func TestWithLocalPostgres(t *testing.T) {
	if !runPulleyTests {
		t.Skipf("test is used for development against real pulley schema")
	}

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

	start := time.Now()

	log.Println("starting to insert company", company)
	ok.NoError(cli.Insert(out...))
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

func TestFullProcessWithRealDatabase(t *testing.T) {
	if !runPulleyTests {
		t.Skipf("test is used for development against real pulley schema")
	}

	company := testCompany // replace with actual company ID

	log.Println("starting to clone company", company)

	ok := assert.New(t)
	conn, err := pgxpool.Connect(context.Background(), `postgresql://postgres:postgres@localhost:5432/postgres`)
	ok.NoError(err)

	db, err := NewPostgres(context.Background(), conn)
	ok.NoError(err)

	// Begin a transaction specifically for cloning and reverse mapping
	tx, err := conn.BeginTx(context.Background(), pgx.TxOptions{})
	ok.NoError(err)
	defer tx.Rollback(context.Background())

	exportOpts := []Opt{
		DontInclude("user"),
		DontInclude("firm_company"),
		DontRecurse("stakeholder"),
		DontInclude("sandbox_clone"),
		DontInclude("sandbox"),
	}

	// Downloading data for initial company
	connCli1, err := db.NewBatchClient(context.Background(), conn) // New client for this download
	ok.NoError(err)
	initial, _, err := Download(context.Background(), connCli1, "company", "id", company, exportOpts...)
	ok.NoError(err)
	ok.NotEmpty(initial)

	for _, row := range initial {
		CleanupRow(row)
	}

	cli, err := db.NewBatchClient(context.Background(), tx) // This client is used for cloning and reverse mapping
	in, _ := json.Marshal(initial)
	{
		out := make([]map[string]any, 0, len(initial))
		json.Unmarshal(in, &out)

		log.Println("starting to insert company", company)
		ok.NoError(err)
		ok.NoError(cli.Insert(out...)) // Insert cloned company
	}
	mapping, err := cli.Mapping() // Acquire the mapping between old and new Ids
	ok.NoError(err)
	cloneID := mapping[0].PrimaryKey

	for _, m := range mapping {
		log.Printf("mapped %s (%T %v to %T %v)", m.Table, m.OriginalID, m.OriginalID, m.PrimaryKey, m.PrimaryKey)
	}

	// we can mutate company and clone here to generate merge actions
	deleteStuff := `DELETE FROM "security" WHERE id IN (SELECT id FROM "security" WHERE company_id=$1 LIMIT 1)`
	_, deleteErr := tx.Exec(context.Background(), deleteStuff, cloneID)
	ok.NoError(deleteErr)
	changeStuff := `UPDATE "security" SET issue_date=NOW() WHERE id IN (SELECT id FROM "security" WHERE company_id=$1 LIMIT 1)`
	_, changeErr := tx.Exec(context.Background(), changeStuff, cloneID)
	ok.NoError(changeErr)
	addStuff := `INSERT INTO "security" (company_id) VALUES ($1)`
	_, addErr := tx.Exec(context.Background(), addStuff, cloneID)
	ok.NoError(addErr)

	// here's the juice: given the "initial" snapshot, we can export both the main and sandbox dumps
	// then use them to generate and apply a 3-way merge changeset

	// Re-download both initial and cloned company
	connCli2, err := db.NewBatchClient(context.Background(), conn) // New client for this download
	ok.NoError(err)
	currentCompany, _, err := Download(context.Background(), connCli2, "company", "id", company, exportOpts...)
	ok.NoError(err)
	ok.NotEmpty(currentCompany)

	// cleanup the current company so that the prior cleanup doesnt count as conflicts
	for _, row := range currentCompany {
		CleanupRow(row)
	}

	// get the clone using the tx
	connCli3, err := db.NewBatchClient(context.Background(), tx) // New client for this download
	ok.NoError(err)
	clonedCompany, _, err := Download(context.Background(), connCli3, "company", "id", cloneID, exportOpts...)
	ok.NoError(err)
	ok.NotEmpty(clonedCompany)

	t.Logf(`before mapping: %#v`, clonedCompany[0])

	ReverseForeignKeyMapping(db.ForeignKeys(), mapping, clonedCompany)
	ReversePrimaryKeyMapping(db.PrimaryKeys(), mapping, clonedCompany)

	mas := GenerateMergeStrategy(db.PrimaryKeys(), initial, currentCompany, clonedCompany)

	actions := map[string]bool{}
	for _, ma := range mas {
		actions[ma.Action] = true
		t.Log("merge action:", ma)
	}
	ok.Equal(map[string]bool{"create": true, "delete": true, "update": true}, actions)

	mergeErr := ApplyMergeStrategy(connCli3, mapping, mas)
	ok.NoError(mergeErr)
}
