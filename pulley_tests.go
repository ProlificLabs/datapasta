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

const runPulleyTests = true

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

// func TestDiffTwoCompaniesWithLocalPostgres(t *testing.T) {
// 	if !runPulleyTests {
// 		t.Skipf("test is used for development against real pulley schema")
// 	}

// 	// declare and initialize both company ids
// 	company1 := largeCompany // To be replaced with actual ID
// 	company2 := testCompany  // To be replaced with actual ID

// 	ok := assert.New(t)
// 	conn, err := pgxpool.Connect(context.Background(), `postgresql://postgres:postgres@localhost:5432/postgres`)
// 	ok.NoError(err)
// 	db, err := NewPostgres(context.Background(), conn)
// 	ok.NoError(err)

// 	cli, err := db.NewBatchClient(context.Background(), conn)
// 	ok.NoError(err)

// 	exportOpts := []Opt{
// 		DontInclude("user"),
// 		DontInclude("firm"),
// 		DontRecurse("stakeholder"),
// 		DontInclude("sandbox_clone"),
// 		DontInclude("sandbox"),
// 	}

// 	// Downloading data for company1
// 	res1, _, err := Download(context.Background(), cli, "company", "id", company1, exportOpts...)
// 	ok.NoError(err)

// 	// Downloading data for company2
// 	res2, _, err := Download(context.Background(), cli, "company", "id", company2, exportOpts...)
// 	ok.NoError(err)

// 	// Perform diff between the two companies
// 	nonConflicting, conflicting := ThreeWayDiff(cli, res1, res1, res2)

// 	// Number of conflicting and non-conflicting differences should be printed or used in assert
// 	t.Logf("Number of conflicting diffs: %d", len(conflicting))
// 	t.Logf("Number of nonconflicting diffs: %d", len(nonConflicting))
// }

// func TestFullProcessWithRealDatabase(t *testing.T) {
// 	if !runPulleyTests {
// 		t.Skipf("test is used for development against real pulley schema")
// 	}

// 	company := testCompany // replace with actual company ID

// 	log.Println("starting to clone company", company)

// 	ok := assert.New(t)
// 	conn, err := pgxpool.Connect(context.Background(), `postgresql://postgres:postgres@localhost:5432/postgres`)
// 	ok.NoError(err)

// 	db, err := NewPostgres(context.Background(), conn)
// 	ok.NoError(err)

// 	// Begin a transaction specifically for cloning and reverse mapping
// 	tx, err := conn.BeginTx(context.Background(), pgx.TxOptions{})
// 	ok.NoError(err)
// 	defer tx.Rollback(context.Background())

// 	cli, err := db.NewBatchClient(context.Background(), tx) // This client is used for cloning and reverse mapping
// 	ok.NoError(err)

// 	exportOpts := []Opt{
// 		DontInclude("user"),
// 		DontInclude("firm"),
// 		DontRecurse("stakeholder"),
// 		DontInclude("sandbox_clone"),
// 		DontInclude("sandbox"),
// 	}

// 	// Downloading data for initial company
// 	connCli1, err := db.NewBatchClient(context.Background(), conn) // New client for this download
// 	ok.NoError(err)
// 	initial, _, err := Download(context.Background(), connCli1, "company", "id", company, exportOpts...)
// 	ok.NoError(err)

// 	for _, row := range initial {
// 		CleanupRow(row)
// 	}

// 	// Copy 'initial' to 'out' to clone the initial company
// 	in, _ := json.Marshal(initial)
// 	out := make([]map[string]any, 0, len(initial))
// 	json.Unmarshal(in, &out)

// 	log.Println("starting to insert company", company)
// 	ok.NoError(cli.Insert(out...)) // Insert cloned company

// 	mapping, err := cli.GetMapping() // Acquire the mapping between old and new Ids
// 	ok.NoError(err)

// 	for _, m := range mapping {
// 		log.Println("mapped", m.TableName, m.OriginalID, "to", m.NewID)
// 	}

// 	// Re-download both initial and cloned company
// 	connCli2, err := db.NewBatchClient(context.Background(), conn) // New client for this download
// 	ok.NoError(err)
// 	initialClone, _, err := Download(context.Background(), connCli2, "company", "id", company, exportOpts...)
// 	ok.NoError(err)
// 	ok.NotEmpty(initialClone)

// 	connCli3, err := db.NewBatchClient(context.Background(), conn) // New client for this download
// 	ok.NoError(err)
// 	clonedCompany, _, err := Download(context.Background(), connCli3, "company", "id", mapping[0].NewID, exportOpts...)
// 	ok.NoError(err)
// 	ok.NotEmpty(clonedCompany)

// 	// Reverse map the downloaded clone
// 	ok.NoError(ReverseMapping(cli, clonedCompany, mapping))

// 	log.Println("sample initial clone", initialClone[:5])
// 	log.Println("sample reverse mapped clone ", clonedCompany[:5])

// 	newDiff := FindDiffs(cli, initialClone, clonedCompany) // Perform diff between initial and cloned data after reverse mapping

// 	ok.Empty(newDiff, "Expect no diffs, but got some") // Ensure there is no diff
// }
