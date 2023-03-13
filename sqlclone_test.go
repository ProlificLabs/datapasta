package sqlclone

import (
	"database/sql"
	"reflect"
	"testing"
)

type mockDB struct {
	getTablesReturnValue          []string
	getDependencyOrderReturnValue []string
}

func (m *mockDB) newTransaction() (*sql.Tx, error) {
	return nil, nil
}

// interface methods
func (m *mockDB) getTables() ([]string, error) {
	return m.getTablesReturnValue, nil
}

func (m *mockDB) getReferences() (References, error) {
	myMap := make(map[string][]TableReference, 0)
	myMap["company"] = append(myMap["company"], *NewTableReference("company", "parent_company_id", "company", "id"))
	myMap["person_company"] = append(myMap["person_company"], *NewTableReference("person_company", "person_id", "person", "id"))
	myMap["person_company"] = append(myMap["person_company"], *NewTableReference("person_company", "company_id", "company", "id"))
	myMap["purchase"] = append(myMap["purchase"], *NewTableReference("purchase", "person_id", "person", "id"))
	myMap["purchase"] = append(myMap["purchase"], *NewTableReference("purchase", "company_id", "company", "id"))
	return myMap, nil
}

func (m *mockDB) getPrimaryKeys() (map[string][]string, error) {
	pk := make(map[string][]string, 0)
	pk["person"] = append(pk["person"], "id")
	pk["company"] = append(pk["company"], "id")
	return pk, nil
}

func (m *mockDB) getDependencyOrder() ([]string, error) {
	return m.getDependencyOrderReturnValue, nil
}

func (m *mockDB) getRows(table string, column string, value string) ([]map[string]string, error) {
	result := make([]map[string]string, 0)
	//	fmt.Println("call with " + table + " " + column + " " + fmt.Sprintf("%v", value))

	if (table == "person" && column == "id" && value == "1") ||
		(table == "person" && column == "legal_name" && value == "Fred") {
		result = append(result, map[string]string{"id": "1", "legal_name": "Fred"})
	}
	if (table == "person" && column == "id" && value == "2") ||
		(table == "person" && column == "legal_name" && value == "Bob") {
		result = append(result, map[string]string{"id": "2", "legal_name": "Bob"})
	}
	if (table == "person" && column == "id" && value == "3") ||
		(table == "person" && column == "legal_name" && value == "Alice") {
		result = append(result, map[string]string{"id": "3", "legal_name": "Alice"})
	}
	if (table == "person" && column == "id" && value == "4") ||
		(table == "person" && column == "legal_name" && value == "Eve") {
		result = append(result, map[string]string{"legal_name": "Eve", "id": "4"})
	}

	if (table == "company" && column == "id" && value == "1") ||
		(table == "company" && column == "legal_name" && value == "Meta") {
		result = append(result, map[string]string{"id": "1", "legal_name": "Meta", "parent_company_id": "<nil>"})
	}
	if (table == "company" && column == "id" && value == "2") ||
		(table == "company" && column == "legal_name" && value == "Alphabet") {
		result = append(result, map[string]string{"id": "2", "legal_name": "Alphabet", "parent_company_id": "<nil>"})
	}
	if (table == "company" && column == "id" && value == "3") ||
		(table == "company" && column == "legal_name" && value == "Google") ||
		(table == "company" && column == "parent_company_id" && value == "2") {
		result = append(result, map[string]string{"id": "3", "legal_name": "Google", "parent_company_id": "2"})
	}
	if (table == "company" && column == "id" && value == "4") ||
		(table == "company" && column == "legal_name" && value == "Facebook") ||
		(table == "company" && column == "parent_company_id" && value == "1") {
		result = append(result, map[string]string{"id": "4", "legal_name": "Facebook", "parent_company_id": "1"})
	}
	if (table == "company" && column == "id" && value == "5") ||
		(table == "company" && column == "legal_name" && value == "YouTube") ||
		(table == "company" && column == "parent_company_id" && value == "2") {
		result = append(result, map[string]string{"id": "5", "legal_name": "YouTube", "parent_company_id": "2"})
	}

	if table == "company" && column == "parent_company_id" && value == "<nil>" {
		result = append(result, map[string]string{"id": "1", "legal_name": "Meta", "parent_company_id": "<nil>"})
		result = append(result, map[string]string{"id": "2", "legal_name": "Alphabet", "parent_company_id": "<nil>"})
	}

	if table == "person_company" && column == "person_id" && value == "1" {
		result = append(result, map[string]string{"person_id": "1", "company_id": "1", "permissions": `{"admin":true}`})
	}
	if table == "person_company" && column == "company_id" && value == "1" {
		result = append(result, map[string]string{"person_id": "1", "company_id": "1", "permissions": `{"admin":true}`})
	}
	if table == "person_company" && column == "person_id" && value == "2" {
		result = append(result, map[string]string{"person_id": "2", "company_id": "3", "permissions": `{"admin":false}`})
	}
	if table == "person_company" && column == "company_id" && value == "3" {
		result = append(result, map[string]string{"person_id": "2", "company_id": "3", "permissions": `{"admin":false}`})
	}

	if table == "purchase" && column == "person_id" && value == "2" {
		result = append(result, map[string]string{"payment_token": `9cf973a1-63e1-4967-855e-87bdccf0a6f7`, "price_paid": "145.40203494", "person_id": "2", "company_id": "4"})
	}
	if table == "purchase" && column == "company_id" && value == "4" {
		result = append(result, map[string]string{"payment_token": `9cf973a1-63e1-4967-855e-87bdccf0a6f7`, "price_paid": "145.40203494", "person_id": "2", "company_id": "4"})
	}
	if table == "purchase" && column == "person_id" && value == "3" {
		result = append(result, map[string]string{"payment_token": `40c56909-6df9-45f9-adf9-d6b35093566f`, "price_paid": "57.3125", "person_id": "3", "company_id": "2"})
	}
	if table == "purchase" && column == "company_id" && value == "2" {
		result = append(result, map[string]string{"payment_token": `40c56909-6df9-45f9-adf9-d6b35093566f`, "price_paid": "57.3125", "person_id": "3", "company_id": "2"})
	}

	return result, nil
}

var start_index = 10 // global variable to simulate different target ids generated in the target database
func (m *mockDB) insertRow(tx *sql.Tx, table_name string, columns []string, values []string, auto_value string) (int, error) {
	index := -1
	if auto_value != "" {
		index = start_index
		start_index++
	}
	return index, nil
}

func TestDownload(t *testing.T) {
	// mock database
	mockdb := mockDB{
		getTablesReturnValue:          []string{"purchase", "person", "person_company", "company"},
		getDependencyOrderReturnValue: []string{"person", "company", "purchase", "person_company"},
	}

	var counter = 1
	// --------------
	// test case 1: starting point that is not referencing to any other entry and is not referenced by any other entry
	download_options, _ := NewDownloadOptions(
		Include("person", "legal_name", "Eve"),
		DontRecurse("user"),
	)
	result, _ := Download(&mockdb, download_options)
	expected_result := DatabaseDump{"person": {{"id": "4", "legal_name": "Eve"}}}
	if !compareDumps(result, expected_result) {
		t.Errorf("TestDownload() returned unexpected result for TestCase_%d: \n expected result: %v \n returned result: %v", counter, expected_result, result)
	}
	counter++

	// --------------
	// test case 2: starting point that is from a self-referencing table and dont_recurse list contains the table of origin
	download_options, _ = NewDownloadOptions(
		Include("company", "legal_name", "YouTube"),
		DontRecurse("person"),
		DontRecurse("company"),
		DontRecurse("person_company"),
		DontRecurse("purchase"),
	)
	result, _ = Download(&mockdb, download_options)
	expected_result = DatabaseDump{"company": {{"id": "5", "legal_name": "YouTube", "parent_company_id": "2"}, {"id": "2", "legal_name": "Alphabet", "parent_company_id": "<nil>"}}}

	if !compareDumps(result, expected_result) {
		t.Errorf("TestDownload() returned unexpected result for TestCase_%d: \n expected result: %v \n returned result: %v", counter, expected_result, result)
	}
	counter++

	// --------------
	// test case 3: starting point that is not referencing to any other entry and is not referenced by any other entry
	// and dont_recurse list contains the table of origin
	download_options, _ = NewDownloadOptions(
		Include("person", "legal_name", "Eve"),
		DontRecurse("person"),
	)
	result, _ = Download(&mockdb, download_options)
	expected_result = DatabaseDump{"person": {{"id": "4", "legal_name": "Eve"}}}

	if !compareDumps(result, expected_result) {
		t.Errorf("TestDownload() returned unexpected result for TestCase_%d: \n expected result: %v \n returned result: %v", counter, expected_result, result)
	}
	counter++

	// --------------
	// test case 4: two starting points
	download_options, _ = NewDownloadOptions(
		Include("person", "legal_name", "Eve"),
		Include("person", "id", "3"),
		DontRecurse("user"),
	)
	result, _ = Download(&mockdb, download_options)
	expected_result = DatabaseDump{
		"person":         {{"id": "1", "legal_name": "Fred"}, {"id": "2", "legal_name": "Bob"}, {"id": "3", "legal_name": "Alice"}, {"id": "4", "legal_name": "Eve"}},
		"company":        {{"id": "1", "legal_name": "Meta", "parent_company_id": "<nil>"}, {"id": "2", "legal_name": "Alphabet", "parent_company_id": "<nil>"}, {"id": "3", "legal_name": "Google", "parent_company_id": "2"}, {"id": "4", "legal_name": "Facebook", "parent_company_id": "1"}, {"id": "5", "legal_name": "YouTube", "parent_company_id": "2"}},
		"person_company": {{"person_id": "1", "company_id": "1", "permissions": `{"admin":true}`}, {"person_id": "2", "company_id": "3", "permissions": `{"admin":false}`}},
		"purchase":       {{"payment_token": `9cf973a1-63e1-4967-855e-87bdccf0a6f7`, "price_paid": "145.40203494", "person_id": "2", "company_id": "4"}, {"payment_token": `40c56909-6df9-45f9-adf9-d6b35093566f`, "price_paid": "57.3125", "person_id": "3", "company_id": "2"}}}

	if !compareDumps(result, expected_result) {
		t.Errorf("TestDownload() returned unexpected result for TestCase_%d: \n expected result: %v \n returned result: %v", counter, expected_result, result)
	}
	counter++

	// --------------
	// test case 5: two starting points and two dont_recurse tables
	download_options, _ = NewDownloadOptions(
		Include("person", "legal_name", "Eve"),
		Include("company", "id", "4"),
		DontRecurse("person_company"),
		DontRecurse("purchase"),
	)
	result, _ = Download(&mockdb, download_options)
	expected_result = DatabaseDump{
		"person":  {{"id": "4", "legal_name": "Eve"}},
		"company": {{"id": "1", "legal_name": "Meta", "parent_company_id": "<nil>"}, {"id": "4", "legal_name": "Facebook", "parent_company_id": "1"}},
	}

	if !compareDumps(result, expected_result) {
		t.Errorf("TestDownload() returned unexpected result for TestCase_%d: \n expected result: %v \n returned result: %v", counter, expected_result, result)
	}
	counter++

	// --------------
	// test case 6: starting point with nil value
	download_options, _ = NewDownloadOptions(
		Include("company", "parent_company_id", "<nil>"),
		DontRecurse("person_company"),
		DontRecurse("purchase"),
	)
	result, _ = Download(&mockdb, download_options)
	expected_result = DatabaseDump{
		"company": {{"id": "1", "legal_name": "Meta", "parent_company_id": "<nil>"}, {"id": "2", "legal_name": "Alphabet", "parent_company_id": "<nil>"}},
	}

	if !compareDumps(result, expected_result) {
		t.Errorf("TestDownload() returned unexpected result for TestCase_%d: \n expected result: %v \n returned result: %v", counter, expected_result, result)
	}
	counter++
}

func TestUpload(t *testing.T) {
	// mock database
	mockdb := mockDB{
		getTablesReturnValue:          []string{"purchase", "person", "person_company", "company"},
		getDependencyOrderReturnValue: []string{"person", "company", "purchase", "person_company"},
	}

	var counter = 1
	// --------------
	// test case 1: one entry into person table with autovalue id
	data := DatabaseDump{"person": {{"id": "4", "legal_name": "Eve"}}}
	result, _ := upload(nil, &mockdb, data)

	expected_result := Mapping{"person": {"4": "10"}}
	if !reflect.DeepEqual(result, expected_result) {
		t.Errorf("TestUpload() returned unexpected result for TestCase_%d: \n expected result: %v \n returned result: %v", counter, expected_result, result)
	}
	counter++

	// --------------
	// test case 2: clone all entries in the source table
	data = DatabaseDump{
		"person":         {{"id": "1", "legal_name": "Fred"}, {"id": "2", "legal_name": "Bob"}, {"id": "3", "legal_name": "Alice"}, {"id": "4", "legal_name": "Eve"}},
		"company":        {{"id": "1", "legal_name": "Meta", "parent_company_id": "<nil>"}, {"id": "2", "legal_name": "Alphabet", "parent_company_id": "<nil>"}, {"id": "3", "legal_name": "Google", "parent_company_id": "2"}, {"id": "4", "legal_name": "Facebook", "parent_company_id": "1"}, {"id": "5", "legal_name": "YouTube", "parent_company_id": "2"}},
		"person_company": {{"person_id": "1", "company_id": "1", "permissions": `{"admin":true}`}, {"person_id": "2", "company_id": "3", "permissions": `{"admin":false}`}},
		"purchase":       {{"payment_token": `9cf973a1-63e1-4967-855e-87bdccf0a6f7`, "price_paid": "145.40203494", "person_id": "2", "company_id": "4"}, {"payment_token": `40c56909-6df9-45f9-adf9-d6b35093566f`, "price_paid": "57.3125", "person_id": "3", "company_id": "2"}}}

	result, _ = upload(nil, &mockdb, data)

	expected_result = Mapping{
		"person":  {"1": "11", "2": "12", "3": "13", "4": "14"},
		"company": {"1": "15", "2": "16", "3": "17", "4": "18", "5": "19"},
	}
	if !reflect.DeepEqual(result, expected_result) {
		t.Errorf("TestUpload() returned unexpected result for TestCase_%d: \n expected result: %v \n returned result: %v", counter, expected_result, result)
	}
	counter++

	// --------------
	// test case 3: clone all entries in the source table with different ordering. especially the company table is of importance
	//   as it is self-referencing
	data = DatabaseDump{
		"person":         {{"id": "1", "legal_name": "Fred"}, {"id": "2", "legal_name": "Bob"}, {"id": "3", "legal_name": "Alice"}, {"id": "4", "legal_name": "Eve"}},
		"company":        {{"id": "2", "legal_name": "Alphabet", "parent_company_id": "<nil>"}, {"id": "5", "legal_name": "YouTube", "parent_company_id": "2"}, {"id": "3", "legal_name": "Google", "parent_company_id": "2"}, {"id": "1", "legal_name": "Meta", "parent_company_id": "<nil>"}, {"id": "4", "legal_name": "Facebook", "parent_company_id": "1"}},
		"person_company": {{"person_id": "1", "company_id": "1", "permissions": `{"admin":true}`}, {"person_id": "2", "company_id": "3", "permissions": `{"admin":false}`}},
		"purchase":       {{"payment_token": `9cf973a1-63e1-4967-855e-87bdccf0a6f7`, "price_paid": "145.40203494", "person_id": "2", "company_id": "4"}, {"payment_token": `40c56909-6df9-45f9-adf9-d6b35093566f`, "price_paid": "57.3125", "person_id": "3", "company_id": "2"}}}

	result, _ = upload(nil, &mockdb, data)

	expected_result = Mapping{
		"person":  {"1": "20", "2": "21", "3": "22", "4": "23"},
		"company": {"1": "24", "2": "25", "3": "26", "4": "27", "5": "28"},
	}
	if !reflect.DeepEqual(result, expected_result) {
		t.Errorf("TestUpload() returned unexpected result for TestCase_%d: \n expected result: %v \n returned result: %v", counter, expected_result, result)
	}
	counter++
}

// type DatabaseDump map[string][]map[string]string
func compareDumps(d1 DatabaseDump, d2 DatabaseDump) bool {
	if len(d1) != len(d2) {
		return false
	}

	for k1, v1 := range d1 {
		v2, ok := d2[k1]
		if !ok {
			return false
		}

		if len(v1) != len(v2) {
			return false
		}

		for _, m1 := range v1 {
			if !contains(v2, m1) {
				return false
			}
		}
	}
	return true
}

func contains(s []map[string]string, m map[string]string) bool {
	for _, v := range s {
		if reflect.DeepEqual(m, v) {
			return true
		}
	}
	return false
}
