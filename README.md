# sqlclone

The library deeply clones records from a database into another database. The database schema should be identical in both the source and target database.

### Discovery Step (Download)

First, we discover every record that needs to be cloned. This is a recursive search - the search starts with a record and collects all records that these records reference via foreign keys (backward search) AND visits every table that has a foreign key reference to this record (forward search). 

This step can be configured - we can specify multiple starting points for the backward search and we can exlude tables from the forward search.


### Cloning Step (Upload)

This steps inserts the collected records into the target database. The new IDs and their corresponding old IDs are recorded in memory with a mapping. The mapping is returned to the caller after the upload has taken place. 

### How it would be used

Here is some Go code that uses the above library with the example schema below.

```go
// download company with id = 3, not recursing the tables person and company forward
download_options, _ := sqlclone.NewDownloadOptions(
  sqlclone.Include("company", "id", "3"),
  sqlclone.DontRecurse("person"),
  sqlclone.DontRecurse("company"),
)
fromDB, _ := sqlclone.NewPostgresAdapter("yourHost", yourPort, "yourUsername", "yourPassword", "yourSourceDatabase")

data, _ := sqlclone.Download( fromDB, download_options ) // retrieves all records related to the starting points

// the data can be manipulated by the caller (for example to anonymize personal information)
for _, i := range data["person"] {
	data["person"][i]["ssn"] = random.SSN()
}

dataBytes, err := json.Marshal( data ) // data can be transported as JSON

newData := make(sqlclone.DatabaseDump)
_ := json.Unmarshal( dataBytes, &newData )

toDB, _ := sqlclone.NewPostgresAdapter("yourHost", yourPort, "yourUsername", "yourPassword", "yourTargetDatabase")

mapping, _ := sqlclone.Upload( toDB, newData ) // uploads data to the target database

// 'mapping' now lets us look up the old ID's from the new ones
map[string]map[string]string{
	"person": {
		"1": "24",
		"2": "46",
		"3": "63",
	},
}
```

### Example Schema

Suppose the following 3-table schema.

```sql
CREATE TABLE company ( 
  id INT PRIMARY KEY 
  legal_name TEXT
  parent_company_id INT REFERENCES company(id)
);

CREATE TABLE person ( 
  id INT PRIMARY KEY
  legal_name text
);

CREATE TABLE person_company ( 
  company_id INT REFERENCES company( id ),
  person INT REFERENCES person( id ),
  permissions TEXT
);
```
