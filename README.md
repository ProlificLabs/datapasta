# sqlclone

The library should deeply clone an object from a database into another database. The database schema provides everything necessary to clone, and it is guaranteed the schema is identical in both the source and target database.

This library is a great candidate for a useful open source library and should be designed as such, with tests and documentation. Only Postgres support is required for now.

### Discovery Step (Download)

First, we need to discover every record that needs to be cloned. This is a recursive search - we start with an ID and a table and visit every table that has a foreign key reference to this record. It is recommended that the function that figures out the graph of the database schema be a standalone function, as it will likely be used during upload as well.

There is some configuration required here - some tables should not be recursed further, and we need to configure the records from which we start recursive searches.

There must be an intermediate representation of the downloaded data. This is because the script may not have access to the target database, and need to serialize the clone for execution in another environment.

Finally, care must be taken to avoid infinite recursion and double visiting - if two records have foreign keys to the same object, that object should still only be present in the dump once.

### Cloning Step (Upload)

Next, we insert every record into the second database. There is some nuance to this step:

- The new ID’s should be recorded in memory with a mapping, such that when we insert a record, we store a mapping from the new ID and table name to the old ID. In other words, when we insert a record without an ID, the database will return to us a new ID, which we must store in the mapping.
- Foreign keys must be updated using that mapping. When a column previously pointed to a given ID, we need to swap it to point to the ID of the cloned record we created for it.
- Order remains important - we should clone records in the same order we initially visited them, to ensure foreign keys point to existing cloned records while inserting. This can be achieved by scanning the schema again, similar to how it was done for downloading.
- Even default columns like “created_at” should still use the value from the original dump.

The mapping must be returned to the caller of this library after the upload has taken place.

### How it would be used

Here is an example of what the libraries export signatures could look like:

```go
package sqlclone

type DatabaseDump map[string]map[string]any
type Mapping map[string]map[string]string

func DontRecurse( table string ) downloadOpt
func Include( table, id string ) downloadOpt

func Download( connectionParams, ... downloadOpt ) ( DatabaseDump, error )
func Upload( connectionParams, DatabaseDump ) ( Mapping, error )
```

Here is some Go code that uses the above library to clone a “company” with all of its “clients”.

```go
// download company 10, not recursing into the user table
breakpoint := sqlclone.DontRecurse( "user" )
startWith := sqlclone.Include( "company", "10" )
data, err := sqlclone.Download( connectionParameters, startWith, breakpoint )

// the data can be manipulated by the caller (for example to anonymize PII)
for _, i := range data["client"] {
	data["client"][i]["ssn"] = random.SSN()
}

// data can be transported as JSON
dataBytes, err := json.Marshal( data )

newData := make(sqlclone.DatabaseDump)
err := json.Unmarshal( dataBytes, &newData )

// upload the data to the new database
mapping, err := sqlclone.Upload( connectionParameters, newData )

// 'mapping' now lets us look up the old ID's from the new ones
map[string]map[string]string{
	"client": map[string]string{
		"1": "24",
		"2": "46",
		"3": "63",
	},
}
```

### Example Schema

Suppose the following 3-table schema (clients have a company and a login).

```sql
CREATE TABLE company ( 
  id text PRIMARY KEY 
);

CREATE TABLE login ( 
  id text PRIMARY KEY
);

CREATE TABLE client ( 
  id text PRIMARY KEY, 
  address TEXT,
  company_id TEXT REFERENCES company( id ),
  login_id TEXT REFERENCES login( id ),
  referred_by TEXT REFERENCES client( id )
);
```

We can insert some sample data:

```sql
INSERT INTO company( id ) 
  VALUES ( 'Amazil' ), ( 'RandomCompany' );

INSERT INTO login ( id )
  VALUES ( 'jeff@amazil.com' ), ( 'bob@amazil.com' ), ( 'random@company.com' );
  
INSERT INTO client( id, address, company_id, login_id, referred_by ) 
  VALUES ( 'Jeff', '123 Fake St', 'Amazil', 'jeff@amazil.com', NULL );
INSERT INTO client( id, address, company_id, login_id, referred_by ) 
  VALUES ( 'Bob', '124 Fake Ave', 'Amazil', 'bob@amazil.com', 'Jeff' );
INSERT INTO client( id, address, company_id, login_id, referred_by ) 
  VALUES ( 'Fred', '125 Fake St', 'Amazil', NULL, 'Bob' );
INSERT INTO client( id, address, company_id, login_id, referred_by ) 
  VALUES ( 'Jim', '234 Lucky St', 'RandomCompany', 'random@company.com', NULL );
```

When we **download** ‘Amazil’, here’s what should happen:

1. Store the columns of the Amazil company
2. Find that clients have a foreign key to company
3. Store the columns of clients that reference Amazil
4. Find that clients reference clients
5. Store the columns of clients we haven’t yet stored
6. Find that login references clients
7. Store the columns of logins that reference clients

Notably, Jim should not be included as he does not reference Amazil in any way

When we ************upload************ the columns we have stored:

1. Insert Amazil, getting a new ID from the database (Amazil2)
2. All the clients that reference Amazil are updated to Amazil2
3. Insert the logins to get new login ID’s
4. Update the clients in the dump with null references to other clients
5. Update the clients in the dump to the new login IDs
6. Update the clients in the dump with the new company ID, Amazil2
7. Map the old client ID’s to the new ones just created (Jeff2, Bob2, Fred2)
8. Update the clients with references to the new clients
9. Return the mapping of Amazil2 to Amazil, Jeff2 to Jeff, etc.

This order is important - suppose 2 clients reference each other. To clone these, we will need to initially insert them with null foreign keys and update them afterwards to point to each other.

### Notes

- Performance isn’t a concern. For example, when uploading, we could optimize the order to upload batch inserts for a whole table at once rather than revisiting it over and over, but performance is not a concern for this library.
- ID’s should be stored and manipulated as strings - however, the ID column may be an integer, so converting to strings for mapping purposes would be required.
- When testing, remember that ID’s should be auto-generated. Feel free to use SERIAL or UUID, but only integers are required to be supported for our needs.
- All postgres datatypes should be supported, especially including JSON types.
