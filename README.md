## DataPasta 🍝

Export and import heirarchal objects in a database.

[![golangci-lint](https://github.com/ProlificLabs/datapasta/actions/workflows/golangci-lint.yml/badge.svg?branch=main)](https://github.com/ProlificLabs/datapasta/actions/workflows/golangci-lint.yml)

___

### Summary

This library provides deep-clone functionality for a database object. It currently has an adapter for Postgres, but custom adapters can be created to satisfy a minimal interface.

### Mechanism

There are 2 steps:

- **Download**: this recursive process fetches a record, recurses into all the records that have a foreign key reference to it and appends them to an output, then appends this record, and finally recurses into any records this record has a foreign key reference to.
- **Upload**: this naive process loops through a slice of objects, inserting each object to its given table. While doing so, however, it keeps track of changes (such as newly generated primary keys), and updates references to those changes in the following records.

These 2 mechanisms allow for easily downloading an export of a hierarchial structure from a database, and then uploading that export to either the same database or a new database.

### Example

`main.go`
```go
// c is a connection to a postgres database
pg, err := datapasta.NewPostgres(ctx, c)
assert.NoError(err)
```

`export.go`
```go
// we want to export everything about user 50
cli, err := pg.NewClient(ctx, c)
assert.NoError(err)

// download user id 50 - it will recursively find everything related to the user
dl, trace, err := datapasta.Download(ctx, cli, "user", "id", 50)
assert.NoError(err)
```
`import.go`
```go
// now upload a copy of that user
cli, err := pg.NewClient(ctx, db)
assert.NoError(err)

datapasta.Upload(ctx, cli, dump)

// return the new id of the user (as postgres provided a new id)
return dump[0]["id"].(int32), nil
```

### Export Tips

Download accepts a few options, which you will *definitely* want to provide. The most important option is `DontRecurse`, which tells the clone to include *but not recurse into* a table. For example, consider:

```sql
user ( id serial )
item ( id serial )
purchase ( 
    user_id REFERENCES user(id), 
    item_id REFERENCES item(id),
)
```

If we export a `user`, the export will recurse into `purchase`, and then recurse into other `user` records that have made purchases, which will likely clone your entire database!

This can be solved by telling Download not to recurse out of the `purchase` table, with `datapasta.DontRecurse("purchase")`.

This can also be solved by telling Download not to include the `user` table at all, with `datapasta.DontInclude("purchase")`.

### Import Tips

There's a very good chance that the resulting export won't be importable without some cleaning up, for a few reasons.

- Some unique columns that *aren't* primary keys will need to be nulled or mocked.
- If you're exporting to a different database, any records excluded from the dump may still be referenced by a foreign key, which will need to be nulled.
- You might want to strip PII.

Luckily, as the dump is just an array of arbitrary objects, it's pretty easy to clean up the dump between import and export. Here's an example that removes "access codes" from users as those have a unique constraint:

```go
for _, obj := range dump {
    if obj[datapasta.DumpTableKey] == "user" {
		obj["access_code"] = nil
	}
}
```