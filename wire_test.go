package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

// Missing sql
// SSL not supported
// CANNOT DROP A TABLE
// CREATE SCHEMA IF NOT EXISTS sbom;
// PRIMARY KEY NEEDS TO HAVE SIZE IN VARCHAR
// cannot parse /, i.e. insert into person(name, identity, birthday)values ('Chloe','ZAA21','10/10/1980')

func TestWire(t *testing.T) {
	t.Run("POSTGRES", func(t *testing.T) {
		containerID := runPostgres(t)
		defer stopContainer(t, containerID)
		psqlInfo := "host=localhost port=5432  user=immudb password=immudb dbname=defaultdb sslmode=disable"
		db, err := sqlx.Open("postgres", psqlInfo)
		assert.NoError(t, err)
		defer db.Close()
		testSQL(t, db, false)
	})

	t.Run("IMMUDB", func(t *testing.T) {
		containerID := runImmudb(t)
		defer stopContainer(t, containerID)
		psqlInfo := "host=localhost port=5432  user=immudb password=immudb dbname=defaultdb sslmode=disable"
		db, err := sqlx.Open("postgres", psqlInfo)
		assert.NoError(t, err)
		defer db.Close()
		testSQL(t, db, true)
	})
}

func testSQL(t *testing.T, db *sqlx.DB, immuSyntax bool) {
	primaryIndexVarchar := func() string {
		if immuSyntax {
			return "VARCHAR[128]"
		}
		return "VARCHAR"
	}

	res, err := db.Exec(fmt.Sprintf(`
    CREATE TABLE IF NOT EXISTS packages 
    (
        id           %s   NOT NULL,
        name         VARCHAR   NOT NULL,
        version      VARCHAR   NOT NULL,
        kind         VARCHAR   NOT NULL,
        content_type VARCHAR   NOT NULL,
        purl         VARCHAR   NOT NULL,
        created      timestamp NOT NULL,
        tenant_uuid  %s   NOT NULL,
        is_base      BOOLEAN   NOT NULL,
    
        PRIMARY KEY (id, tenant_uuid)
    )`, primaryIndexVarchar(), primaryIndexVarchar()))

	assert.NoError(t, err)
	_, err = res.RowsAffected()
	assert.NoError(t, err)
	res, err = db.Exec(`
    INSERT INTO packages (id, name, version, kind, content_type, purl, created, tenant_uuid, is_base)
    VALUES ('1', 'one', '1.0.0', 'test', 'no type', 'purl://', NOW(), 'tenantOne', False )
        `)
	assert.NoError(t, err)
	affected, err := res.RowsAffected()
	assert.NoError(t, err)
	if !immuSyntax {
		assert.Equal(t, int64(1), affected)
	}

	if immuSyntax {
		// immudb pgwire does not support time ...
		res, err = db.Exec(`
        INSERT INTO packages (id, name, version, kind, content_type, purl, created, tenant_uuid, is_base)
        VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7, $8  )
        `,
			"2", "two", "2.0.0", "test", "no type", "purl://2", "tenantOne", true /*, time.Now()*/)
	} else {
		res, err = db.Exec(`
        INSERT INTO packages (id, name, version, kind, content_type, purl, created, tenant_uuid, is_base)
        VALUES ($1, $2, $3, $4, $5, $6, $9, $7, $8  )
        `,
			"2", "two", "2.0.0", "test", "no type", "purl://2", "tenantOne", true, time.Now())
	}

	assert.NoError(t, err)
	affected, err = res.RowsAffected()
	assert.NoError(t, err)
	if !immuSyntax {
		assert.Equal(t, int64(1), affected)
	}

	// TODO: add selects
	rows, err := db.Queryx(`
    SELECT id, name, version FROM packages WHERE id='1'`)
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	// Compared i.e. to jdbc, go pg driver is not asserting on column names when reading rows. It only asserts on column types and order. At lest when not using
	// named params
	fmt.Printf("COLUMNS: %+v\n", columns)
	for rows.Next() {
		var id, name, version string
		err := rows.Scan(&id, &name, &version)
		assert.NoError(t, err)
		assert.Equal(t, "1", id)
		assert.Equal(t, "one", name)
		assert.Equal(t, "1.0.0", version)
	}

	rows, err = db.Queryx(`
    SELECT id, name, version FROM packages WHERE id='1'`)
	assert.NoError(t, err)
	if immuSyntax {
		// immudb has bad column naming (any ORM will break on that ...)
		for rows.Next() {
			idNameVer := struct {
				ID      string `db:"(defaultdb.packages.id)"`
				Name    string `db:"(defaultdb.packages.name)"`
				Version string `db:"(defaultdb.packages.version)"`
			}{}
			err := rows.StructScan(&idNameVer)
			assert.NoError(t, err)
			assert.Equal(t, "1", idNameVer.ID)
			assert.Equal(t, "one", idNameVer.Name)
			assert.Equal(t, "1.0.0", idNameVer.Version)
		}
	} else {
		for rows.Next() {
			idNameVer := struct {
				ID      string
				Name    string
				Version string
			}{}
			err := rows.StructScan(&idNameVer)
			assert.NoError(t, err)
			assert.Equal(t, "1", idNameVer.ID)
			assert.Equal(t, "one", idNameVer.Name)
			assert.Equal(t, "1.0.0", idNameVer.Version)
		}
	}
}
