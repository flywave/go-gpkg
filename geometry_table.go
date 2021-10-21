package gpkg

import (
	"fmt"
	"strings"

	"github.com/flywave/go-geom"
)

type column struct {
	cid       int
	name      string
	ctype     string
	notnull   int
	dfltValue *int
	pk        int
}

type table struct {
	name    string
	columns []column
	gcolumn string
	gtype   string
	srs     int
}

func (t table) createSQL() string {
	create := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%v"`, t.name)
	var columnparts []string
	for _, column := range t.columns {
		columnpart := column.name + ` ` + column.ctype
		if column.notnull == 1 {
			columnpart = columnpart + ` NOT NULL`
		}
		if column.pk == 1 {
			columnpart = columnpart + ` PRIMARY KEY`
		}

		columnparts = append(columnparts, columnpart)
	}

	columnparts = append(columnparts, t.gcolumn+` BLOB NOT NULL`)

	query := create + `(` + strings.Join(columnparts, `, `) + `);`
	return query
}

func (t table) selectSQL() string {
	var csql []string
	for _, c := range t.columns {
		csql = append(csql, c.name)
	}
	query := `SELECT ` + strings.Join(csql, `,`) + ` FROM "` + t.name + `";`
	return query
}

func (t table) insertSQL() string {
	var csql, vsql []string
	for _, c := range t.columns {
		if c.name != t.gcolumn {
			csql = append(csql, c.name)
			vsql = append(vsql, `?`)
		}
	}
	csql = append(csql, t.gcolumn)
	vsql = append(vsql, `?`)
	query := `INSERT INTO "` + t.name + `"(` + strings.Join(csql, `,`) + `) VALUES(` + strings.Join(vsql, `,`) + `)`
	return query
}

func buildGeometryTable(table_name string, fc *geom.FeatureCollection, gcolumn string, srs int, gtype string) table {
	return table{}
}
