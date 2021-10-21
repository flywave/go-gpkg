package gpkg

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/flywave/go-geom"
)

const (
	FID = "fid"
	ID  = "id"
)

func changeColumnValue(val interface{}, c *column) interface{} {
	if strings.Contains(strings.ToLower(c.ctype), "varchar") {
		switch v := val.(type) {
		case string:
			return v
		case []byte:
			return string(v)
		case int:
			return strconv.Itoa(v)
		case int64:
			return strconv.Itoa(int(v))
		case int32:
			return strconv.Itoa(int(v))
		case uint64:
			return strconv.Itoa(int(v))
		case uint32:
			return strconv.Itoa(int(v))
		default:
			data, _ := json.Marshal(val)
			return string(data)
		}
	} else if strings.ToLower(c.ctype) == "integer" {
		switch v := val.(type) {
		case string:
			i, _ := strconv.Atoi(v)
			return i
		case bool:
			return v
		case int:
			return v
		case int64:
			return v
		case int32:
			return v
		case uint64:
			return v
		case uint32:
			return v
		case float32:
			return int(v)
		case float64:
			return int(v)
		}
	} else if strings.ToLower(c.ctype) == "real" {
		switch v := val.(type) {
		case string:
			i, _ := strconv.ParseFloat(v, 64)
			return i
		case int:
			return float64(v)
		case int64:
			return float64(v)
		case int32:
			return float64(v)
		case uint64:
			return float64(v)
		case uint32:
			return float64(v)
		case float32:
			return v
		case float64:
			return v
		}
	} else if strings.ToLower(c.ctype) == "blob" {
		switch v := val.(type) {
		case string:
			return []byte(v)
		case []byte:
			return v
		case int:
			return []byte(strconv.Itoa(v))
		case int64:
			return []byte(strconv.Itoa(int(v)))
		case int32:
			return []byte(strconv.Itoa(int(v)))
		case uint64:
			return []byte(strconv.Itoa(int(v)))
		case uint32:
			return []byte(strconv.Itoa(int(v)))
		default:
			data, _ := json.Marshal(val)
			return data
		}
	} else if strings.ToLower(c.ctype) == "text" {
		switch v := val.(type) {
		case string:
			return v
		case []byte:
			return string(v)
		case int:
			return strconv.Itoa(v)
		case int64:
			return strconv.Itoa(int(v))
		case int32:
			return strconv.Itoa(int(v))
		case uint64:
			return strconv.Itoa(int(v))
		case uint32:
			return strconv.Itoa(int(v))
		default:
			data, _ := json.Marshal(val)
			return string(data)
		}
	}

	return nil
}

func newPKColumn(name string, val interface{}) *column {
	if name == "fid" || name == "id" {
		switch val.(type) {
		case string:
			return &column{name: name, ctype: "varchar(255)", notnull: 1, pk: 1}
		case int:
		case int64:
		case int32:
		case uint64:
		case uint32:
		case float32:
		case float64:
			return &column{name: name, ctype: "integer", notnull: 1, pk: 1}
		}
	}
	return nil
}

func newValueColumn(name string, val interface{}) *column {
	switch val.(type) {
	case string:
		return &column{name: name, ctype: "text", notnull: 0, pk: 0}
	case []byte:
		return &column{name: name, ctype: "blob", notnull: 0, pk: 0}
	case bool:
	case int:
	case int64:
	case int32:
	case uint64:
	case uint32:
		return &column{name: name, ctype: "integer", notnull: 0, pk: 0}
	case float32:
	case float64:
		return &column{name: name, ctype: "real", notnull: 0, pk: 0}
	default:
		return &column{name: name, ctype: "text", notnull: 0, pk: 0}
	}
	return nil
}

type column struct {
	cid       int
	name      string
	ctype     string
	notnull   int
	dfltValue *int
	pk        int
}

func (c *column) eq(t *column) bool {
	return c.name == t.name && c.ctype == t.ctype && c.notnull == t.notnull && c.pk == t.pk
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
	columnMap := make(map[string]*column)

	for _, f := range fc.Features {
		if f.ID != nil {
			if c, ok := columnMap[ID]; ok {
				c1 := newPKColumn(ID, f.ID)
				if c1 != nil && !c1.eq(c) {
					f.ID = changeColumnValue(f.ID, c)
				}
			} else {
				columnMap[ID] = newPKColumn(ID, f.ID)
			}
		}

		for k, v := range f.Properties {
			k := strings.ToLower(k)
			if c, ok := columnMap[k]; ok {
				c1 := newValueColumn(k, v)
				if c1 != nil && !c1.eq(c) {
					newv := changeColumnValue(v, c)
					if newv != nil {
						f.Properties[k] = newv
					} else {
						delete(f.Properties, k)
					}
				}
			} else {
				columnMap[k] = newValueColumn(k, v)
			}
		}
	}

	columns := []column{}

	for _, v := range columnMap {
		if v != nil {
			columns = append(columns, *v)
		}
	}

	return table{name: table_name, columns: columns, gcolumn: gcolumn, gtype: gtype, srs: srs}
}
