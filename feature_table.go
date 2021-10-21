package gpkg

import (
	"github.com/flywave/go-geom"
	"github.com/flywave/go-geom/general"
)

type FeatureTable struct {
	geometry geom.Geometry
	columns  []interface{}
}

func NewFeatureTable(fc *geom.FeatureCollection, tab *table) []FeatureTable {
	rets := []FeatureTable{}

	for _, f := range fc.Features {
		g := f.Geometry
		if g == nil {
			g = general.GeometryDataAsGeometry(&f.GeometryData)
		}
		columns := []interface{}{}

		for _, c := range tab.columns {
			k := c.name
			if k == ID {
				newv := changeColumnValue(f.ID, &c)
				columns = append(columns, newv)
			} else {
				if v, ok := f.Properties[k]; ok {
					newv := changeColumnValue(v, &c)
					columns = append(columns, newv)
				} else {
					columns = append(columns, nil)
				}
			}
		}

		rets = append(rets, FeatureTable{geometry: g, columns: columns})
	}

	return rets
}
