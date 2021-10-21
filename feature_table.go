package gpkg

import "github.com/flywave/go-geom"

type FeatureTable struct {
	geometry geom.Geometry
	columns  []interface{}
}

func NewFeatureTable(fc *geom.FeatureCollection, tab *table) []FeatureTable {
	return nil
}
