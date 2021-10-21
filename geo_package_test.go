package gpkg

import (
	"os"
	"testing"

	"github.com/flywave/go-geo"
	"github.com/flywave/go-geom/general"
)

func TestWriteGPKGTile(t *testing.T) {
	gpkg := Create("./test.gpkg")

	conf := geo.DefaultTileGridOptions()
	conf[geo.TILEGRID_SRS] = geo.NewProj("EPSG:900913")

	grid := geo.NewTileGrid(conf)

	gpkg.AddTilesTable("test", grid, geo.NewBBoxCoverage(*grid.BBox, grid.Srs, false))

	gpkg.StoreTile("test", 0, 0, 0, []byte("test"))

	cov, err := gpkg.GetCoverage()

	if err != nil || cov == nil {
		t.FailNow()
	}

	gpkg.Close()
	os.Remove("./test.gpkg")
}

func TestWriteGPKGGeom(t *testing.T) {
	gpkg := Create("./test.gpkg")

	columns := []column{
		{name: "fid", ctype: "varchar(255)", notnull: 1, pk: 1},
		{name: "state", ctype: "integer", notnull: 0, pk: 0},
		{name: "desc", ctype: "text", notnull: 0, pk: 0},
	}

	tt := table{name: "test", columns: columns, gcolumn: "geom", gtype: "Point", srs: 4326}

	gpkg.buildTable(tt)

	ft := FeatureTable{geometry: general.NewPoint([]float64{180, 180}), columns: []interface{}{"1001", 2, ""}}

	gpkg.writeFeatures([]FeatureTable{ft}, tt, 1)

	gpkg.Close()
	os.Remove("./test.gpkg")
}
