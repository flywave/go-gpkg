package gpkg

import (
	"io/ioutil"
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

	f, _ := os.Open("./data.json")

	data, _ := ioutil.ReadAll(f)

	fcs, _ := general.UnmarshalFeatureCollection(data)

	tt := buildGeometryTable("test", fcs, "geom", 4326, "Point")

	gpkg.buildTable(tt)

	ft := NewFeatureTable(fcs, &tt)

	gpkg.writeFeatures(ft, tt, 1)

	newfc, _ := gpkg.GetFeatureCollection("test")

	if newfc == nil {
		t.FailNow()
	}

	ext, _ := gpkg.GetExtent()

	if ext == nil {
		t.FailNow()
	}

	gpkg.Close()
	os.Remove("./test.gpkg")
}
