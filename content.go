package gpkg

import (
	"time"

	"github.com/flywave/go-geo"
)

const (
	DataTypeFeatures   = "features"
	DataTypeAttributes = "attributes"
	DataTypeTitles     = "titles"
)

type Content struct {
	ContentTableName         string     `sql:"type:text" gorm:"column:table_name;unique;not null;primary_key"`
	DataType                 string     `sql:"type:text" gorm:"column:data_type;not null"`
	Identifier               string     `sql:"type:text" gorm:"column:identifier;unique"`
	Description              string     `sql:"type:text" gorm:"column:description;default:''"`
	LastChange               *time.Time `gorm:"column:last_change;not null"`
	MinX                     float64    `gorm:"column:min_x"`
	MinY                     float64    `gorm:"column:min_y"`
	MaxX                     float64    `gorm:"column:max_x"`
	MaxY                     float64    `gorm:"column:max_y"`
	SpatialReferenceSystemId int        `sql:"type:integer REFERENCES gpkg_spatial_ref_sys(srs_id)" gorm:"column:srs_id"`
}

func (Content) TableName() string {
	return "gpkg_contents"
}

func NewContent(tableName string, dataType string, grid *geo.TileGrid) *Content {
	bbox := grid.BBox
	srsId := geo.GetEpsgNum(grid.Srs.SrsCode)
	return &Content{ContentTableName: tableName, Identifier: tableName, DataType: dataType, Description: "", MinX: bbox.Min[0], MinY: bbox.Min[1], MaxX: bbox.Max[0], MaxY: bbox.Max[1], SpatialReferenceSystemId: srsId}
}
