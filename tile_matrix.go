package gpkg

import (
	"github.com/flywave/go-geo"
)

type TileMatrix struct {
	Name         string  `sql:"type:text" gorm:"column:table_name;not null"`
	ZoomLevel    int8    `gorm:"column:zoom_level;not null"`
	MatrixWidth  uint64  `gorm:"column:matrix_width;not null"`
	MatrixHeight uint64  `gorm:"column:matrix_height;not null"`
	TileWidth    uint32  `gorm:"column:tile_width;not null"`
	TileHeight   uint32  `gorm:"column:tile_height;not null"`
	PixelXSize   float64 `gorm:"column:pixel_x_size;not null"`
	PixelYSize   float64 `gorm:"column:pixel_y_size;not null"`
}

func (TileMatrix) TableName() string {
	return "gpkg_tile_matrix"
}

func NewTileMatrixs(tableName string, grid *geo.TileGrid) []TileMatrix {
	len := int(grid.Levels)
	tms := []TileMatrix{}
	for i := 0; i < len; i++ {
		res := grid.Resolutions[i]
		grids := grid.GridSizes[i]

		tms = append(tms, TileMatrix{
			Name:         tableName,
			ZoomLevel:    int8(i),
			MatrixWidth:  uint64(grids[0]),
			MatrixHeight: uint64(grids[1]),
			TileWidth:    grid.TileSize[0],
			TileHeight:   grid.TileSize[1],
			PixelXSize:   res,
			PixelYSize:   res,
		})
	}
	return tms
}
