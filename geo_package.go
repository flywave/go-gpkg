package gpkg

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	vec2d "github.com/flywave/go3d/float64/vec2"

	"github.com/flywave/go-geo"
	"github.com/flywave/go-geom"
	"github.com/flywave/go-geom/general"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/pkg/errors"
)

const (
	ApplicationID = 0x47504B47 // "GPKG"
	UserVersion   = 0x000027D9 // 10201
)

var (
	initialSQL = fmt.Sprintf(
		`
		PRAGMA application_id = %d;
		PRAGMA user_version = %d ;
		PRAGMA foreign_keys = ON ;
		`,
		ApplicationID,
		UserVersion,
	)
)

type GeoPackage struct {
	Uri string
	DB  *gorm.DB
}

func New(uri string) *GeoPackage {
	return &GeoPackage{
		Uri: uri,
	}
}

func Create(uri string) *GeoPackage {
	gpkg := &GeoPackage{
		Uri: uri,
	}
	gpkg.Init()
	gpkg.AutoMigrate()
	return gpkg
}

func (g *GeoPackage) Exists() bool {
	if _, err := os.Stat(g.Uri); os.IsNotExist(err) {
		return false
	}
	return true
}

func (g *GeoPackage) Size() (int64, error) {
	fi, err := os.Stat(g.Uri)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (g *GeoPackage) Init() error {
	db, err := gorm.Open("sqlite3", g.Uri)
	if err != nil {
		return err
	}
	err = db.Exec(initialSQL).Error
	if err != nil {
		return err
	}
	g.DB = db
	return nil
}

func (g *GeoPackage) AutoMigrate() error {
	err := g.DB.AutoMigrate(Content{}).Error
	if err != nil {
		return errors.Wrap(err, "Error migrating Content")
	}
	err = g.DB.AutoMigrate(TileMatrix{}).Error
	if err != nil {
		return errors.Wrap(err, "Error migrating TileMatrix")
	}
	err = g.DB.AutoMigrate(TileMatrixSet{}).Error
	if err != nil {
		return errors.Wrap(err, "Error migrating TileMatrixSet")
	}
	err = g.DB.AutoMigrate(Metadata{}).Error
	if err != nil {
		return errors.Wrap(err, "Error migrating Metadata")
	}
	err = g.DB.AutoMigrate(MetadataReference{}).Error
	if err != nil {
		return errors.Wrap(err, "Error migrating MetadataReference")
	}
	err = g.DB.AutoMigrate(SpatialReferenceSystem{}).Error
	if err != nil {
		return errors.Wrap(err, "Error migrating SpatialReferenceSystem")
	}
	err = g.DB.AutoMigrate(GeometryColumn{}).Error
	if err != nil {
		return errors.Wrap(err, "Error migrating GeometryColumn")
	}
	return nil
}

func (g *GeoPackage) AutoMigrateRelatedTables() error {
	err := g.DB.AutoMigrate(Relation{}).Error
	if err != nil {
		return errors.Wrap(err, "Error migrating Relation")
	}

	err = g.DB.AutoMigrate(Extension{}).Error
	if err != nil {
		return errors.Wrap(err, "Error migrating Extension")
	}

	extension := Extension{
		Table:      Relation{}.TableName(),
		Column:     nil,
		Extension:  "related_tables",
		Definition: "TBD",
		Scope:      "read-write",
	}

	err = g.DB.Where(extension).Assign(extension).FirstOrCreate(&extension).Error
	if err != nil {
		return errors.Wrap(err, "Error creating extension "+fmt.Sprint(extension))
	}
	return nil
}

func (g *GeoPackage) GetSpatialReferenceSystem(srs_id int) (SpatialReferenceSystem, error) {
	srs := SpatialReferenceSystem{}
	err := g.DB.First(&srs, SpatialReferenceSystem{SpatialReferenceSystemId: &srs_id}).Error
	return srs, err
}

func (g *GeoPackage) GetSpatialReferenceSystemCode(srs_id int) (string, error) {
	srs, err := g.GetSpatialReferenceSystem(srs_id)
	if err != nil {
		return "", err
	}
	return srs.Code(), nil
}

func (g *GeoPackage) QueryInt(stmt string) (int, error) {
	result := 0

	rows, err := g.DB.DB().Query(stmt)
	if err != nil {
		return result, err
	}

	if rows.Next() {
		if err := rows.Scan(&result); err != nil {
			return result, err
		}
	}

	return result, nil
}

func (g *GeoPackage) GetTileWidth(table string) (int, error) {
	stmt := "SELECT tile_width FROM gpkg_tile_matrix WHERE table_name = \"%s\" ORDER BY zoom_level LIMIT 1;"
	return g.QueryInt(fmt.Sprintf(stmt, table))
}

func (g *GeoPackage) GetTileHeight(table string) (int, error) {
	stmt := "SELECT tile_height FROM gpkg_tile_matrix WHERE table_name = \"%s\" ORDER BY zoom_level LIMIT 1;"
	return g.QueryInt(fmt.Sprintf(stmt, table))
}

func (g *GeoPackage) GetExtent() (*general.Extent, error) {
	extent := general.Extent{}

	rows, err := g.DB.DB().Query("SELECT min(min_x), max(max_x), min(min_y), max(max_y) FROM gpkg_contents;")
	if err != nil {
		return nil, err
	}

	if rows.Next() {
		var minx, miny, maxx, maxy *float64
		if err := rows.Scan(&minx, &miny, &maxx, &maxy); err != nil {
			return nil, err
		}
		if minx != nil && miny != nil && maxx != nil && maxy != nil {
			extent = general.Extent{*minx, *miny, *maxx, *maxy}
			return &extent, nil
		}
	}

	return nil, errors.New("bounds not set!")
}

func (g *GeoPackage) GetCoverage() (geo.Coverage, error) {
	ext, err := g.GetExtent()
	if err != nil {
		return nil, err
	}

	rows, err := g.DB.DB().Query("SELECT srs_id FROM gpkg_contents LIMIT 1;")
	if err != nil {
		return nil, err
	}
	var srscode int
	if rows.Next() {
		if err := rows.Scan(&srscode); err != nil {
			return nil, err
		}
	}

	return geo.NewBBoxCoverage(vec2d.Rect{Min: vec2d.T{ext[0], ext[1]}, Max: vec2d.T{ext[2], ext[3]}}, geo.NewProj(srscode), false), nil
}

func (g *GeoPackage) GetGeometryType(table_name string, column_name string) (string, error) {
	geometry_type := ""

	rows, err := g.DB.DB().Query("SELECT geometry_type_name FROM gpkg_geometry_columns WHERE table_name='" + table_name + "' and column_name='" + column_name + "';")
	if err != nil {
		return "", err
	}

	if rows.Next() {
		if err := rows.Scan(&geometry_type); err != nil {
			return "", err
		}
	}

	return geometry_type, nil
}

func (g *GeoPackage) GetTile(table string, z int, x int, y int) ([]byte, error) {
	b := make([]byte, 0)

	stmt := "SELECT tile_data FROM %s WHERE zoom_level = %d and tile_column = %d and tile_row = %d LIMIT 1;"
	rows, err := g.DB.DB().Query(fmt.Sprintf(stmt, table, z, x, y))
	if err != nil {
		return b, err
	}

	if rows.Next() {
		if err := rows.Scan(&b); err != nil {
			return b, err
		}
	}

	return b, nil
}

func (g *GeoPackage) StoreTile(table string, z int, x int, y int, data []byte) error {
	stmt := fmt.Sprintf("INSERT OR REPLACE INTO [%s] (zoom_level, tile_column, tile_row, tile_data) VALUES (?,?,?,?)", table)

	_, err := g.DB.DB().Exec(stmt, z, x, y, data)
	if err != nil {
		return err
	}

	return nil
}

func (g *GeoPackage) GetMaxZoom(table string) (int, error) {
	stmt := "SELECT max(zoom_level) FROM gpkg_tile_matrix WHERE table_name = \"%s\";"
	return g.QueryInt(fmt.Sprintf(stmt, table))
}

func (g *GeoPackage) GetZoomLevelsAndResolutions(table string) ([]int, []float64, error) {
	stmt := "SELECT zoom_level, pixel_x_size FROM gpkg_tile_matrix WHERE table_name = \"%s\";"
	levels := make([]int, 0)
	resolutions := make([]float64, 0)

	rows, err := g.DB.DB().Query(stmt, table)
	if err != nil {
		return nil, nil, err
	}

	for rows.Next() {
		level := 0
		res := float64(0)
		if err := rows.Scan(&level, &res); err != nil {
			return nil, nil, err
		}
		levels = append(levels, level)
		resolutions = append(resolutions, res)
	}

	return levels, resolutions, nil
}

func (g *GeoPackage) GetFeatureCollection(table_name string) (*geom.FeatureCollection, error) {
	stmt := "SELECT * FROM %s;"
	rows, err := g.DB.DB().Query(fmt.Sprintf(stmt, table_name))
	if err != nil {
		return &geom.FeatureCollection{}, err
	}

	columns, _ := rows.Columns()
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	fc := geom.NewFeatureCollection()

	for rows.Next() {
		var featureId interface{}
		featureProperties := map[string]interface{}{}
		var featureGeometry *geom.GeometryData
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return &geom.FeatureCollection{}, err
		}
		for i, col := range columns {
			if col == ID || col == FID {
				switch values[i].(type) {
				case []byte:
					featureId = string(values[i].([]byte))
				default:
					featureId = values[i]
				}
			} else {
				switch values[i].(type) {
				case []byte:
					geometryType, err := g.GetGeometryType(table_name, col)
					if err != nil {
						return &geom.FeatureCollection{}, err
					}
					if len(geometryType) > 0 {
						v := values[i].([]byte)
						g, err := DecodeGeometry(v)
						if err != nil {
							return &geom.FeatureCollection{}, err
						}

						featureGeometry = g.Geometry
					} else {
						featureProperties[col] = string(values[i].([]byte))
					}
				default:
					featureProperties[col] = values[i]
				}
			}
		}

		fc.AddFeature(&geom.Feature{ID: featureId, Properties: featureProperties, GeometryData: *featureGeometry})
	}

	return fc, nil
}

func (g *GeoPackage) GetVectorLayers() ([]VectorLayer, error) {
	vectorLayers := make([]VectorLayer, 0)

	rows, err := g.DB.DB().Query("SELECT C.table_name, G.geometry_type_name FROM gpkg_contents as C LEFT JOIN gpkg_geometry_columns AS G ON C.table_name = G.table_name WHERE C.data_type = 'features';")
	if err != nil {
		return vectorLayers, err
	}

	for rows.Next() {
		layerName := ""
		layerType := ""
		if err := rows.Scan(&layerName, &layerType); err != nil {
			return vectorLayers, err
		}
		vectorLayers = append(vectorLayers, VectorLayer{
			Name: layerName,
			Type: layerType,
		})
	}

	return vectorLayers, nil
}

func (g *GeoPackage) GetVectorLayersAsList() (*VectorLayerList, error) {
	vectorLayers, err := g.GetVectorLayers()
	if err != nil {
		return &VectorLayerList{}, err
	}
	return &VectorLayerList{vectorLayers: vectorLayers}, nil
}

func (g *GeoPackage) GetTileMatrixSets() ([]TileMatrixSet, error) {
	tileMatrixSets := make([]TileMatrixSet, 0)
	err := g.DB.Find(&tileMatrixSets).Error
	return tileMatrixSets, err
}

func (g *GeoPackage) GetTileMatrixSetsAsIterator() (*TileMatrixSetIterator, error) {
	tileMatrixSets, err := g.GetTileMatrixSets()
	if err != nil {
		return &TileMatrixSetIterator{}, err
	}
	return &TileMatrixSetIterator{tileMatrixSets: tileMatrixSets, index: 0}, nil
}

func (g *GeoPackage) GetTileMatrixSetsAsList() (*TileMatrixSetList, error) {
	tileMatrixSets, err := g.GetTileMatrixSets()
	if err != nil {
		return &TileMatrixSetList{}, err
	}
	return &TileMatrixSetList{tileMatrixSets: tileMatrixSets}, nil
}

func (g *GeoPackage) Close() error {
	return g.DB.Close()
}

func (g *GeoPackage) verifyTable(table_name string) bool {
	table_type := ""

	rows, err := g.DB.DB().Query("SELECT name FROM sqlite_master WHERE type='table' AND name= '" + table_name + "';")
	if err != nil {
		return false
	}

	if rows.Next() {
		if err := rows.Scan(&table_type); err != nil {
			return false
		}
	}

	return table_type != ""
}

func (g *GeoPackage) verifyGPKGContents(table_name string, type_ string, srsCode int) bool {
	row := g.DB.DB().QueryRow("SELECT data_type, srs_id FROM gpkg_contents WHERE table_name = \"" + table_name + "\";")

	if row.Err() != nil {
		return false
	}

	var dataType string
	var srs_id int

	err := row.Scan(&dataType, &srs_id)
	if err != nil {
		return false
	}

	var coordsys_id int
	cur := g.DB.DB().QueryRow("SELECT organization_coordsys_id FROM gpkg_spatial_ref_sys WHERE srs_id = ?;", srs_id)

	err = cur.Scan(&coordsys_id)
	if err != nil {
		return false
	}

	if dataType != type_ {
		return false
	}

	if coordsys_id != srsCode {
		return false
	}

	return true
}

func (g *GeoPackage) verifyTileSize(table_name string, tileSize [2]int) bool {
	tileHeight, err := g.GetTileHeight(table_name)
	if err != nil {
		return false
	}
	tileWidth, err := g.GetTileWidth(table_name)
	if err != nil {
		return false
	}
	if tileHeight != tileSize[1] || tileWidth != tileSize[0] {
		return false
	}
	return true
}

func (g *GeoPackage) GetTileFormat(table_name string) (TileFormat, error) {
	b := make([]byte, 0)

	stmt := "SELECT tile_data FROM %s LIMIT 1;"
	rows, err := g.DB.DB().Query(fmt.Sprintf(stmt, table_name))
	if err != nil {
		return UNKNOWN, err
	}

	if rows.Next() {
		if err := rows.Scan(&b); err != nil {
			return UNKNOWN, err
		}
	}

	return detectTileFormat(&b)
}

func (g *GeoPackage) AddTilesTable(table_name string, grid *geo.TileGrid, cov geo.Coverage) error {
	const (
		validateSRSSQL = `
		SELECT Count(*) 
		FROM gpkg_spatial_ref_sys 
		WHERE 
			srs_id=?
		`
		updateContentsTableSQL = `
		INSERT INTO gpkg_contents(
			table_name,
			data_type,
			identifier,
			description,
			srs_id,
			last_change
		)
		VALUES (?,?,?,?,?,?)
    	ON CONFLICT(table_name) DO NOTHING;
		`
		createTilesTableSql = `
		CREATE TABLE IF NOT EXISTS "%v"
		(id          INTEGER PRIMARY KEY AUTOINCREMENT,
		 zoom_level  INTEGER NOT NULL,                
		 tile_column INTEGER NOT NULL,                
		 tile_row    INTEGER NOT NULL,                  
		 tile_data   BLOB    NOT NULL,                 
		 UNIQUE (zoom_level, tile_column, tile_row))
		 `
	)
	var count int

	srs_id := geo.GetEpsgNum(grid.Srs.GetSrsCode())

	err := g.DB.DB().QueryRow(validateSRSSQL, srs_id).Scan(&count)
	if err != nil {
		return err
	}
	if count == 0 {
		srsdef, ok := DefaultSpatialReferenceSystem[srs_id]
		if !ok {
			return fmt.Errorf("unknown srs: %v", srs_id)
		}
		if err = g.UpdateSRS(srsdef); err != nil {
			return err
		}
	}
	_, err = g.DB.DB().Exec(updateContentsTableSQL, table_name, DataTypeTitles, table_name, table_name, srs_id, time.Now())
	if err != nil {
		return err
	}
	_, err = g.DB.DB().Exec(fmt.Sprintf(createTilesTableSql, table_name))
	if err != nil {
		return err
	}

	if cov != nil {
		cov = cov.TransformTo(grid.Srs)
		bbox := cov.GetBBox()
		err := g.UpdateGeometryExtent(table_name, &general.Extent{bbox.Min[0], bbox.Min[1], bbox.Max[0], bbox.Max[1]})
		if err != nil {
			return err
		}
	}

	return g.saveTileMatrixSet(NewTileMatrixSet(table_name, grid), NewTileMatrixs(table_name, grid))
}

func (g *GeoPackage) AddGeometryColumn(table GeometryColumn) error {
	const (
		validateSRSSQL = `
		SELECT Count(*) 
		FROM gpkg_spatial_ref_sys 
		WHERE 
			srs_id=?
		`
		validateTableFieldSQL = `
		SELECT "%v"
		FROM "%v"
		LIMIT 1
		`
		updateContentsTableSQL = `
		INSERT INTO gpkg_contents(
			table_name,
			data_type,
			identifier,
			description,
			srs_id,
			last_change
		)
		VALUES (?,?,?,?,?,?)
    	ON CONFLICT(table_name) DO NOTHING;
		`
		updateGeometryColumnsTableSQL = `
		INSERT INTO gpkg_geometry_columns(
			table_name,
			column_name,
			geometry_type_name,
			srs_id,
			z,
			m
		)
		VALUES(?,?,?,?,?,?)
    	ON CONFLICT(table_name) DO NOTHING;
		`
	)

	var count int

	err := g.DB.DB().QueryRow(validateSRSSQL, table.SpatialReferenceSystemId).Scan(&count)
	if err != nil {
		return err
	}
	if count == 0 {
		srsdef, ok := DefaultSpatialReferenceSystem[table.SpatialReferenceSystemId]
		if !ok {
			return fmt.Errorf("unknown srs: %v", table.SpatialReferenceSystemId)
		}
		if err = g.UpdateSRS(srsdef); err != nil {
			return err
		}
	}
	rows, err := g.DB.DB().Query(fmt.Sprintf(validateTableFieldSQL, table.ColumnName, table.GeometryColumnTableName))
	if err != nil {
		return fmt.Errorf("unknown table %v or field %v : %v", table.GeometryColumnTableName, table.ColumnName, err)
	}
	rows.Close()
	_, err = g.DB.DB().Exec(updateContentsTableSQL, table.GeometryColumnTableName, DataTypeFeatures, table.GeometryColumnTableName, table.GeometryColumnTableName, table.SpatialReferenceSystemId, time.Now())
	if err != nil {
		return err
	}
	_, err = g.DB.DB().Exec(updateGeometryColumnsTableSQL, table.GeometryColumnTableName, table.ColumnName, table.GeometryType, table.SpatialReferenceSystemId, table.Z, table.M)
	return err

}

func (g *GeoPackage) UpdateSRS(srss ...SpatialReferenceSystem) error {
	const (
		UpdateSQL = `
	INSERT INTO gpkg_spatial_ref_sys(
		srs_name,
		srs_id,
		organization,
		organization_coordsys_id,
		definition,
		description
	)
	VALUES %v
    ON CONFLICT(srs_id) DO NOTHING;
	`
		placeHolders = `(?,?,?,?,?,?) `
	)
	if len(srss) == 0 {
		return nil
	}

	valuePlaceHolder := strings.Join(
		strings.SplitN(
			strings.Repeat(placeHolders, len(srss)),
			" ",
			len(srss),
		),
		",",
	)
	updateSQL := fmt.Sprintf(UpdateSQL, valuePlaceHolder)
	values := make([]interface{}, 0, len(srss)*6)

	for _, srs := range srss {
		values = append(
			values,
			srs.Name,
			srs.SpatialReferenceSystemId,
			srs.Organization,
			srs.OrganizationCoordinateSystemId,
			srs.Definition,
			srs.Description,
		)
	}
	_, err := g.DB.DB().Exec(updateSQL, values...)
	return err
}

func (g *GeoPackage) getTableColumns(table string) []column {
	var columns []column
	query := `PRAGMA table_info('%v');`
	rows, err := g.DB.DB().Query(fmt.Sprintf(query, table))

	if err != nil {
		log.Fatalf("err during closing rows: %v - %v", query, err)
		return nil
	}

	for rows.Next() {
		var column column
		err := rows.Scan(&column.cid, &column.name, &column.ctype, &column.notnull, &column.dfltValue, &column.pk)
		if err != nil {
			log.Fatalf("error getting the column information: %s", err)
			return nil
		}
		columns = append(columns, column)
	}
	defer rows.Close()
	return columns
}

func (g *GeoPackage) UpdateGeometryExtent(tablename string, extent *general.Extent) error {
	if extent == nil {
		return nil
	}

	var (
		minx,
		miny,
		maxx,
		maxy *float64

		ext *general.Extent
	)
	const (
		selectSQL = `
		SELECT
			min_x,
			min_y,
			max_x,
			max_y
		FROM 
			gpkg_contents
		WHERE
			table_name = ?
		`
		updateSQL = `
		UPDATE gpkg_contents
		SET
			min_x = ?,
			min_y = ?,
			max_x = ?,
			max_y = ?
		WHERE 
			table_name = ?
		`
	)
	err := g.DB.DB().QueryRow(selectSQL, tablename).Scan(&minx, &miny, &maxx, &maxy)
	if err != nil {
		return err
	}
	if minx == nil || miny == nil || maxx == nil || maxy == nil {
		ext = extent
	} else {
		ext = general.NewExtent([]float64{*minx, *miny}, []float64{*maxx, *maxy})
		ext.Add(extent)
	}
	_, err = g.DB.DB().Exec(updateSQL, ext.MinX(), ext.MinY(), ext.MaxX(), ext.MaxY(), tablename)
	return err
}

func (g *GeoPackage) GetGeomColumn(tablename string) (string, error) {
	selectGeomColSQL := `
	SELECT 
		column_name
	FROM 
		gpkg_geometry_columns
	WHERE
		table_name = ?
	`
	var columnName string

	if err := g.DB.DB().QueryRow(selectGeomColSQL, tablename).Scan(&columnName); err != nil {
		return "", err
	}
	return columnName, nil
}

func (g *GeoPackage) GetSrsid(tablename string) (int, error) {
	selectGeomColSQL := `
	SELECT 
		srs_id
	FROM 
		gpkg_geometry_columns
	WHERE
		table_name = ?
	`

	var srs_id int

	if err := g.DB.DB().QueryRow(selectGeomColSQL, tablename).Scan(&srs_id); err != nil {
		return -1, err
	}
	return srs_id, nil
}

func (g *GeoPackage) CalculateGeometryExtent(tablename string) (*general.Extent, error) {
	const (
		selectGeomColSQL = `
		SELECT 
			column_name
		FROM 
			gpkg_geometry_columns
		WHERE
			table_name = ?
		`
		selectAllSQLFormat = ` SELECT "%v" FROM "%v"`
	)

	var (
		columnName string
		ext        *general.Extent
		err        error
		rows       *sql.Rows
		sb         StandardBinary
	)

	if err = g.DB.DB().QueryRow(selectGeomColSQL, tablename).Scan(&columnName); err != nil {
		return nil, err
	}
	if rows, err = g.DB.DB().Query(fmt.Sprintf(selectAllSQLFormat, columnName, tablename)); err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		rows.Scan(&sb)
		if geom.IsGeometryEmpty(sb.Geometry) {
			continue
		}
		if ext == nil {
			ext, err = general.NewExtentFromGeometry(sb.Geometry)
			if err != nil {
				ext = nil
			}
			continue
		}
		ext.AddGeometry(sb.Geometry)
	}
	return ext, nil
}

func (g *GeoPackage) buildTable(t table) error {
	sql := t.createSQL()
	_, err := g.DB.DB().Exec(sql)
	if err != nil {
		log.Fatalf("error building table in target GeoPackage: %s", err)
	}

	err = g.AddGeometryColumn(GeometryColumn{
		GeometryColumnTableName:  t.name,
		ColumnName:               t.gcolumn,
		GeometryType:             t.gtype,
		SpatialReferenceSystemId: t.srs,
		Z:                        0,
		M:                        0,
	})
	if err != nil {
		log.Println("error adding geometry table in target GeoPackage:", err)
		return err
	}
	return nil
}

func (g *GeoPackage) writeFeatures(datas []FeatureTable, t table, p int) error {
	var ext *general.Extent

	var features [][]interface{}

	for i := range datas {
		feature := datas[i]

		sb, err := NewBinary(int32(t.srs), feature.geometry)
		if err != nil {
			log.Fatalf("Could not create a binary geometry: %s", err)
		}

		data := feature.columns
		raw, _ := sb.Encode()
		data = append(data, raw)
		features = append(features, data)

		if len(features)%p == 0 {
			writeFeaturesArray(features, g, t)
			features = nil
		}

		if ext == nil {
			ext, err = general.NewExtentFromGeometry(feature.geometry)
			if err != nil {
				ext = nil
				log.Println("Failed to create new extent:", err)
				continue
			}
		} else {
			ext.AddGeometry(feature.geometry)
		}

		if i == len(datas)-1 {
			writeFeaturesArray(features, g, t)
			features = nil
		}
	}
	return g.UpdateGeometryExtent(t.name, ext)
}

func writeFeaturesArray(features [][]interface{}, g *GeoPackage, t table) {
	tx, err := g.DB.DB().Begin()
	if err != nil {
		log.Fatalf("Could not start a transaction: %s", err)
	}

	stmt, err := tx.Prepare(t.insertSQL())
	if err != nil {
		log.Fatalf("Could not prepare a statement: %s", err)
	}

	for _, f := range features {
		_, err = stmt.Exec(f...)
		if err != nil {
			log.Fatalf("Could not get a result summary from the prepared statement: %s", err)
		}
	}

	stmt.Close()
	tx.Commit()
}

func (g *GeoPackage) saveTileMatrixSet(tms *TileMatrixSet, ts []TileMatrix) error {
	err := g.DB.Save(tms).Error

	if err != nil {
		return err
	}

	for i := range ts {
		err := g.DB.Save(ts[i]).Error

		if err != nil {
			return err
		}

	}

	return nil
}

func (g *GeoPackage) StoreFeatureCollection(table_name string, fc *geom.FeatureCollection) error {
	selectGeomColSQL := `
	SELECT 
		column_name,
		geometry_type_name,
		srs_id
	FROM 
		gpkg_geometry_columns
	WHERE
		table_name = ?
	`
	var gcolumn string
	var gtype string
	var srs int

	if err := g.DB.DB().QueryRow(selectGeomColSQL, table_name).Scan(&gcolumn, &gtype, &srs); err != nil {
		return err
	}

	var tab table
	columns := g.getTableColumns(table_name)

	if len(columns) != 0 {
		tab = table{name: table_name, columns: columns, gcolumn: gcolumn, srs: srs, gtype: gtype}
	} else {
		tab = buildGeometryTable(table_name, fc, gcolumn, srs, gtype)
		err := g.buildTable(tab)
		if err != nil {
			return err
		}
	}

	ftables := NewFeatureTable(fc, &tab)

	return g.writeFeatures(ftables, tab, 20)
}
