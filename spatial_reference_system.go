package gpkg

import (
	"fmt"
	"strconv"
	"strings"
)

type SpatialReferenceSystem struct {
	Name                           string `gorm:"column:srs_name;unique;not null;primary_key"`
	SpatialReferenceSystemId       *int   `gorm:"column:srs_id;unique;not null;primary_key"`
	Organization                   string `gorm:"column:organization;not null" json:"org"`
	OrganizationCoordinateSystemId *int   `gorm:"column:organization_coordsys_id;not null" json:"org_id"`
	Definition                     string `gorm:"column:definition;not null" json:"def"`
	Description                    string `gorm:"column:description" json:"description"`
}

func (srs *SpatialReferenceSystem) Code() string {
	if len(srs.Organization) > 0 && srs.OrganizationCoordinateSystemId != nil {
		return strings.ToUpper(srs.Organization + ":" + strconv.Itoa(*srs.OrganizationCoordinateSystemId))
	}
	return ""
}

func (SpatialReferenceSystem) TableName() string {
	return "gpkg_spatial_ref_sys"
}

func NewSpatialReferenceSystem(epsg int) *SpatialReferenceSystem {
	return &SpatialReferenceSystem{Name: fmt.Sprintf("EPSG:%d", epsg), SpatialReferenceSystemId: &epsg, OrganizationCoordinateSystemId: &epsg, Organization: "epsg", Definition: "Not provided"}
}

var (
	epsg_0      = 0
	epsg_1      = -1
	epsg_3857   = 3857
	epsg_4326   = 4326
	epsg_900913 = 900913
)

var DefaultSpatialReferenceSystem = map[int]SpatialReferenceSystem{
	epsg_0: {Name: "any", SpatialReferenceSystemId: &epsg_0, OrganizationCoordinateSystemId: &epsg_0, Organization: "none", Definition: ""},
	epsg_1: {Name: "any", SpatialReferenceSystemId: &epsg_1, OrganizationCoordinateSystemId: &epsg_1, Organization: "none", Definition: ""},
	epsg_3857: {Name: "WGS 84 / Pseudo-Mercator", SpatialReferenceSystemId: &epsg_3857, OrganizationCoordinateSystemId: &epsg_3857, Organization: "epsg", Definition: `
	PROJCS["WGS 84 / Pseudo-Mercator",
    GEOGCS["WGS 84",
        DATUM["WGS_1984",
            SPHEROID["WGS 84",6378137,298.257223563,
                AUTHORITY["EPSG","7030"]],
            AUTHORITY["EPSG","6326"]],
        PRIMEM["Greenwich",0,
            AUTHORITY["EPSG","8901"]],
        UNIT["degree",0.0174532925199433,
            AUTHORITY["EPSG","9122"]],
        AUTHORITY["EPSG","4326"]],
    PROJECTION["Mercator_1SP"],
    PARAMETER["central_meridian",0],
    PARAMETER["scale_factor",1],
    PARAMETER["false_easting",0],
    PARAMETER["false_northing",0],
    UNIT["metre",1,
        AUTHORITY["EPSG","9001"]],
    AXIS["X",EAST],
    AXIS["Y",NORTH],
    EXTENSION["PROJ4","+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs"],
    AUTHORITY["EPSG","3857"]]
	`},
	epsg_4326: {Name: "WGS 84", SpatialReferenceSystemId: &epsg_4326, OrganizationCoordinateSystemId: &epsg_4326, Organization: "epsg", Definition: `
	GEOGCS["WGS 84",
    DATUM["WGS_1984",
        SPHEROID["WGS 84",6378137,298.257223563,
            AUTHORITY["EPSG","7030"]],
        AUTHORITY["EPSG","6326"]],
    PRIMEM["Greenwich",0,
        AUTHORITY["EPSG","8901"]],
    UNIT["degree",0.0174532925199433,
        AUTHORITY["EPSG","9122"]],
    AUTHORITY["EPSG","4326"]]
	`},
	epsg_900913: {Name: "Google Maps Global Mercator", SpatialReferenceSystemId: &epsg_900913, OrganizationCoordinateSystemId: &epsg_900913, Organization: "epsg", Definition: `
	PROJCS["Google Maps Global Mercator",
    GEOGCS["WGS 84",
        DATUM["WGS_1984",
            SPHEROID["WGS 84",6378137,298.257223563,
                AUTHORITY["EPSG","7030"]],
            AUTHORITY["EPSG","6326"]],
        PRIMEM["Greenwich",0,
            AUTHORITY["EPSG","8901"]],
        UNIT["degree",0.01745329251994328,
            AUTHORITY["EPSG","9122"]],
        AUTHORITY["EPSG","4326"]],
    PROJECTION["Mercator_2SP"],
    PARAMETER["standard_parallel_1",0],
    PARAMETER["latitude_of_origin",0],
    PARAMETER["central_meridian",0],
    PARAMETER["false_easting",0],
    PARAMETER["false_northing",0],
    UNIT["Meter",1],
    EXTENSION["PROJ4","+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs"],
    AUTHORITY["EPSG","900913"]]
	`},
}
