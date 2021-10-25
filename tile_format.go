package gpkg

import (
	"bytes"
	"encoding/binary"
	"errors"

	qmt "github.com/flywave/go-quantized-mesh"
)

type TileFormat uint8

const (
	UNKNOWN TileFormat = iota
	PNG
	JPG
	PBF
	WEBP
	LERC
	TIFF
	TERRAIN
)

func (t TileFormat) String() string {
	switch t {
	case PNG:
		return "png"
	case JPG:
		return "jpg"
	case WEBP:
		return "webp"
	case LERC:
		return "lerc"
	case TIFF:
		return "tiff"
	case PBF:
		return "pbf"
	case TERRAIN:
		return "terrain"
	default:
		return ""
	}
}

func (t TileFormat) ContentType() string {
	switch t {
	case PNG:
		return "image/png"
	case JPG:
		return "image/jpeg"
	case WEBP:
		return "image/webp"
	case LERC:
		return "image/lerc"
	case TIFF:
		return "image/tiff"
	case PBF:
		return "application/x-protobuf"
	case TERRAIN:
		return "application/vnd.quantized-mesh"
	default:
		return ""
	}
}

func detectTileFormat(data *[]byte) (TileFormat, error) {
	patterns := map[TileFormat][][]byte{
		PNG:  {[]byte("\x89\x50\x4E\x47\x0D\x0A\x1A\x0A")},
		JPG:  {[]byte("\xFF\xD8\xFF")},
		WEBP: {[]byte("\x52\x49\x46\x46\xc0\x00\x00\x00\x57\x45\x42\x50\x56\x50")},
		LERC: {[]byte("\x43\x6E\x74\x5A\x49\x6D\x61\x67\x65\x20"), []byte("\x4C\x65\x72\x63\x32\x20")},
		TIFF: {[]byte("\x4D\x4D"), []byte("\x49\x49")},
		PBF:  {[]byte("\x1f\x8b")},
	}

	for format, pattern := range patterns {
		for _, p := range pattern {
			if bytes.HasPrefix(*data, p) {
				return format, nil
			}
		}
	}

	var (
		byteOrder = binary.LittleEndian
	)

	var header qmt.QuantizedMeshHeader

	err := binary.Read(bytes.NewBuffer(*data), byteOrder, &header)
	if err != nil {
		return UNKNOWN, err
	}

	if header.CenterX == header.BoundingSphereCenterX && header.CenterY == header.BoundingSphereCenterY && header.CenterZ == header.BoundingSphereCenterZ {
		return TERRAIN, err
	}

	return UNKNOWN, errors.New("could not detect tile format")
}
