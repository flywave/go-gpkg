package gpkg

import (
	"bytes"
	"errors"
)

type TileFormat uint8

const (
	UNKNOWN TileFormat = iota
	PNG
	JPG
	PBF
	WEBP
)

func (t TileFormat) String() string {
	switch t {
	case PNG:
		return "png"
	case JPG:
		return "jpg"
	case WEBP:
		return "webp"
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
	default:
		return ""
	}
}

func detectTileFormat(data *[]byte) (TileFormat, error) {
	patterns := map[TileFormat][]byte{
		PNG:  []byte("\x89\x50\x4E\x47\x0D\x0A\x1A\x0A"),
		JPG:  []byte("\xFF\xD8\xFF"),
		WEBP: []byte("\x52\x49\x46\x46\xc0\x00\x00\x00\x57\x45\x42\x50\x56\x50"),
	}

	for format, pattern := range patterns {
		if bytes.HasPrefix(*data, pattern) {
			return format, nil
		}
	}

	return UNKNOWN, errors.New("could not detect tile format")
}
