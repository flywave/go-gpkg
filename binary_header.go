package gpkg

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/flywave/go-geom"
	"github.com/flywave/go-geom/general"
	"github.com/flywave/go-geom/wkb"
)

type envelopeType uint8

var Magic = [2]byte{0x47, 0x50}

const (
	EnvelopeTypeNone    = envelopeType(0)
	EnvelopeTypeXY      = envelopeType(1)
	EnvelopeTypeXYZ     = envelopeType(2)
	EnvelopeTypeXYM     = envelopeType(3)
	EnvelopeTypeXYZM    = envelopeType(4)
	EnvelopeTypeInvalid = envelopeType(5)
)

func (et envelopeType) NumberOfElements() int {
	switch et {
	case EnvelopeTypeNone:
		return 0
	case EnvelopeTypeXY:
		return 4
	case EnvelopeTypeXYZ:
		return 6
	case EnvelopeTypeXYM:
		return 6
	case EnvelopeTypeXYZM:
		return 8
	default:
		return -1
	}
}

func (et envelopeType) String() string {
	str := "NONEXYZMXYMINVALID"
	switch et {
	case EnvelopeTypeNone:
		return str[0:4]
	case EnvelopeTypeXY:
		return str[4 : 4+2]
	case EnvelopeTypeXYZ:
		return str[4 : 4+3]
	case EnvelopeTypeXYM:
		return str[8 : 8+3]
	case EnvelopeTypeXYZM:
		return str[4 : 4+4]
	default:
		return str[11:]
	}
}

const (
	maskByteOrder        = 1 << 0
	maskEnvelopeType     = 1<<3 | 1<<2 | 1<<1
	maskEmptyGeometry    = 1 << 4
	maskGeoPackageBinary = 1 << 5
)

type headerFlags byte

func (hf headerFlags) String() string { return fmt.Sprintf("0x%02x", uint8(hf)) }

func (hf headerFlags) Endian() binary.ByteOrder {
	if hf&maskByteOrder == 0 {
		return binary.BigEndian
	}
	return binary.LittleEndian
}

func (hf headerFlags) Envelope() envelopeType {
	et := uint8((hf & maskEnvelopeType) >> 1)
	if et >= uint8(EnvelopeTypeInvalid) {
		return EnvelopeTypeInvalid
	}
	return envelopeType(et)
}

func (hf headerFlags) IsEmpty() bool { return ((hf & maskEmptyGeometry) >> 4) == 1 }

func (hf headerFlags) IsStandard() bool { return ((hf & maskGeoPackageBinary) >> 5) == 0 }

func EncodeHeaderFlags(byteOrder binary.ByteOrder, envelope envelopeType, extendGeom bool, emptyGeom bool) headerFlags {
	var hf byte
	if byteOrder == binary.LittleEndian {
		hf = 1
	}
	hf = hf | byte(envelope)<<1
	if emptyGeom {
		hf = hf | maskEmptyGeometry
	}
	if extendGeom {
		hf = hf | maskGeoPackageBinary
	}
	return headerFlags(hf)
}

type BinaryHeader struct {
	magic      [2]byte // should be 0x47 0x50  (GP in ASCII)
	version    uint8
	flags      headerFlags
	srsid      int32
	envelope   []float64
	headerSize int // total bytes in header
}

func NewBinaryHeaderByGeom(byteOrder binary.ByteOrder, srsid int32, envelope []float64, et envelopeType, extendGeom bool, emptyGeom bool) (*BinaryHeader, error) {
	if et.NumberOfElements() != len(envelope) {
		return nil, errors.New("ErrEnvelopeEnvelopeTypeMismatch")
	}
	return &BinaryHeader{
		magic:    Magic,
		flags:    EncodeHeaderFlags(byteOrder, et, extendGeom, emptyGeom),
		srsid:    srsid,
		envelope: envelope,
	}, nil
}

func NewBinaryHeader(data []byte) (*BinaryHeader, error) {
	if len(data) < 8 {
		return nil, errors.New("not enough bytes to decode header")
	}

	var bh BinaryHeader
	bh.magic[0] = data[0]
	bh.magic[1] = data[1]
	bh.version = data[2]
	bh.flags = headerFlags(data[3])
	en := bh.flags.Endian()
	bh.srsid = int32(en.Uint32(data[4 : 4+4]))

	bytes := data[8:]
	et := bh.flags.Envelope()
	if et == EnvelopeTypeInvalid {
		return nil, errors.New("invalid envelope type")
	}
	if et == EnvelopeTypeNone {
		return &bh, nil
	}
	num := et.NumberOfElements()
	if len(bytes) < (num * 8) {
		return nil, errors.New("not enough bytes to decode header")
	}

	bh.envelope = make([]float64, 0, num)
	for i := 0; i < num; i++ {
		bits := en.Uint64(bytes[i*8 : (i*8)+8])
		bh.envelope = append(bh.envelope, math.Float64frombits(bits))
	}
	if bh.magic[0] != Magic[0] || bh.magic[1] != Magic[1] {
		return &bh, errors.New("invalid magic number")
	}
	return &bh, nil

}

func (h *BinaryHeader) Magic() [2]byte {
	if h == nil {
		return Magic
	}
	return h.magic
}

func (h *BinaryHeader) Version() uint8 {
	if h == nil {
		return 0
	}
	return h.version
}

func (h *BinaryHeader) EnvelopeType() envelopeType {
	if h == nil {
		return EnvelopeTypeInvalid
	}
	return h.flags.Envelope()
}

func (h *BinaryHeader) SRSId() int32 {
	if h == nil {
		return 0
	}
	return h.srsid
}

func (h *BinaryHeader) Envelope() []float64 {
	if h == nil {
		return nil
	}
	return h.envelope
}

func (h *BinaryHeader) IsGeometryEmpty() bool {
	if h == nil {
		return true
	}
	return h.flags.IsEmpty()
}

func (h *BinaryHeader) IsStandardGeometry() bool {
	if h == nil {
		return true
	}
	return h.flags.IsStandard()
}

func (h *BinaryHeader) Size() int {
	if h == nil {
		return 0
	}
	return (len(h.envelope) * 8) + 8
}

func (h *BinaryHeader) EncodeTo(data *bytes.Buffer) error {
	if data == nil {
		return errors.New("buffer is nil")
	}
	var err error
	hh := h
	if hh == nil {
		hh, err = NewBinaryHeaderByGeom(h.Endian(), 0, []float64{}, h.EnvelopeType(), false, true)
		if err != nil {
			return err
		}
	}
	en := h.Endian()
	data.Write([]byte{h.magic[0], h.magic[1], byte(h.version), byte(h.flags)})
	err = binary.Write(data, en, h.srsid)
	if err != nil {
		return err
	}
	return binary.Write(data, en, h.envelope)
}

func (h *BinaryHeader) Encode() ([]byte, error) {
	var data bytes.Buffer
	if err := h.EncodeTo(&data); err != nil {
		return nil, err
	}
	return data.Bytes(), nil
}
func (h *BinaryHeader) Endian() binary.ByteOrder {
	if h == nil {
		return binary.BigEndian
	}
	return h.flags.Endian()
}

func DecodeBinaryHeader(data []byte) (*BinaryHeader, error) {
	if len(data) < 8 {
		return nil, errors.New("ErrInsufficentBytes")
	}

	var bh BinaryHeader
	bh.magic[0] = data[0]
	bh.magic[1] = data[1]
	bh.version = data[2]
	bh.flags = headerFlags(data[3])
	en := bh.flags.Endian()
	bh.srsid = int32(en.Uint32(data[4 : 4+4]))

	bytes := data[8:]
	et := bh.flags.Envelope()
	if et == EnvelopeTypeInvalid {
		return nil, errors.New("ErrInsufficentBytes")
	}
	if et == EnvelopeTypeNone {
		return &bh, nil
	}
	num := et.NumberOfElements()

	if len(bytes) < (num * 8) {
		return nil, errors.New("ErrInsufficentBytes")
	}

	bh.envelope = make([]float64, 0, num)
	for i := 0; i < num; i++ {
		bits := en.Uint64(bytes[i*8 : (i*8)+8])
		bh.envelope = append(bh.envelope, math.Float64frombits(bits))
	}
	if bh.magic[0] != Magic[0] || bh.magic[1] != Magic[1] {
		return &bh, errors.New("ErrInsufficentBytes")
	}
	return &bh, nil
}

type StandardBinary struct {
	Header   *BinaryHeader
	SRSID    int32
	Geometry *geom.GeometryData
}

func DecodeGeometry(bytes_ []byte) (*StandardBinary, error) {
	h, err := DecodeBinaryHeader(bytes_)
	if err != nil {
		return nil, err
	}

	geo, _, err := wkb.DecodeWKB(bytes.NewBuffer(bytes_[h.Size():]))
	if err != nil {
		return nil, err
	}
	return &StandardBinary{
		Header:   h,
		SRSID:    h.SRSId(),
		Geometry: geo,
	}, nil
}

func (sb StandardBinary) Encode() ([]byte, error) {
	var data bytes.Buffer
	err := sb.Header.EncodeTo(&data)
	if err != nil {
		return nil, err
	}
	srsid := uint32(sb.Header.srsid)
	err = wkb.EncodeWKB(sb.Geometry, &srsid, &data)
	if err != nil {
		return nil, err
	}
	return data.Bytes(), nil
}

func NewBinary(srs int32, geo geom.Geometry) (*StandardBinary, error) {
	var (
		emptyGeo = geom.IsGeometryEmpty(geo)
		err      error
		extent   = []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN()}
		h        *BinaryHeader
	)

	if !emptyGeo {
		ext, err := general.NewExtentFromGeometry(geo)
		if err != nil {
			return nil, err
		}
		extent = ext[:]
	}

	h, err = NewBinaryHeaderByGeom(binary.LittleEndian, srs, extent, EnvelopeTypeXY, false, emptyGeo)
	if err != nil {
		return nil, err
	}

	return &StandardBinary{
		Header:   h,
		SRSID:    srs,
		Geometry: geom.NewGeometryData(geo),
	}, nil
}

func (sb *StandardBinary) Extent() *general.Extent {
	if sb == nil {
		return nil
	}
	if geom.IsGeometryEmpty(sb.Geometry) {
		return nil
	}
	extent, err := general.NewExtentFromGeometry(sb.Geometry)
	if err != nil {
		return nil
	}
	return extent
}

func (sb *StandardBinary) Value() (driver.Value, error) {
	if sb == nil {
		return nil, errors.New("nil")
	}
	return sb.Encode()
}

func (sb *StandardBinary) Scan(value interface{}) error {
	if sb == nil {
		return errors.New("nil")
	}
	data, ok := value.([]byte)
	if !ok {
		return errors.New("only support byte slice for Geometry")
	}
	sb1, err := DecodeGeometry(data)
	if err != nil {
		return err
	}
	sb.Header = sb1.Header
	sb.SRSID = sb1.SRSID
	sb.Geometry = sb1.Geometry
	return nil
}
