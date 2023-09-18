package snowflakepro

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"time"
)

/*
A SFID is a 16 byte snowflake  Sortable Identifier

The components are encoded as 16 octets.
Each component is encoded with the MSB first (network byte order).

0                   1                   2                   3
0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      32_bit_uint_time_high                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     16_bit_uint_time_low      |       16_bit_uint_node_id     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       32_bit_uint_random                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     8_bit_uint_random |          24_bit_uint_sn               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*/
type SFID [16]byte

var (
	// ErrDataSize is returned when parsing or unmarshaling SFIDs with the wrong
	// data size.
	ErrDataSize = errors.New("SFID: bad data size when unmarshaling")

	// ErrInvalidCharacters is returned when parsing or unmarshaling SFIDs with
	// invalid Base32 encodings.
	ErrInvalidCharacters = errors.New("SFID: bad data characters when unmarshaling")

	// ErrBufferSize is returned when marshalling SFIDs to a buffer of insufficient
	// size.
	ErrBufferSize = errors.New("SFID: bad buffer size when marshaling")

	// ErrBigTime is returned when constructing a SFID with a time that is larger
	// than MaxTime.
	ErrBigTime = errors.New("SFID: time too big")

	ErrBigNonce = errors.New("SFID: nonce too big")
	ErrBigSN    = errors.New("SFID: sn too big")

	// ErrOverflow is returned when unmarshaling a SFID whose first character is
	// larger than 7, thereby exceeding the valid bit depth of 128.
	ErrOverflow = errors.New("SFID: overflow when unmarshaling")

	// ErrScanValue is returned when the value passed to scan cannot be unmarshaled
	// into the SFID.
	ErrScanValue = errors.New("SFID: source value must be a string or byte slice")
)

// Parse parses an encoded SFID, returning an error in case of failure.
//
// ErrDataSize is returned if the len(SFID) is different from an encoded
// SFID's length. Invalid encodings produce undefined SFIDs. For a version that
// returns an error instead, see ParseStrict.
func Parse(SFID string) (id SFID, err error) {
	return id, parse([]byte(SFID), false, &id)
}

// ParseStrict parses an encoded SFID, returning an error in case of failure.
//
// It is like Parse, but additionally validates that the parsed SFID consists
// only of valid base32 characters. It is slightly slower than Parse.
//
// ErrDataSize is returned if the len(SFID) is different from an encoded
// SFID's length. Invalid encodings return ErrInvalidCharacters.
func ParseStrict(SFID string) (id SFID, err error) {
	return id, parse([]byte(SFID), true, &id)
}

func parse(v []byte, strict bool, id *SFID) error {
	// Check if a base32 encoded SFID is the right length.
	if len(v) != EncodedSize {
		return ErrDataSize
	}

	// Check if all the characters in a base32 encoded SFID are part of the
	// expected base32 character set.
	if strict &&
		(dec[v[0]] == 0xFF ||
			dec[v[1]] == 0xFF ||
			dec[v[2]] == 0xFF ||
			dec[v[3]] == 0xFF ||
			dec[v[4]] == 0xFF ||
			dec[v[5]] == 0xFF ||
			dec[v[6]] == 0xFF ||
			dec[v[7]] == 0xFF ||
			dec[v[8]] == 0xFF ||
			dec[v[9]] == 0xFF ||
			dec[v[10]] == 0xFF ||
			dec[v[11]] == 0xFF ||
			dec[v[12]] == 0xFF ||
			dec[v[13]] == 0xFF ||
			dec[v[14]] == 0xFF ||
			dec[v[15]] == 0xFF ||
			dec[v[16]] == 0xFF ||
			dec[v[17]] == 0xFF ||
			dec[v[18]] == 0xFF ||
			dec[v[19]] == 0xFF ||
			dec[v[20]] == 0xFF ||
			dec[v[21]] == 0xFF ||
			dec[v[22]] == 0xFF ||
			dec[v[23]] == 0xFF ||
			dec[v[24]] == 0xFF ||
			dec[v[25]] == 0xFF) {
		return ErrInvalidCharacters
	}

	// Check if the first character in a base32 encoded SFID will overflow. This
	// happens because the base32 representation encodes 130 bits, while the
	// SFID is only 128 bits.
	//
	// See https://github.com/oklog/SFID/issues/9 for details.
	if v[0] > '7' {
		return ErrOverflow
	}

	// Use an optimized unrolled loop (from https://github.com/RobThree/NSFID)
	// to decode a base32 SFID.

	// 6 bytes timestamp (48 bits)
	(*id)[0] = (dec[v[0]] << 5) | dec[v[1]]
	(*id)[1] = (dec[v[2]] << 3) | (dec[v[3]] >> 2)
	(*id)[2] = (dec[v[3]] << 6) | (dec[v[4]] << 1) | (dec[v[5]] >> 4)
	(*id)[3] = (dec[v[5]] << 4) | (dec[v[6]] >> 1)
	(*id)[4] = (dec[v[6]] << 7) | (dec[v[7]] << 2) | (dec[v[8]] >> 3)
	(*id)[5] = (dec[v[8]] << 5) | dec[v[9]]

	// 10 bytes of others (80 bits)
	(*id)[6] = (dec[v[10]] << 3) | (dec[v[11]] >> 2)
	(*id)[7] = (dec[v[11]] << 6) | (dec[v[12]] << 1) | (dec[v[13]] >> 4)
	(*id)[8] = (dec[v[13]] << 4) | (dec[v[14]] >> 1)
	(*id)[9] = (dec[v[14]] << 7) | (dec[v[15]] << 2) | (dec[v[16]] >> 3)
	(*id)[10] = (dec[v[16]] << 5) | dec[v[17]]
	(*id)[11] = (dec[v[18]] << 3) | dec[v[19]]>>2
	(*id)[12] = (dec[v[19]] << 6) | (dec[v[20]] << 1) | (dec[v[21]] >> 4)
	(*id)[13] = (dec[v[21]] << 4) | (dec[v[22]] >> 1)
	(*id)[14] = (dec[v[22]] << 7) | (dec[v[23]] << 2) | (dec[v[24]] >> 3)
	(*id)[15] = (dec[v[24]] << 5) | dec[v[25]]

	return nil
}

// MustParse is a convenience function equivalent to Parse that panics on failure
// instead of returning an error.
func MustParse(SFID string) SFID {
	id, err := Parse(SFID)
	if err != nil {
		panic(err)
	}
	return id
}

// MustParseStrict is a convenience function equivalent to ParseStrict that
// panics on failure instead of returning an error.
func MustParseStrict(SFID string) SFID {
	id, err := ParseStrict(SFID)
	if err != nil {
		panic(err)
	}
	return id
}

// Bytes returns bytes slice representation of SFID.
func (id SFID) Bytes() []byte {
	return id[:]
}

// String returns a lexicographically sortable string encoded SFID
// (26 characters, non-standard base 32) e.g. 01AN4Z07BY79KA1307SR9X4MV3.
// Format: tttttttttteeeeeeeeeeeeeeee where t is time and e is entropy.
func (id SFID) String() string {
	SFID := make([]byte, EncodedSize)
	_ = id.MarshalTextTo(SFID)
	return string(SFID)
}

// MarshalBinary implements the encoding.BinaryMarshaler interface by
// returning the SFID as a byte slice.
func (id SFID) MarshalBinary() ([]byte, error) {
	SFID := make([]byte, len(id))
	return SFID, id.MarshalBinaryTo(SFID)
}

// MarshalBinaryTo writes the binary encoding of the SFID to the given buffer.
// ErrBufferSize is returned when the len(dst) != 16.
func (id SFID) MarshalBinaryTo(dst []byte) error {
	if len(dst) != len(id) {
		return ErrBufferSize
	}

	copy(dst, id[:])
	return nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface by
// copying the passed data and converting it to a SFID. ErrDataSize is
// returned if the data length is different from SFID length.
func (id *SFID) UnmarshalBinary(data []byte) error {
	if len(data) != len(*id) {
		return ErrDataSize
	}

	copy((*id)[:], data)
	return nil
}

// Encoding is the base 32 encoding alphabet used in SFID strings.
const Encoding = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// MarshalText implements the encoding.TextMarshaler interface by
// returning the string encoded SFID.
func (id SFID) MarshalText() ([]byte, error) {
	SFID := make([]byte, EncodedSize)
	return SFID, id.MarshalTextTo(SFID)
}

// MarshalTextTo writes the SFID as a string to the given buffer.
// ErrBufferSize is returned when the len(dst) != 26.
func (id SFID) MarshalTextTo(dst []byte) error {
	// Optimized unrolled loop ahead.
	// From https://github.com/RobThree/NSFID

	if len(dst) != EncodedSize {
		return ErrBufferSize
	}

	// 10 byte timestamp
	dst[0] = Encoding[(id[0]&224)>>5]
	dst[1] = Encoding[id[0]&31]
	dst[2] = Encoding[(id[1]&248)>>3]
	dst[3] = Encoding[((id[1]&7)<<2)|((id[2]&192)>>6)]
	dst[4] = Encoding[(id[2]&62)>>1]
	dst[5] = Encoding[((id[2]&1)<<4)|((id[3]&240)>>4)]
	dst[6] = Encoding[((id[3]&15)<<1)|((id[4]&128)>>7)]
	dst[7] = Encoding[(id[4]&124)>>2]
	dst[8] = Encoding[((id[4]&3)<<3)|((id[5]&224)>>5)]
	dst[9] = Encoding[id[5]&31]

	// 16 bytes of entropy
	dst[10] = Encoding[(id[6]&248)>>3]
	dst[11] = Encoding[((id[6]&7)<<2)|((id[7]&192)>>6)]
	dst[12] = Encoding[(id[7]&62)>>1]
	dst[13] = Encoding[((id[7]&1)<<4)|((id[8]&240)>>4)]
	dst[14] = Encoding[((id[8]&15)<<1)|((id[9]&128)>>7)]
	dst[15] = Encoding[(id[9]&124)>>2]
	dst[16] = Encoding[((id[9]&3)<<3)|((id[10]&224)>>5)]
	dst[17] = Encoding[id[10]&31]
	dst[18] = Encoding[(id[11]&248)>>3]
	dst[19] = Encoding[((id[11]&7)<<2)|((id[12]&192)>>6)]
	dst[20] = Encoding[(id[12]&62)>>1]
	dst[21] = Encoding[((id[12]&1)<<4)|((id[13]&240)>>4)]
	dst[22] = Encoding[((id[13]&15)<<1)|((id[14]&128)>>7)]
	dst[23] = Encoding[(id[14]&124)>>2]
	dst[24] = Encoding[((id[14]&3)<<3)|((id[15]&224)>>5)]
	dst[25] = Encoding[id[15]&31]

	return nil
}

// Byte to index table for O(1) lookups when unmarshaling.
// We use 0xFF as sentinel value for invalid indexes.
var dec = [...]byte{
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x01,
	0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
	0x0F, 0x10, 0x11, 0xFF, 0x12, 0x13, 0xFF, 0x14, 0x15, 0xFF,
	0x16, 0x17, 0x18, 0x19, 0x1A, 0xFF, 0x1B, 0x1C, 0x1D, 0x1E,
	0x1F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0A, 0x0B, 0x0C,
	0x0D, 0x0E, 0x0F, 0x10, 0x11, 0xFF, 0x12, 0x13, 0xFF, 0x14,
	0x15, 0xFF, 0x16, 0x17, 0x18, 0x19, 0x1A, 0xFF, 0x1B, 0x1C,
	0x1D, 0x1E, 0x1F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
}

// EncodedSize is the length of a text encoded SFID.
const EncodedSize = 26

// UnmarshalText implements the encoding.TextUnmarshaler interface by
// parsing the data as string encoded SFID.
//
// ErrDataSize is returned if the len(v) is different from an encoded
// SFID's length. Invalid encodings produce undefined SFIDs.
func (id *SFID) UnmarshalText(v []byte) error {
	return parse(v, false, id)
}

// Time returns the Unix time in milliseconds encoded in the SFID.
// Use the top level Time function to convert the returned value to
// a time.Time.
func (id SFID) Time() uint64 {
	return uint64(id[5]) | uint64(id[4])<<8 |
		uint64(id[3])<<16 | uint64(id[2])<<24 |
		uint64(id[1])<<32 | uint64(id[0])<<40
}

// Timestamp returns the time encoded in the SFID as a time.Time.
func (id SFID) Timestamp() time.Time {
	ms := id.Time()
	s := int64(ms / 1e3)
	ns := int64((ms % 1e3) * 1e6)
	return time.Unix(s, ns)
}

// Node return node id in the SFID
func (id SFID) Node() uint16 {
	return uint16((id)[6])<<8 | uint16((id)[7])
}

func (id SFID) Nonce() uint64 {
	return uint64((id)[8])<<32 | uint64((id)[9])<<24 | uint64((id)[10])<<16 | uint64((id)[11])<<8 | uint64((id)[12])
}

func (id SFID) SN() uint32 {
	return uint32((id)[13])<<16 | uint32((id)[14])<<8 | uint32((id)[15])

}

// maxTime is the maximum Unix time in milliseconds that can be
// represented in a SFID.
var maxTime = SFID{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}.Time()

// MaxTime returns the maximum Unix time in milliseconds that
// can be encoded in a SFID.
func MaxTime() uint64 { return maxTime }

// SetTime sets the time component of the SFID to the given Unix time
// in milliseconds.
func (id *SFID) SetTime(ms uint64) error {
	if ms > maxTime {
		return ErrBigTime
	}

	(*id)[0] = byte(ms >> 40)
	(*id)[1] = byte(ms >> 32)
	(*id)[2] = byte(ms >> 24)
	(*id)[3] = byte(ms >> 16)
	(*id)[4] = byte(ms >> 8)
	(*id)[5] = byte(ms)
	return nil
}

func (id *SFID) SetNode(node uint16) {
	(*id)[6] = byte(node >> 8)
	(*id)[7] = byte(node)
}

const MaxNonce = 0xFFFFFFFFFF
const maxSN = (uint32(1) << 24) - 1

func (id *SFID) SetNonce(nonce uint64) error {
	if nonce > MaxNonce {
		return ErrBigNonce
	}
	(*id)[8] = byte(nonce >> 32)
	(*id)[9] = byte(nonce >> 24)
	(*id)[10] = byte(nonce >> 16)
	(*id)[11] = byte(nonce >> 8)
	(*id)[12] = byte(nonce)
	return nil
}

func (id *SFID) SetSN(sn uint32) error {
	if sn > maxSN {
		return ErrBigSN
	}

	(*id)[13] = byte(sn >> 16)
	(*id)[14] = byte(sn >> 8)
	(*id)[15] = byte(sn)
	return nil
}

// Compare returns an integer comparing id and other lexicographically.
// The result will be 0 if id==other, -1 if id < other, and +1 if id > other.
func (id SFID) Compare(other SFID) int {
	return bytes.Compare(id[:], other[:])
}

// Scan implements the sql.Scanner interface. It supports scanning
// a string or byte slice.
func (id *SFID) Scan(src interface{}) error {
	switch x := src.(type) {
	case nil:
		return nil
	case string:
		return id.UnmarshalText([]byte(x))
	case []byte:
		return id.UnmarshalBinary(x)
	}

	return ErrScanValue
}

// Value implements the sql/driver.Valuer interface, returning the SFID as a
// slice of bytes, by invoking MarshalBinary. If your use case requires a string
// representation instead, you can create a wrapper type that calls String()
// instead.
//
//	type stringValuer SFID.SFID
//
//	func (v stringValuer) Value() (driver.Value, error) {
//	    return SFID.SFID(v).String(), nil
//	}
//
//	// Example usage.
//	db.Exec("...", stringValuer(id))
//
// All valid SFIDs, including zero-value SFIDs, return a valid Value with a nil
// error. If your use case requires zero-value SFIDs to return a non-nil error,
// you can create a wrapper type that special-cases this behavior.
//
//	var zeroValueSFID SFID.SFID
//
//	type invalidZeroValuer SFID.SFID
//
//	func (v invalidZeroValuer) Value() (driver.Value, error) {
//	    if SFID.SFID(v).Compare(zeroValueSFID) == 0 {
//	        return nil, fmt.Errorf("zero value")
//	    }
//	    return SFID.SFID(v).Value()
//	}
//
//	// Example usage.
//	db.Exec("...", invalidZeroValuer(id))
func (id SFID) Value() (driver.Value, error) {
	return id.MarshalBinary()
}
