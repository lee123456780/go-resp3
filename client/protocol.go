/*
Copyright 2019 Stefan Miller

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strconv"

	"go-resp3/client/internal/monitor"
)

const (
	redisVersion = "3"
)

// Redis reply constants.
const (
	ReplyOK     = "OK"
	ReplyQueued = "QUEUED"
)

const (
	blobStringType             = '$'
	simpleStringType           = '+'
	simpleErrorType            = '-'
	numberType                 = ':'
	nullType                   = '_'
	doubleType                 = ','
	booleanType                = '#'
	blobErrorType              = '!'
	verbatimStringType         = '='
	bigNumberType              = '('
	arrayType                  = '*'
	mapType                    = '%'
	setType                    = '~'
	attributeType              = '|'
	pushType                   = '>'
	streamedType               = '?'
	streamedStringToken        = ';'
	streamedDataTypeTerminator = '.'

	booleanTrue  = 't'
	booleanFalse = 'f'

	lf = '\r'
	nl = '\n'

	lineBreak = string(lf) + string(nl)
)

const (
	pubSubSubscribe    = "subscribe"
	pubSubUnsubscribe  = "unsubscribe"
	pubSubPsubscribe   = "psubscribe"
	pubSubPunsubscribe = "punsubscribe"
	pubSubMessage      = "message"
	pubSubPMessage     = "pmessage"
	invalidateMessage  = "invalidate"
)

// An InvalidTypeError is raised by encoding
// an unsupported value type.
type InvalidTypeError struct {
	Value interface{}
}

func (e *InvalidTypeError) Error() string {
	return fmt.Sprintf("encode: unsupported type %[1]T value %[1]v", e.Value)
}

// An UnexpectedCharacterError is raised by decoding
// an unexpected character.
type UnexpectedCharacterError struct {
	ActChar byte
	ExpChar byte
}

func (e *UnexpectedCharacterError) Error() string {
	return fmt.Sprintf("decode: unsupported character %s expected %s"+string(e.ExpChar), e.ActChar, e.ExpChar)
}

// An InvalidNumberError is raised by decoding
// a number including unexpected characters.
type InvalidNumberError struct {
	Value string
}

func (e *InvalidNumberError) Error() string {
	return fmt.Sprintf("decode: invalid number %s", e.Value)
}

// An InvalidDoubleError is raised by decoding
// a double including unexpected characters.
type InvalidDoubleError struct {
	Value string
}

func (e *InvalidDoubleError) Error() string {
	return fmt.Sprintf("decode: invalid double %s", e.Value)
}

// An InvalidBigNumberError is raised by decoding
// a bignumber including unexpected characters.
type InvalidBigNumberError struct {
	Value string
}

func (e *InvalidBigNumberError) Error() string {
	return fmt.Sprintf("decode: invalid bignumber %s", e.Value)
}

// StringPtr returns a pointer to the string parameter.
func StringPtr(s string) *string {
	return &s
}

// Int64Ptr returns a pointer to the int64 parameter.
func Int64Ptr(i int64) *int64 {
	return &i
}

// check if objects implement interfaces
var _ Encoder = (*encode)(nil)
var _ Decoder = (*decode)(nil)

// Encoder is the interface that wraps the Encode method.
//
// Encode is not a general redis encoder but encodes redis commands only.
// Redis commands are send as ARRAY of BULK STRINGS.
// Encode is encoding all elements of v to BULK STRINGS.
// In case an element is a not suported type, Encode will panic.
type Encoder interface {
	Encode(values ...interface{}) error
	Flush() error
}

// NewEncoder returns an Encoder for redis commands.
func NewEncoder(w io.Writer) Encoder {
	return newEncode(w)
}

type encode struct {
	wr  io.Writer
	w   *bytes.Buffer
	err error
	cnt int
}

const preBytes = 23

func newEncode(wr io.Writer) *encode {
	return &encode{
		wr: wr,
		w:  bytes.NewBuffer(make([]byte, preBytes)), // buffer for 'array' prefix
	}
}

func (e *encode) Encode(values ...interface{}) error {
	for _, v := range values {
		err := e.encode(v)
		if err != nil {
			e.err = err
			return err
		}
		e.cnt++
	}
	return nil
}

func (e *encode) reset() {
	e.err = nil
	e.cnt = 0
	e.w.Truncate(preBytes)
}

func (e *encode) bytes() []byte {
	b := e.w.Bytes()
	b[preBytes-2], b[preBytes-1] = lf, nl
	size := strconv.Itoa(e.cnt) // array size
	pos := preBytes - (len(size) + 2)
	copy(b[pos:], size)
	b[pos-1] = arrayType
	return b[pos-1:]
}

func (e *encode) Flush() error {
	if e.err != nil {
		return e.err
	}
	_, err := e.wr.Write(e.bytes())
	e.reset()
	return err
}

func (e *encode) encode(v interface{}) error {
	switch v := v.(type) {
	case string:
		e.encodeString(v)
	case []byte:
		e.encodeString(string(v))
	case bool:
		e.encodeString(strconv.FormatBool(v))
	case int:
		e.encodeString(strconv.FormatInt(int64(v), 10))
	case uint:
		e.encodeString(strconv.FormatUint(uint64(v), 10))
	case float32:
		e.encodeString(strconv.FormatFloat(float64(v), 'g', -1, 64))
	case float64:
		e.encodeString(strconv.FormatFloat(v, 'g', -1, 64))
	case Zfloat64:
		e.encodeString(v.String())
	default:
		return e.encodeValue(reflect.ValueOf(v))
	}
	return nil
}

func (e *encode) encodeValue(v reflect.Value) error {
	switch v.Kind() {
	case reflect.String:
		e.encodeString(v.String())
	case reflect.Bool:
		e.encodeString(strconv.FormatBool(v.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		e.encodeString(strconv.FormatInt(v.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		e.encodeString(strconv.FormatUint(v.Uint(), 10))
	case reflect.Float32, reflect.Float64:
		e.encodeString(strconv.FormatFloat(v.Float(), 'g', -1, 64))
	case reflect.Interface:
		e.encodeValue(v.Elem())
	case reflect.Ptr:
		e.encodeValue(v.Elem())
	default:
		return &InvalidTypeError{v}
	}
	return nil
}

func (e *encode) encodeString(s string) {
	e.w.WriteByte(blobStringType)
	e.w.WriteString(strconv.Itoa(len(s)))
	e.w.WriteString(lineBreak)
	e.w.WriteString(s)
	e.w.WriteString(lineBreak)
}

// Decoder is the interface that wraps the Decode method.
type Decoder interface {
	Decode() (value RedisValue, err error)
}

// NewDecoder returns a Decoder for Redis results.
func NewDecoder(r io.Reader) Decoder {
	return &decode{r: bufio.NewReader(r)}
}

type decode struct {
	r *bufio.Reader
}

func (d *decode) discardByte(db byte) error {
	b, err := d.r.ReadByte()
	if err != nil {
		return err
	}
	if b != db {
		return &UnexpectedCharacterError{ActChar: b, ExpChar: db}
	}
	return nil
}

func (d *decode) readLineBreak() error {
	if err := d.discardByte(lf); err != nil {
		return err
	}
	if err := d.discardByte(nl); err != nil {
		return err
	}
	return nil
}

func (d *decode) readByte() (byte, error) {
	b, err := d.r.ReadByte()
	if err != nil {
		return 0, err
	}
	if err := d.readLineBreak(); err != nil {
		return 0, err
	}
	return b, nil
}

func (d *decode) readBytes(b []byte) error {
	if _, err := io.ReadFull(d.r, b); err != nil {
		return err
	}
	if err := d.readLineBreak(); err != nil {
		return err
	}
	return nil
}

func (d *decode) readString() (string, error) {
	s, err := d.r.ReadString(lf)
	if err != nil {
		return "", err
	}
	if err := d.discardByte(nl); err != nil {
		return "", err
	}
	return s[:len(s)-1], nil
}

func (d *decode) readSize() (int64, bool, error) {
	s, err := d.readString()
	if err != nil {
		return 0, false, err
	}
	if len(s) == 1 && s[0] == streamedType {
		return 0, true, nil
	}
	size, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false, &InvalidNumberError{Value: s}
	}
	return size, false, nil
}

func (d *decode) Decode() (RedisValue, error) {
	b, err := d.r.ReadByte()
	if err != nil {
		return invalidValue, err
	}

	switch b {

	case blobErrorType:
		size, err := d.decodeNumber()
		if err != nil {
			return invalidValue, err
		}
		v, err := d.decodeBlobString(size)
		return RedisValue{RkError, newRedisError(v), nil}, err

	case simpleErrorType:
		v, err := d.decodeString()
		return RedisValue{RkError, newRedisError(v), nil}, err

	default:
		return d.decodeValue(b)

	}
}

func (d *decode) decodeValue(b byte) (RedisValue, error) {

	var attr Map

	if b == attributeType {
		size, err := d.decodeNumber()
		if err != nil {
			return invalidValue, err
		}
		attr, err = d.decodeMap(size)
		if err != nil {
			return invalidValue, err
		}
		b, err = d.r.ReadByte()
		if err != nil {
			return invalidValue, err
		}

	}

	switch b {

	case nullType:
		err := d.readLineBreak()
		return RedisValue{RkNull, nil, attr}, err

	case blobStringType:
		size, streamed, err := d.readSize()
		if err != nil {
			return invalidValue, err
		}

		var v string

		if streamed {
			v, err = d.decodeStreamedString()
		} else {
			v, err = d.decodeBlobString(size)
		}

		if monitor.IsNotification(v) {
			return RedisValue{RkPush, monitor.Parse(v), attr}, err
		}
		return RedisValue{RkString, v, attr}, err

	case verbatimStringType:
		size, streamed, err := d.readSize()
		if err != nil {
			return invalidValue, err
		}

		var v string

		if streamed {
			v, err = d.decodeStreamedString()
		} else {
			v, err = d.decodeBlobString(size)
		}

		if monitor.IsNotification(v) {
			return RedisValue{RkPush, monitor.Parse(v), attr}, err
		}
		return RedisValue{RkVerbatimString, VerbatimString(v), attr}, err

	case simpleStringType:
		v, err := d.decodeString()
		if monitor.IsNotification(v) {
			return RedisValue{RkPush, monitor.Parse(v), attr}, err
		}
		return RedisValue{RkString, v, attr}, err

	case numberType:
		v, err := d.decodeNumber()
		return RedisValue{RkNumber, v, attr}, err

	case doubleType:
		v, err := d.decodeDouble()
		return RedisValue{RkDouble, v, attr}, err

	case bigNumberType:
		v, err := d.decodeBigNumber()
		return RedisValue{RkBigNumber, v, attr}, err

	case booleanType:
		v, err := d.decodeBoolean()
		return RedisValue{RkBoolean, v, attr}, err

	case arrayType:
		size, streamed, err := d.readSize()
		if err != nil {
			return invalidValue, err
		}

		var v Slice

		if streamed {
			v, err = d.decodeStreamedSlice()
		} else {
			v, err = d.decodeSlice(size)
		}
		return RedisValue{RkSlice, v, attr}, err

	case pushType: // like not streamed array
		size, err := d.decodeNumber()
		if err != nil {
			return invalidValue, err
		}
		v, err := d.decodePushSlice(size)
		return RedisValue{RkPush, v, attr}, err

	case mapType:
		size, streamed, err := d.readSize()
		if err != nil {
			return invalidValue, err
		}

		var v Map

		if streamed {
			v, err = d.decodeStreamedMap()
		} else {
			v, err = d.decodeMap(size)
		}
		return RedisValue{RkMap, v, attr}, err

	case setType:
		size, streamed, err := d.readSize()
		if err != nil {
			return invalidValue, err
		}

		var v Set

		if streamed {
			v, err = d.decodeStreamedSet()
		} else {
			v, err = d.decodeSet(size)
		}
		return RedisValue{RkSet, v, attr}, err

	default:
		panic("unsupported redis type " + string(b))
	}
}

func (d *decode) decodeBlobString(size int64) (string, error) {
	p := getBuffer(size)
	defer freeBuffer(p)

	if err := d.readBytes(p); err != nil {
		return "", err
	}

	return string(p), nil
}

func (d *decode) decodeStreamedString() (string, error) {
	p := getBuffer(0)
	defer freeBuffer(p)

	part := getBuffer(0)
	defer freeBuffer(part)

	for {
		b, err := d.r.ReadByte()
		if err != nil {
			return "", err
		}
		if b != streamedStringToken {
			return "", &UnexpectedCharacterError{ActChar: b, ExpChar: streamedStringToken}
		}
		l, err := d.decodeNumber()
		if err != nil {
			return "", err
		}
		if l == 0 {
			break
		}

		part = resizeBuffer(part, l)

		if err := d.readBytes(part); err != nil {
			return "", err
		}

		p = append(p, part...)
	}
	return string(p), nil
}

func (d *decode) decodeString() (string, error) {
	s, err := d.readString()
	if err != nil {
		return "", err
	}
	return s, nil
}

func (d *decode) decodeNumber() (int64, error) {
	s, err := d.readString()
	if err != nil {
		return 0, err
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, &InvalidNumberError{Value: s}
	}
	return i, nil
}

func (d *decode) decodeDouble() (float64, error) {
	s, err := d.readString()
	if err != nil {
		return 0, err
	}
	i, err := strconv.ParseFloat(s, 64) // supports inf, -inf
	if err != nil {
		return 0, &InvalidDoubleError{Value: s}
	}
	return i, nil
}

func (d *decode) decodeBigNumber() (*big.Int, error) {
	s, err := d.readString()
	if err != nil {
		return zeroInt, err
	}
	i, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return zeroInt, &InvalidBigNumberError{Value: s}
	}
	return i, nil
}

func (d *decode) decodeBoolean() (bool, error) {
	b, err := d.readByte()
	if err != nil {
		return false, err
	}
	return b == booleanTrue, nil
}

func (d *decode) decodeToString() (string, error) {
	b, err := d.r.ReadByte()
	if err != nil {
		return "", err
	}
	val, err := d.decodeValue(b)
	if err != nil {
		return "", err
	}
	return val.ToString()
}

func (d *decode) decodeToInt64() (int64, error) {
	b, err := d.r.ReadByte()
	if err != nil {
		return 0, err
	}
	val, err := d.decodeValue(b)
	if err != nil {
		return 0, err
	}
	return val.ToInt64()
}

func (d *decode) decodePushSlice(size int64) (interface{}, error) {
	kind, err := d.decodeToString()
	if err != nil {
		return nil, err
	}

	switch kind {
	case pubSubSubscribe, pubSubPsubscribe:
		channel, err := d.decodeToString()
		if err != nil {
			return nil, err
		}
		count, err := d.decodeToInt64()
		if err != nil {
			return nil, err
		}
		return subscribeNotification{channel: channel, count: count}, nil

	case pubSubUnsubscribe, pubSubPunsubscribe:
		channel, err := d.decodeToString()
		if err != nil {
			return nil, err
		}
		count, err := d.decodeToInt64()
		if err != nil {
			return nil, err
		}
		return unsubscribeNotification{channel: channel, count: count}, nil

	case pubSubMessage:
		channel, err := d.decodeToString()
		if err != nil {
			return nil, err
		}
		msg, err := d.decodeToString()
		if err != nil {
			return nil, err
		}
		return publishNotification{channel: channel, msg: msg}, nil

	case pubSubPMessage:
		pattern, err := d.decodeToString()
		if err != nil {
			return nil, err
		}
		channel, err := d.decodeToString()
		if err != nil {
			return nil, err
		}
		msg, err := d.decodeToString()
		if err != nil {
			return nil, err
		}
		return publishNotification{pattern: pattern, channel: channel, msg: msg}, nil

	case invalidateMessage:
		slot, err := d.decodeToInt64()
		if err != nil {
			return nil, err
		}
		return invalidateNotification(slot), nil

	default:
		values, err := d.decodeSlice(size - 1)
		if err != nil {
			return nil, err
		}
		return genericNotification{kind: kind, values: values}, nil

	}

}

func (d *decode) decodeSlice(size int64) (Slice, error) {
	s := make(Slice, size)
	for i := int64(0); i < size; i++ {
		b, err := d.r.ReadByte()
		if err != nil {
			return nil, err
		}
		val, err := d.decodeValue(b)
		if err != nil {
			return nil, err
		}
		s[i] = val
	}
	return s, nil
}

func (d *decode) decodeStreamedSlice() (Slice, error) {
	s := make(Slice, 0)
	for {
		b, err := d.r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == streamedDataTypeTerminator {
			break
		}
		val, err := d.decodeValue(b)
		if err != nil {
			return nil, err
		}
		s = append(s, val)
	}
	if err := d.readLineBreak(); err != nil {
		return nil, err
	}
	return s, nil
}

func (d *decode) decodeMap(size int64) (Map, error) {
	m := make(Map, size)
	for i := int64(0); i < size; i++ {
		b, err := d.r.ReadByte()
		if err != nil {
			return nil, err
		}
		key, err := d.decodeValue(b)
		if err != nil {
			return nil, err
		}
		b, err = d.r.ReadByte()
		if err != nil {
			return nil, err
		}
		val, err := d.decodeValue(b)
		if err != nil {
			return nil, err
		}
		m[i] = MapItem{key, val}
	}
	return m, nil
}

func (d *decode) decodeStreamedMap() (Map, error) {
	m := make(Map, 0)
	for {
		b, err := d.r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == streamedDataTypeTerminator {
			break
		}
		key, err := d.decodeValue(b)
		if err != nil {
			return nil, err
		}
		b, err = d.r.ReadByte()
		if err != nil {
			return nil, err
		}
		val, err := d.decodeValue(b)
		if err != nil {
			return nil, err
		}
		m = append(m, MapItem{key, val})
	}
	if err := d.readLineBreak(); err != nil {
		return nil, err
	}
	return m, nil
}

func (d *decode) decodeSet(size int64) (Set, error) {
	s := make(Set, size)
	for i := int64(0); i < size; i++ {
		b, err := d.r.ReadByte()
		if err != nil {
			return nil, err
		}
		val, err := d.decodeValue(b)
		if err != nil {
			return nil, err
		}
		s[i] = val
	}
	return s, nil
}

func (d *decode) decodeStreamedSet() (Set, error) {
	s := make(Set, 0)
	for {
		b, err := d.r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == streamedDataTypeTerminator {
			break
		}
		val, err := d.decodeValue(b)
		if err != nil {
			return nil, err
		}
		s = append(s, val)
	}
	if err := d.readLineBreak(); err != nil {
		return nil, err
	}
	return s, nil
}
