package datasizeunit

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Bits struct {
	bits float64
}

func (b Bits) ToBits() float64    { return b.bits }
func (b Bits) ToBytes() float64   { return b.bits / 8 }
func FromBytesInt64(i int64) Bits { return Bits{float64(i) * 8} }

var datarateRegex = regexp.MustCompile(`^([-0-9\.]*)\s*(bit|(|K|Ki|M|Mi|G|Gi|T|Ti)([bB]))$`)

func (r *Bits) UnmarshalJSON(b []byte) error {
	s := string(b)

	genericErr := func(err error) error {
		var buf strings.Builder
		fmt.Fprintf(&buf, "cannot parse %q using regex %s", s, datarateRegex)
		if err != nil {
			fmt.Fprintf(&buf, ": %s", err)
		}
		return errors.New(buf.String())
	}

	match := datarateRegex.FindStringSubmatch(s)
	if match == nil {
		return genericErr(nil)
	}

	bps, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return genericErr(err)
	}

	if match[2] == "bit" {
		if math.Round(bps) != bps {
			return genericErr(errors.New("unit bit must be an integer value"))
		}
		r.bits = bps
		return nil
	}

	factorMap := map[string]uint64{
		"": 1,

		"K": 1e3,
		"M": 1e6,
		"G": 1e9,
		"T": 1e12,

		"Ki": 1 << 10,
		"Mi": 1 << 20,
		"Gi": 1 << 30,
		"Ti": 1 << 40,
	}
	factor, ok := factorMap[match[3]]
	if !ok {
		panic(match)
	}

	baseUnitFactorMap := map[string]uint64{
		"b": 1,
		"B": 8,
	}
	baseUnitFactor, ok := baseUnitFactorMap[match[4]]
	if !ok {
		panic(match)
	}

	r.bits = bps * float64(factor) * float64(baseUnitFactor)
	return nil
}

var _ yaml.Unmarshaler = (*Bits)(nil)

func (r *Bits) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	return r.UnmarshalJSON([]byte(s))
}
