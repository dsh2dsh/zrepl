package endpoint

import (
	"math"
	"runtime/debug"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dsh2dsh/zrepl/internal/config/env"
)

func TestCreateTXGRange(t *testing.T) {
	type testCaseExpectation struct {
		input  uint64
		expect bool
	}
	type testCase struct {
		name                     string
		config                   *CreateTXGRange
		configAllowZeroCreateTXG bool
		expectInvalid            bool
		expectString             string
		expect                   []testCaseExpectation
	}

	tcs := []testCase{
		{
			name:          "unbounded",
			expectInvalid: false,
			config: &CreateTXGRange{
				Since: nil,
				Until: nil,
			},
			expectString: "~,~",
			expect: []testCaseExpectation{
				{0, true},
				{math.MaxUint64, true},
				{1, true},
				{math.MaxUint64 - 1, true},
			},
		},
		{
			name:          "wrong order obvious",
			expectInvalid: true,
			config: &CreateTXGRange{
				Since: &CreateTXGRangeBound{23, true},
				Until: &CreateTXGRangeBound{20, true},
			},
			expectString: "[23,20]",
		},
		{
			name:          "wrong order edge-case could also be empty",
			expectInvalid: true,
			config: &CreateTXGRange{
				Since: &CreateTXGRangeBound{23, false},
				Until: &CreateTXGRangeBound{22, true},
			},
			expectString: "(23,22]",
		},
		{
			name:          "empty",
			expectInvalid: true,
			config: &CreateTXGRange{
				Since: &CreateTXGRangeBound{2, false},
				Until: &CreateTXGRangeBound{2, false},
			},
			expectString: "(2,2)",
		},
		{
			name:          "inclusive-since-exclusive-until",
			expectInvalid: false,
			config: &CreateTXGRange{
				Since: &CreateTXGRangeBound{2, true},
				Until: &CreateTXGRangeBound{5, false},
			},
			expectString: "[2,5)",
			expect: []testCaseExpectation{
				{0, false},
				{1, false},
				{2, true},
				{3, true},
				{4, true},
				{5, false},
				{6, false},
			},
		},
		{
			name:          "exclusive-since-inclusive-until",
			expectInvalid: false,
			config: &CreateTXGRange{
				Since: &CreateTXGRangeBound{2, false},
				Until: &CreateTXGRangeBound{5, true},
			},
			expectString: "(2,5]",
			expect: []testCaseExpectation{
				{0, false},
				{1, false},
				{2, false},
				{3, true},
				{4, true},
				{5, true},
				{6, false},
			},
		},
		{
			name:          "zero-createtxg-not-allowed-because-likely-programmer-error",
			expectInvalid: true,
			config: &CreateTXGRange{
				Since: nil,
				Until: &CreateTXGRangeBound{0, true},
			},
			expectString: "~,0]",
		},
		{
			name:          "half-open-no-until",
			expectInvalid: false,
			config: &CreateTXGRange{
				Since: &CreateTXGRangeBound{2, false},
				Until: nil,
			},
			expectString: "(2,~",
			expect: []testCaseExpectation{
				{0, false},
				{1, false},
				{2, false},
				{3, true},
				{4, true},
				{5, true},
				{6, true},
			},
		},
		{
			name:          "half-open-no-since",
			expectInvalid: false,
			config: &CreateTXGRange{
				Since: nil,
				Until: &CreateTXGRangeBound{4, true},
			},
			expectString: "~,4]",
			expect: []testCaseExpectation{
				{0, true},
				{1, true},
				{2, true},
				{3, true},
				{4, true},
				{5, false},
			},
		},
		{
			name:          "edgeSince",
			expectInvalid: false,
			config: &CreateTXGRange{
				Since: &CreateTXGRangeBound{math.MaxUint64, true},
				Until: nil,
			},
			expectString: "[18446744073709551615,~",
			expect: []testCaseExpectation{
				{math.MaxUint64, true},
				{math.MaxUint64 - 1, false},
				{0, false},
				{1, false},
			},
		},
		{
			name:          "edgeSinceNegative",
			expectInvalid: true,
			config: &CreateTXGRange{
				Since: &CreateTXGRangeBound{math.MaxUint64, false},
				Until: nil,
			},
			expectString: "(18446744073709551615,~",
		},
		{
			name:          "edgeUntil",
			expectInvalid: false,
			config: &CreateTXGRange{
				Until: &CreateTXGRangeBound{0, true},
			},
			configAllowZeroCreateTXG: true,
			expectString:             "~,0]",
			expect: []testCaseExpectation{
				{0, true},
				{math.MaxUint64, false},
				{1, false},
			},
		},
		{
			name:                     "edgeUntilNegative",
			expectInvalid:            true,
			configAllowZeroCreateTXG: true,
			config: &CreateTXGRange{
				Until: &CreateTXGRangeBound{0, false},
			},
			expectString: "~,0)",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			require.NotEqual(t, tc.expectInvalid, (len(tc.expect) > 0), "invalid test config: must either expect invalid or have expectations: %s", tc.name)
			require.NotEmpty(t, tc.expectString)
			assert.Equal(t, tc.expectString, tc.config.String())

			save := env.Values.CreatetxgRangeBoundAllow
			env.Values.CreatetxgRangeBoundAllow = tc.configAllowZeroCreateTXG
			defer func() {
				env.Values.CreatetxgRangeBoundAllow = save
			}()

			if tc.expectInvalid {
				t.Run(tc.name, func(t *testing.T) {
					assert.Error(t, tc.config.Validate())
				})
			} else {
				for i, e := range tc.expect {
					t.Run(strconv.Itoa(i), func(t *testing.T) {
						defer func() {
							v := recover()
							if v != nil {
								t.Fatalf("should not panic: %T %v\n%s", v, v, debug.Stack())
							}
						}()
						assert.Equal(t, e.expect, tc.config.Contains(e.input))
					})
				}
			}
		})
	}
}
