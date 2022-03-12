package collector

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_stringsContains(t *testing.T) {
	ss := []string{"first_example_string", "second_example_string", "third_example_string"}

	assert.True(t, stringsContains(ss, "first_example_string"))
	assert.False(t, stringsContains(ss, "unknown_string"))
	assert.False(t, stringsContains(nil, "example"))
}

func Test_semverStringToInt(t *testing.T) {
	testcases := []struct {
		valid   bool
		version string
		want    int
	}{
		{valid: true, version: "0.0.1-pre0", want: 1},
		{valid: true, version: "0.0.1", want: 1},
		{valid: true, version: "0.0.1.2", want: 1},
		{valid: true, version: "0.1.2", want: 102},
		{valid: true, version: "0.1.2-pre0", want: 102},
		{valid: true, version: "1.2.3", want: 10203},
		{valid: true, version: "1.2.3-pre0", want: 10203},
		{valid: true, version: "1.2.13", want: 10213},
		{valid: true, version: "1.2.13-pre0", want: 10213},
		{valid: true, version: "1.12.23", want: 11223},
		{valid: true, version: "1.12.23-pre0", want: 11223},
		{valid: true, version: "11.22.33", want: 112233},
		{valid: true, version: "11.22.33-pre0", want: 112233},
		{valid: false, version: "22.33"},
	}

	for _, tc := range testcases {
		got, err := semverStringToInt(tc.version)
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		} else {
			assert.Error(t, err)
		}
	}
}
