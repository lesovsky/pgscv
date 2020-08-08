package filter

import (
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
)

func TestDefaultFilters(t *testing.T) {
	var testcases = []struct {
		name string
		in   map[string]Filter
		want map[string]Filter
	}{
		{name: "empty map", in: map[string]Filter{}, want: map[string]Filter{
			"diskstats/device":  {Exclude: `^(ram|loop|fd|(h|s|v|xv)d[a-z]|nvme\d+n\d+p)\d+$`},
			"netdev/device":     {Exclude: `docker|virbr`},
			"filesystem/fstype": {Include: `^(ext3|ext4|xfs|btrfs)$`},
		}},
		{
			name: "defined filters",
			in: map[string]Filter{
				"diskstats/device":  {Include: "^(test123|example123)$"},
				"netdev/device":     {Include: "^(test456|example456)$"},
				"filesystem/fstype": {Exclude: "^(test789|example789)$"},
				"test/example":      {Exclude: "^(test|example)$"},
			},
			want: map[string]Filter{
				"diskstats/device":  {Include: "^(test123|example123)$"},
				"netdev/device":     {Include: "^(test456|example456)$"},
				"filesystem/fstype": {Exclude: "^(test789|example789)$"},
				"test/example":      {Exclude: "^(test|example)$"},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			DefaultFilters(tc.in)
			assert.Equal(t, tc.want, tc.in)
		})
	}
}

func TestCompileFilters(t *testing.T) {
	var testcases = []struct {
		name  string
		valid bool
		in    map[string]Filter
	}{
		{
			name: "defined filters", valid: true,
			in: map[string]Filter{
				"test/example": {Exclude: "^(test|example)$", Include: "^(rumba|samba)$"},
			},
		},
		{name: "invalid exclude", valid: false, in: map[string]Filter{"test": {Exclude: "["}}},
		{name: "invalid include", valid: false, in: map[string]Filter{"test": {Include: "["}}},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.valid {
				assert.NoError(t, CompileFilters(tc.in))
				assert.NotNil(t, tc.in["test/example"].ExcludeRE)
				assert.NotNil(t, tc.in["test/example"].IncludeRE)
			} else {
				assert.Error(t, CompileFilters(tc.in))
			}

		})
	}
}

func TestFilter_Pass(t *testing.T) {
	var testcases = []struct {
		name string
		in   Filter
		want bool
	}{
		{name: "empty regexps", in: Filter{ExcludeRE: nil, IncludeRE: nil}, want: true},
		{name: "+exclude,+include", in: Filter{ExcludeRE: regexp.MustCompile("test"), IncludeRE: regexp.MustCompile("test")}, want: false},
		{name: "-exclude,-include", in: Filter{ExcludeRE: regexp.MustCompile("example"), IncludeRE: regexp.MustCompile("example")}, want: false},
		{name: "+exclude,-include", in: Filter{ExcludeRE: regexp.MustCompile("test"), IncludeRE: regexp.MustCompile("example")}, want: false},
		{name: "-exclude,+include", in: Filter{ExcludeRE: regexp.MustCompile("example"), IncludeRE: regexp.MustCompile("test")}, want: true},
		{name: "+exclude,nil", in: Filter{ExcludeRE: regexp.MustCompile("test"), IncludeRE: nil}, want: false},
		{name: "nil,+include", in: Filter{ExcludeRE: nil, IncludeRE: regexp.MustCompile("example")}, want: false},
		{name: "nil,+include", in: Filter{ExcludeRE: nil, IncludeRE: regexp.MustCompile("test")}, want: true},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.in.Pass("test"))
		})
	}
}
