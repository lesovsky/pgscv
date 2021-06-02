package filter

import (
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
)

func TestFilters_Compile(t *testing.T) {
	var testcases = []struct {
		name  string
		valid bool
		in    Filters
	}{
		{
			name: "defined filters", valid: true,
			in: Filters{
				"test/example": {Exclude: "^(test|example)$", Include: "^(rumba|samba)$"},
			},
		},
		{name: "invalid exclude", valid: false, in: Filters{"test": {Exclude: "["}}},
		{name: "invalid include", valid: false, in: Filters{"test": {Include: "["}}},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.valid {
				assert.NoError(t, tc.in.Compile())
				assert.NotNil(t, tc.in["test/example"].ExcludeRE)
				assert.NotNil(t, tc.in["test/example"].IncludeRE)
			} else {
				assert.Error(t, tc.in.Compile())
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

// Pass2 extra tests for filters.
func TestFilter_Pass2(t *testing.T) {
	testcases := []struct {
		in    string
		ftype string
		pass  bool
	}{
		{in: "hda", ftype: "diskstats/device", pass: true},
		{in: "sdb", ftype: "diskstats/device", pass: true},
		{in: "vdc", ftype: "diskstats/device", pass: true},
		{in: "xvdd", ftype: "diskstats/device", pass: true},
		{in: "ram1", ftype: "diskstats/device", pass: false},
		{in: "loop0", ftype: "diskstats/device", pass: false},
		{in: "fd2", ftype: "diskstats/device", pass: false},
		{in: "sr0", ftype: "diskstats/device", pass: false},
		{in: "hda1", ftype: "diskstats/device", pass: false},
		{in: "sdb2", ftype: "diskstats/device", pass: false},
		{in: "vdc3", ftype: "diskstats/device", pass: false},
		{in: "xvdd4", ftype: "diskstats/device", pass: false},
		{in: "nvme0n1p1", ftype: "diskstats/device", pass: false},
		{in: "eth0", ftype: "netdev/device", pass: true},
		{in: "docker0", ftype: "netdev/device", pass: false},
		{in: "virbr", ftype: "netdev/device", pass: false},
		{in: "ext3", ftype: "filesystem/fstype", pass: true},
		{in: "ext4", ftype: "filesystem/fstype", pass: true},
		{in: "xfs", ftype: "filesystem/fstype", pass: true},
		{in: "btrfs", ftype: "filesystem/fstype", pass: true},
		{in: "ramfs", ftype: "filesystem/fstype", pass: false},
		{in: "overlay", ftype: "filesystem/fstype", pass: false},
		{in: "tmpfs", ftype: "filesystem/fstype", pass: false},
	}

	filters := New()
	filters.Add("diskstats/device", Filter{Exclude: `^(ram|loop|fd|sr|(h|s|v|xv)d[a-z]|nvme\d+n\d+p)\d+$`})
	filters.Add("netdev/device", Filter{Exclude: `docker|virbr`})
	filters.Add("filesystem/fstype", Filter{Include: `^(ext3|ext4|xfs|btrfs)$`})

	assert.NoError(t, filters.Compile())

	for _, tc := range testcases {
		f := filters[tc.ftype]
		assert.Equal(t, tc.pass, f.Pass(tc.in))
	}
}
