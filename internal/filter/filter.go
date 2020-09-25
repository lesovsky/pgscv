package filter

import (
	"github.com/barcodepro/pgscv/internal/log"
	"regexp"
)

// Filter describes settings for filtering stats values for metrics.
type Filter struct {
	// Exclude pattern string.
	Exclude string `yaml:"exclude,omitempty"`
	// Compiled exclude pattern regexp.
	ExcludeRE *regexp.Regexp
	// Include pattern string.
	Include string `yaml:"include,omitempty"`
	// Compiled include pattern regexp.
	IncludeRE *regexp.Regexp
}

// DefaultFilters set up default collectors filters.
func DefaultFilters(filters map[string]Filter) {
	log.Debug("define default filters")

	// Setting up default EXCLUDE pattern for storage devices.
	if _, ok := filters["diskstats/device"]; !ok {
		filters["diskstats/device"] = Filter{Exclude: `^(ram|loop|fd|(h|s|v|xv)d[a-z]|nvme\d+n\d+p)\d+$`}
	}

	// Setting up default EXCLUDE pattern for network devices.
	if _, ok := filters["netdev/device"]; !ok {
		filters["netdev/device"] = Filter{Exclude: `docker|virbr`}
	}

	// Setting up default INCLUDE pattern for filesystem types.
	if _, ok := filters["filesystem/fstype"]; !ok {
		filters["filesystem/fstype"] = Filter{Include: `^(ext3|ext4|xfs|btrfs)$`}
	}
}

// CompileFilters walk trough filters and compile them.
func CompileFilters(filters map[string]Filter) error {
	log.Debug("compile filters")

	for key, f := range filters {
		if f.Exclude != "" {
			re, err := regexp.Compile(f.Exclude)
			if err != nil {
				return err
			}
			f.ExcludeRE = re
		}

		if f.Include != "" {
			re, err := regexp.Compile(f.Include)
			if err != nil {
				return err
			}
			f.IncludeRE = re
		}

		// Save updated filter back to map.
		filters[key] = f
	}

	log.Debug("filters compiled successfully")
	return nil
}

// Pass checks that target is satisfied to filter's regexps.
func (f *Filter) Pass(target string) bool {
	// Filters not specified - pass the target.
	if f.ExcludeRE == nil && f.IncludeRE == nil {
		return true
	}

	if f.ExcludeRE != nil && f.IncludeRE != nil {
		// Target matches to 'exclude' and 'include' - reject, exclude has higher priority.
		if f.ExcludeRE.MatchString(target) && f.IncludeRE.MatchString(target) {
			return false
		}
		// Target neither match 'exclude' nor 'include' - reject, target doesn't match to include explicitly.
		if !f.ExcludeRE.MatchString(target) && !f.IncludeRE.MatchString(target) {
			return false
		}
		// Target matches to 'exclude' and doesn't match to 'include' - reject.
		if f.ExcludeRE.MatchString(target) && !f.IncludeRE.MatchString(target) {
			return false
		}
		// Target doesn't match to 'exclude' and matches to 'include' - pass.
		if !f.ExcludeRE.MatchString(target) && f.IncludeRE.MatchString(target) {
			return true
		}
	}

	// Exclude is specified and target matches 'exclude' - reject.
	if f.ExcludeRE != nil && f.ExcludeRE.MatchString(target) {
		log.Debugln("exclude target ", target)
		return false
	}
	// Include is specified and target doesn't match 'include' - reject.
	if f.IncludeRE != nil && !f.IncludeRE.MatchString(target) {
		log.Debugln("exclude target ", target)
		return false
	}
	// Here means Include is specified and target matches 'include' - pass.
	return true
}
