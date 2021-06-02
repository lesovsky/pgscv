package filter

import (
	"github.com/weaponry/pgscv/internal/log"
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
		return false
	}
	// Include is specified and target doesn't match 'include' - reject.
	if f.IncludeRE != nil && !f.IncludeRE.MatchString(target) {
		return false
	}
	// Here means Include is specified and target matches 'include' - pass.
	return true
}

// Filters is the set of named filters
type Filters map[string]Filter

// New create new and empty Filters object.
func New() Filters {
	return map[string]Filter{}
}

// Add add new filter to existing set of filters. After adding new filter, filters should be recompiled.
func (f Filters) Add(name string, filter Filter) {
	f[name] = filter
}

// Compile walk trough filters and compile them.
func (f Filters) Compile() error {
	log.Debug("compile filters")

	for key, filter := range f {
		if filter.Exclude != "" {
			re, err := regexp.Compile(filter.Exclude)
			if err != nil {
				return err
			}
			filter.ExcludeRE = re
		}

		if filter.Include != "" {
			re, err := regexp.Compile(filter.Include)
			if err != nil {
				return err
			}
			filter.IncludeRE = re
		}

		// Save updated filter back to map.
		f[key] = filter
	}

	log.Debug("filters compiled successfully")
	return nil
}
