package app

import (
	"github.com/prometheus/common/log"
	"time"
)

// Schedule defines scheduling settings for stats descriptor
type Schedule struct {
	Active    bool          // is this schedule active
	Interval  time.Duration // collecting interval
	LastFired time.Time     // timestamp of last collect
}

// ActivateSchedule method activates existing schedule
func (s *StatDesc) ActivateSchedule() {
	s.Active = true
}

// IsScheduleActive method returns true if the schedule is active
func (s *StatDesc) IsScheduleActive() bool {
	return s.Active
}

// IsScheduleExpired method returns true if schedule's time is up
func (s *StatDesc) IsScheduleExpired() bool {
	elapsed := time.Since(s.LastFired)
	if elapsed < s.Interval {
		return false
	}
	log.Debugf("time for %s, elapsed: %v > %v", s.Name, elapsed, s.Interval)
	return true
}

// ScheduleUpdateExpired method updates schedule when it's expired
func (s *StatDesc) ScheduleUpdateExpired() {
	if s.Active && s.IsScheduleExpired() {
		s.LastFired = time.Now()
	}
}
