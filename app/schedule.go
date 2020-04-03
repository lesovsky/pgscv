package app

import (
	"pgscv/app/log"
	"time"
)

// Schedule defines scheduling settings for stats descriptor
type Schedule struct {
	Active    bool          // is this schedule active
	Interval  time.Duration // collecting interval
	LastFired time.Time     // timestamp of last collect
}

const defaultScheduleInterval = 5 * time.Minute

func newSchedule(interval time.Duration) Schedule {
	if interval == 0 {
		interval = defaultScheduleInterval
	}
	return Schedule{Interval: interval}
}

// ActivateDescriptor means to collect metric during the next round
func (s *statDescriptor) ActivateDescriptor() {
	s.Active = true
}

// DeactivateDescriptor means don'e collect metric during the next round
func (s *statDescriptor) DeacivateDescriptor() {
	s.Active = false
}

// IsScheduleActive method returns true if the schedule is active
func (s *statDescriptor) IsDescriptorActive() bool {
	return s.Active
}

// IsScheduleExpired method returns true if schedule's time is up
func (s *statDescriptor) IsScheduleExpired() bool {
	elapsed := time.Since(s.LastFired)
	if elapsed < s.Interval {
		return false
	}
	log.Debugf("time for %s, elapsed: %v > %v", s.Name, elapsed, s.Interval)
	return true
}

// ScheduleUpdateExpired method updates schedule when it's expired
func (s *statDescriptor) ScheduleUpdateExpired() {
	if s.Active && s.IsScheduleExpired() {
		s.LastFired = time.Now()
	}
}
