package app

import (
	"github.com/rs/zerolog/log"
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

// ActivateSchedule method activates existing schedule
func (s *statDescriptor) ActivateSchedule() {
	s.Active = true
}

// IsScheduleActive method returns true if the schedule is active
func (s *statDescriptor) IsScheduleActive() bool {
	return s.Active
}

// IsScheduleExpired method returns true if schedule's time is up
func (s *statDescriptor) IsScheduleExpired() bool {
	elapsed := time.Since(s.LastFired)
	if elapsed < s.Interval {
		return false
	}
	log.Debug().Msgf("time for %s, elapsed: %v > %v", s.Name, elapsed, s.Interval)
	return true
}

// ScheduleUpdateExpired method updates schedule when it's expired
func (s *statDescriptor) ScheduleUpdateExpired() {
	if s.Active && s.IsScheduleExpired() {
		s.LastFired = time.Now()
	}
}
