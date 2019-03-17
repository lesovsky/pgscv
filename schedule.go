package main

import (
	"github.com/prometheus/common/log"
	"time"
)

// Настройки планирования запуска сбора метрик - позволяет собирать метрики только через определенный интервал
type Schedule struct {
	Active    bool          // is this schedule active
	Interval  time.Duration // runtime interval
	LastFired time.Time     // timestamp of last run
}

// Activate method activates existing schedule
func (s *Schedule) Activate() {
	s.Active = true
}

// IsActive method returns true if the schedule is active
func (s *Schedule) IsActive() bool {
	return s.Active
}

// IsExpired method returns true if schedule's time is up
func (s *Schedule) IsExpired(name string) bool {
	elapsed := time.Since(s.LastFired)
	if elapsed < s.Interval {
		return false
	}
	log.Debugf("time for %s, elapsed: %v > %v", name, elapsed, s.Interval)
	return true
}

// Update method updates time when schedule was executed
func (s *Schedule) Update() {
	if s.Active {
		s.LastFired = time.Now()
	}
}