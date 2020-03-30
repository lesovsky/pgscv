package model

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestService_IsAvailable ...
func TestService_IsAvailable(t *testing.T) {
	var testCases = []struct {
		name    string
		payload func() (*Service, func())
		valid   bool
	}{
		{
			name: "valid system service",
			payload: func() (*Service, func()) {
				s, teardown, err := NewTestService()
				assert.NoError(t, err)
				assert.NotNil(t, s)
				assert.NotNil(t, teardown)
				s.ServiceType = ServiceTypeSystem
				return s, teardown
			},
			valid: true,
		},
		{
			name: "valid non-system service",
			payload: func() (*Service, func()) {
				s, teardown, err := NewTestService()
				assert.NoError(t, err)
				assert.NotNil(t, s)
				assert.NotNil(t, teardown)
				return s, teardown
			},
			valid: true,
		},
		{
			name: "invalid service, wrong pid",
			payload: func() (*Service, func()) {
				s, teardown, err := NewTestService()
				assert.NoError(t, err)
				assert.NotNil(t, s)
				assert.NotNil(t, teardown)
				s.Pid = 0
				return s, teardown
			},
			valid: false,
		},
		{
			name: "invalid service, wrong name",
			payload: func() (*Service, func()) {
				s, teardown, err := NewTestService()
				assert.NoError(t, err)
				assert.NotNil(t, s)
				assert.NotNil(t, teardown)
				s.ProcessName = "invalid"
				return s, teardown
			},
			valid: false,
		},
		{
			name: "invalid service, wrong create time",
			payload: func() (*Service, func()) {
				s, teardown, err := NewTestService()
				assert.NoError(t, err)
				assert.NotNil(t, s)
				assert.NotNil(t, teardown)
				s.ProcessCreateTime = 1
				return s, teardown
			},
			valid: false,
		},
	}

	for _, tc := range testCases {
		s, teardown := tc.payload()
		assert.NotNil(t, s)
		assert.NotNil(t, teardown)
		assert.Equal(t, tc.valid, s.IsAvailable())
		teardown()
	}
}
