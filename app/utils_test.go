package app

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestContains(t *testing.T) {
	var testCases = []struct {
		payload []string
		search  string
		want    bool
	}{
		{
			payload: []string{"test", "example"},
			search:  "example",
			want:    true,
		},
		{
			payload: []string{"test", "example"},
			search:  "qwerty",
			want:    false,
		},
		{
			payload: []string{},
			search:  "example",
			want:    false,
		},
	}

	for _, tc := range testCases {
		got := Contains(tc.payload, tc.search)
		assert.Equal(t, tc.want, got)
	}
}
