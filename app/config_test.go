package app

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecodeProjectIDStr(t *testing.T) {
	testcases := []struct {
		key string
		id  string
	}{
		{
			key: "A1B2C3D4E5F6-A1B2-C3D4-ABCD1EFG",
			id:  "1",
		},
		{
			key: "A1B2C3D4E5F6-A1B2-C3D4-A2CDE5FG",
			id:  "25",
		},
		{
			key: "A1B2C3D4E5F6-A1B2-C3D4-3AB2CD9E",
			id:  "329",
		},
		{
			key: "A1B2C3D4E5F6-A1B2-C3D4-ABCDEFGH",
			id:  "",
		},
		{
			key: "a1b2c3d4e5f6-a1b2-c3d4-abc6defg",
			id:  "",
		},
		{
			key: "A1B2C3D4E5F6-A1B2-C3D4-AB12F+GH",
			id:  "",
		},
		{
			key: "invalid",
			id:  "",
		},
		{
			key: "",
			id:  "",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.key, func(t *testing.T) {
			got := DecodeProjectIDStr(tc.key)
			assert.Equal(t, tc.id, got)
		})
	}
}
