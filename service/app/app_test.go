package app

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetLabelByHostname(t *testing.T) {
	s, err := getLabelByHostname()
	assert.NoError(t, err)
	assert.NotEmpty(t, s)
}

func TestGetLabelByMachineID(t *testing.T) {
	s, err := getLabelByMachineID()
	assert.NoError(t, err)
	assert.NotEmpty(t, s)
}
