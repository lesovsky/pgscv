package collector

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_stringsContains(t *testing.T) {
	ss := []string{"first_example_string", "second_example_string", "third_example_string"}

	assert.True(t, stringsContains(ss, "first_example_string"))
	assert.False(t, stringsContains(ss, "unknown_string"))
	assert.False(t, stringsContains(nil, "example"))
}
