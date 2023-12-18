package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()
	assert.Nil(t, err)
	t.Log("对调用者的空闲空间", size/1024/1024/1024)
	assert.True(t, size > 0)
}
