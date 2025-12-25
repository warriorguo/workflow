package types_test

import (
	"math"
	"strconv"
	"testing"

	"github.com/warriorguo/workflow/types"
	"github.com/stretchr/testify/assert"
)

type testStruct struct {
	Name   string
	Age    int
	IsMale bool
}

func TestData(t *testing.T) {
	data := &types.Data{}

	data.Set("teststruct1", testStruct{"hello", 4, false})
	data.Set("teststruct2", testStruct{"kitty", 5, true})

	hello := &testStruct{}
	kitty := &testStruct{}
	assert.Nil(t, data.GetStruct("teststruct1", hello))
	assert.Nil(t, data.GetStruct("teststruct2", kitty))

	assert.Equal(t, "hello", hello.Name)
	assert.Equal(t, 4, hello.Age)
	assert.Equal(t, false, hello.IsMale)

	assert.Equal(t, "kitty", kitty.Name)
	assert.Equal(t, 5, kitty.Age)
	assert.Equal(t, true, kitty.IsMale)

	data.Set("s1", 1)
	data.Set("s2", "2")
	data.Set("s3", math.Pi)
	data.Set("s4", true)

	_, exists := data.Get("s0")
	assert.False(t, exists)

	s, exists := data.GetString("s1")
	assert.True(t, exists)
	assert.Equal(t, "1", s)
	s, exists = data.GetString("s2")
	assert.True(t, exists)
	assert.Equal(t, "2", s)
	s, exists = data.GetString("s3")
	assert.True(t, exists)
	assert.Equal(t, strconv.FormatFloat(math.Pi, 'f', -1, 64), s)
	s, exists = data.GetString("s4")
	assert.True(t, exists)
	assert.Equal(t, "true", s)
}
