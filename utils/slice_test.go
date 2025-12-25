package utils

import (
	"fmt"
	"testing"
)

func TestUniqueSlice(t *testing.T) {
	fmt.Printf("%+v", UniqueSlice([]int{1}))
	fmt.Printf("%+v", UniqueSlice([]int{1, 1}))
	fmt.Printf("%+v", UniqueSlice([]int{1, 1, 1}))
	fmt.Printf("%+v", UniqueSlice([]int{1, 1, 2}))
	fmt.Printf("%+v", UniqueSlice([]int{1, 2, 2}))
	fmt.Printf("%+v", UniqueSlice([]int{1, 2, 2, 3}))
	fmt.Printf("%+v", UniqueSlice([]int{1, 2, 2, 3, 3}))
	fmt.Printf("%+v", UniqueSlice([]int{1, 2, 2, 3, 3, 3, 3, 3}))
	fmt.Printf("%+v", UniqueSlice([]int{1, 2, 2, 3, 3, 3, 3, 3, 4}))
}
