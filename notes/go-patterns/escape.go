package main

import (
	"fmt"
)

func dothing(x any) {
	fmt.Printf("x: %v\n", x)
}

func main() {
	x := 42
	dothing(x)
}
