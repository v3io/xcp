package backends

import (
	"fmt"
	"testing"
)

func TestFileParser(t *testing.T) {
	tests := []string{
		"aa/bb", "aa/", "a*a/b", "aa/b*", "aa",
	}

	for _, path := range tests {
		p := PathParams{}
		err := ParseFilename(path, &p, true)
		fmt.Println(p, err)
	}
}
