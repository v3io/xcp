package backends

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

type testLocalBackend struct {
	suite.Suite
}

func TestLocalBackendSuite(t *testing.T) {
	suite.Run(t, new(testLocalBackend))
}
