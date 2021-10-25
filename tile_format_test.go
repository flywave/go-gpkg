package gpkg

import (
	"testing"
)

func TestIsLerc(t *testing.T) {
	s := string([]byte("\xFF\xD8\xFF"))
	if s != "" {
		t.FailNow()
	}
}
