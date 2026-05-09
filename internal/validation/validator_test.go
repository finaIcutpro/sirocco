package validation

import "testing"

func TestValidatorLoadsOfficialSpec(t *testing.T) {
	if _, err := New(nil); err != nil {
		t.Fatal(err)
	}
}
