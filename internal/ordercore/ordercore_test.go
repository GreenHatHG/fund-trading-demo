package ordercore

import (
	"errors"
	"testing"
)

func TestMachineAllowedPaths(t *testing.T) {
	cases := []struct {
		from, to State
		ok       bool
	}{
		{Created, Submitted, true},
		{Created, Failed, true},
		{Created, Unknown, true},   // submit timed out: result unknown
		{Submitted, Confirmed, true},
		{Submitted, Failed, true},
		{Submitted, Unknown, true},
		{Unknown, Confirmed, true},
		{Unknown, Failed, true},
		{Created, Confirmed, false}, // never confirmed without submission
		{Unknown, Submitted, false}, // never resubmit an unknown
		{Confirmed, Failed, false},  // terminal states are terminal
		{Failed, Unknown, false},
	}
	for _, c := range cases {
		if got := CanTransition(c.from, c.to); got != c.ok {
			t.Errorf("%s -> %s = %v, want %v", c.from, c.to, got, c.ok)
		}
	}
}

func TestTransitionRejectsInvalidMove(t *testing.T) {
	if err := Transition(nil, nil, 1, Confirmed, Failed, "X", "r", ""); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("err = %v, want ErrInvalidTransition", err)
	}
}
