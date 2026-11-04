package calendar

import (
	"testing"
	"time"
)

func fixed(y int, mo time.Month, d, h, mi int) time.Time {
	return time.Date(y, mo, d, h, mi, 0, 0, time.FixedZone("CST", 8*3600))
}

func TestBeforeCutoffConfirmsNextTradingDay(t *testing.T) {
	// Thursday 2026-11-05 10:00 -> trading day same day, confirm Friday 11-06
	td, cd := StampAtIntake(fixed(2026, 11, 5, 10, 0))
	if got := td.Format("2006-01-02"); got != "2026-11-05" {
		t.Fatalf("trading day = %s, want 2026-11-05", got)
	}
	if got := cd.Format("2006-01-02"); got != "2026-11-06" {
		t.Fatalf("confirm date = %s, want 2026-11-06", got)
	}
}

func TestAfterCutoffRollsToNextTradingDay(t *testing.T) {
	// Thursday 2026-11-05 15:01 (after 15:00 cutoff) -> trading Friday, confirm Monday
	td, cd := StampAtIntake(fixed(2026, 11, 5, 15, 1))
	if got := td.Format("2006-01-02"); got != "2026-11-06" {
		t.Fatalf("trading day = %s, want 2026-11-06", got)
	}
	if got := cd.Format("2006-01-02"); got != "2026-11-09" {
		t.Fatalf("confirm date = %s, want 2026-11-09", got)
	}
}

func TestFridayAfterCutoffSkipsWeekend(t *testing.T) {
	// Friday 2026-11-06 16:00 -> trading Monday 11-09, confirm Tuesday 11-10
	td, cd := StampAtIntake(fixed(2026, 11, 6, 16, 0))
	if got := td.Format("2006-01-02"); got != "2026-11-09" {
		t.Fatalf("trading day = %s, want 2026-11-09", got)
	}
	if got := cd.Format("2006-01-02"); got != "2026-11-10" {
		t.Fatalf("confirm date = %s, want 2026-11-10", got)
	}
}
