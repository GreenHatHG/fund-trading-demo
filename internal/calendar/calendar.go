// Package calendar does trading-day math. One module, one job.
package calendar

import "time"

var shanghai *time.Location

func init() {
	var err error
	shanghai, err = time.LoadLocation("Asia/Shanghai")
	if err != nil {
		shanghai = time.FixedZone("CST", 8*3600)
	}
}

// IsTradingDay: Mon-Fri is a trading day; holiday calendar is a stub for the demo.
func IsTradingDay(t time.Time) bool {
	wd := t.In(shanghai).Weekday()
	return wd >= time.Monday && wd <= time.Friday
}

// Cutoff is 15:00 Asia/Shanghai on the given day.
func Cutoff(t time.Time) time.Time {
	y, m, d := t.In(shanghai).Date()
	return time.Date(y, m, d, 15, 0, 0, 0, shanghai)
}

// NextTradingDay returns the next trading day strictly after t.
func NextTradingDay(t time.Time) time.Time {
	n := t.In(shanghai).AddDate(0, 0, 1)
	for !IsTradingDay(n) {
		n = n.AddDate(0, 0, 1)
	}
	y, m, d := n.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, shanghai)
}

// StampAtIntake returns (trading_day, confirm_date) for an order accepted at time t.
// After cutoff, the order belongs to the next trading day and confirms T+1 after that.
func StampAtIntake(t time.Time) (tradingDay, confirmDate time.Time) {
	tradingDay = t.In(shanghai)
	if tradingDay.After(Cutoff(t)) || !IsTradingDay(tradingDay) {
		tradingDay = NextTradingDay(t)
	}
	confirmDate = tradingDay
	for i := 0; i < 1; i++ { // T+1
		confirmDate = NextTradingDay(confirmDate)
	}
	return tradingDay, confirmDate
}
