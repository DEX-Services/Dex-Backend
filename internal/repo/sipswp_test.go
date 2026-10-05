package repo

import (
	"testing"
	"time"
)

func date(y, m, d int) time.Time {
	return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
}

func TestClampToDayOfMonth(t *testing.T) {
	cases := []struct {
		name        string
		year, month int
		day         int
		wantYMD     [3]int
	}{
		{"mid-month day stays as-is", 2026, 6, 15, [3]int{2026, 6, 15}},
		{"day 31 in a 30-day month clamps to 30", 2026, 4, 31, [3]int{2026, 4, 30}},
		{"day 31 in July (has 31 days) stays 31", 2026, 7, 31, [3]int{2026, 7, 31}},
		{"day 29 in non-leap February clamps to 28", 2026, 2, 29, [3]int{2026, 2, 28}},
		{"day 29 in leap-year February stays 29", 2028, 2, 29, [3]int{2028, 2, 29}},
		{"day 30 in non-leap February clamps to 28", 2026, 2, 30, [3]int{2026, 2, 28}},
		{"day below 1 clamps to 1", 2026, 6, 0, [3]int{2026, 6, 1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := clampToDayOfMonth(c.year, c.month, c.day)
			want := date(c.wantYMD[0], c.wantYMD[1], c.wantYMD[2])
			if !got.Equal(want) {
				t.Fatalf("clampToDayOfMonth(%d,%d,%d) = %v, want %v", c.year, c.month, c.day, got, want)
			}
		})
	}
}

func TestFirstRunDate(t *testing.T) {
	day := func(d int) *int { return &d }

	cases := []struct {
		name        string
		start       time.Time
		frequency   string
		dayOfPeriod *int
		want        time.Time
	}{
		{"daily returns start unchanged", date(2026, 6, 10), "DAILY", nil, date(2026, 6, 10)},
		{"weekly returns start unchanged", date(2026, 6, 10), "WEEKLY", nil, date(2026, 6, 10)},
		{"monthly, start day already equals dayOfPeriod", date(2026, 6, 5), "MONTHLY", day(5), date(2026, 6, 5)},
		{"monthly, dayOfPeriod overflows start's month", date(2026, 4, 1), "MONTHLY", day(31), date(2026, 4, 30)},
		{"yearly uses start's own month/year with dayOfPeriod", date(2026, 3, 1), "YEARLY", day(29), date(2026, 3, 29)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := firstRunDate(c.start, c.frequency, c.dayOfPeriod)
			if !got.Equal(c.want) {
				t.Fatalf("firstRunDate(%v,%q,%v) = %v, want %v", c.start, c.frequency, c.dayOfPeriod, got, c.want)
			}
		})
	}
}

func TestNextRunDateAfter(t *testing.T) {
	day := func(d int) *int { return &d }

	cases := []struct {
		name        string
		from        time.Time
		frequency   string
		dayOfPeriod *int
		want        time.Time
	}{
		{"daily advances by one day", date(2026, 6, 10), "DAILY", nil, date(2026, 6, 11)},
		{"weekly advances by seven days", date(2026, 6, 10), "WEEKLY", nil, date(2026, 6, 17)},
		{"monthly same date next month", date(2026, 6, 5), "MONTHLY", day(5), date(2026, 7, 5)},
		{
			"monthly: day 31 clamps every month it doesn't exist in — Jan 31 -> Feb 28",
			date(2026, 1, 31), "MONTHLY", day(31), date(2026, 2, 28),
		},
		{
			"monthly: clamped date does NOT wait for a month that has day 31 — Feb clamp rolls to March 31, not skipped",
			date(2026, 2, 28), "MONTHLY", day(31), date(2026, 3, 31),
		},
		{"monthly rolls over the year boundary", date(2026, 12, 15), "MONTHLY", day(15), date(2027, 1, 15)},
		{"yearly same month/day next year", date(2026, 3, 10), "YEARLY", day(10), date(2027, 3, 10)},
		{
			"yearly: Feb 29 in a leap year clamps to Feb 28 the next (non-leap) year",
			date(2028, 2, 29), "YEARLY", day(29), date(2029, 2, 28),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := nextRunDateAfter(c.from, c.frequency, c.dayOfPeriod)
			if !got.Equal(c.want) {
				t.Fatalf("nextRunDateAfter(%v,%q,%v) = %v, want %v", c.from, c.frequency, c.dayOfPeriod, got, c.want)
			}
		})
	}
}
