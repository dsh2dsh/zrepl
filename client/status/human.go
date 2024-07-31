package status

import (
	"fmt"
	"math"
	"strings"
	"time"
)

func humanizeFormat(v uint64, iec bool, format string) string {
	humanized, suffix := humanizeSize(v, iec)
	return fmt.Sprintf(format, humanized, suffix)
}

func humanizeSize(s uint64, iec bool) (string, string) {
	sizes := [...]string{"", "k", "M", "G", "T", "P", "E"}
	base := 1000.0

	if iec {
		sizes = [...]string{"", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei"}
		base = 1024.0
	}

	if s < 10 {
		return fmt.Sprintf("%.0f", float64(s)), sizes[0]
	}

	e := math.Floor(math.Log(float64(s)) / math.Log(base))
	suffix := sizes[int(e)]
	v := math.Floor(float64(s)/math.Pow(base, e)*10+0.5) / 10
	if v < 10 {
		return fmt.Sprintf("%.1f", v), suffix
	}
	return fmt.Sprintf("%.0f", v), suffix
}

func humanizeDuration(d time.Duration) string {
	days := int64(d.Hours() / 24)
	hours := int64(math.Mod(d.Hours(), 24))
	minutes := int64(math.Mod(d.Minutes(), 60))
	seconds := int64(math.Mod(d.Seconds(), 60))
	chunks := []int64{days, hours, minutes, seconds}

	parts := []string{}
	force := false
	for i, chunk := range chunks {
		if force || chunk > 0 {
			padding := 0
			if force {
				padding = 2
			}
			parts = append(parts, fmt.Sprintf("%*d%c", padding, chunk, "dhms"[i]))
			force = true
		}
	}
	return strings.Join(parts, " ")
}
