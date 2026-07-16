package report

import (
	"fmt"
	"strings"

	"github.com/goabv/s3-benchmark-go/internal/bench"
)

// RenderTables builds the human-readable resource-usage and per-part-time tables
// shown on the console and saved to summary.txt, mirroring the JS runner.
func RenderTables(run *bench.RunResult, res bench.Resources) string {
	var b strings.Builder

	b.WriteString("\nthroughput (median over measured iterations):\n")
	fmt.Fprintf(&b, "%-14s %8s %7s %11s %12s\n", "size", "parts", "files", "med Gbps", "med MiB/s")
	b.WriteString(strings.Repeat("-", 56) + "\n")
	for _, g := range run.Groups {
		fmt.Fprintf(&b, "%-14s %8d %7d %11.3f %12.1f\n",
			g.Label, g.Parts, g.Files, g.Median.Gbps, g.Median.Mibps)
		if g.TTFB.Count > 0 {
			fmt.Fprintf(&b, "  ttfb ms  p50=%.1f p90=%.1f p99=%.1f p99.9=%.1f\n",
				g.TTFB.P50, g.TTFB.P90, g.TTFB.P99, g.TTFB.P999)
		}
		if g.DistinctIPs > 0 || g.Retries > 0 {
			fmt.Fprintf(&b, "  front-end IPs=%d  conn-reuse=%.0f%%  retries=%d\n",
				g.DistinctIPs, g.ReuseRatio*100, g.Retries)
		}
	}

	b.WriteString("\nresource usage (whole process, during the sampled run):\n")
	fmt.Fprintf(&b, "%-16s %10s %10s %9s %9s %9s\n", "size", "peak RSS", "avg RSS", "peak CPU", "avg CPU", "peak MEM")
	b.WriteString(strings.Repeat("-", 70) + "\n")
	for _, g := range run.Groups {
		fmt.Fprintf(&b, "%-16s %10s %10s %8.0f%% %8.0f%% %8.1f%%\n",
			g.Label,
			humanBytes(res.PeakRssBytes),
			humanBytes(int64(res.AvgRssBytes)),
			res.PeakCpuPercent,
			res.AvgCpuPercent,
			res.PeakMemUtilPercent)
	}
	if res.TotalMemBytes > 0 {
		fmt.Fprintf(&b, "(CPU%% is of all %d cores; MEM%% is of %s total RAM)\n", res.CpuCount, humanBytes(res.TotalMemBytes))
	} else {
		fmt.Fprintf(&b, "(CPU%% is of all %d cores; total RAM unknown on this platform)\n", res.CpuCount)
	}

	// Per-part timing is only available when the runner sees individual parts
	// (the custom runner). The Transfer Manager hides parts, so skip the table
	// rather than print a row of zeros.
	hasPartTimes := false
	for _, g := range run.Groups {
		if g.PartTime.Count > 0 {
			hasPartTimes = true
			break
		}
	}
	if hasPartTimes {
		label := "download"
		if strings.Contains(run.Mode, "upload") {
			label = "upload"
		}
		b.WriteString(fmt.Sprintf("\nper-part %s time (ms), across all measured iterations:\n", label))
		fmt.Fprintf(&b, "%-16s %8s %8s %8s %8s %8s %8s %8s %8s\n",
			"size", "parts", "min", "p50", "p90", "p99", "p99.9", "max", "mean")
		b.WriteString(strings.Repeat("-", 86) + "\n")
		for _, g := range run.Groups {
			p := g.PartTime
			fmt.Fprintf(&b, "%-16s %8d %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
				g.Label, p.Count, p.Min, p.P50, p.P90, p.P99, p.P999, p.Max, p.Mean)
		}
	}
	return b.String()
}
