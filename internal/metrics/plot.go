package metrics

import (
	"bytes"
	"fmt"
	"strings"
)

const (
	plotWidth   = 960
	panelHeight = 190
	padLeft     = 64
	padRight    = 24
	padTop      = 34
	padBottom   = 34
)

type series struct {
	name  string
	unit  string
	color string
	vals  []float64
}

// PlotSVG renders the sampled time series as a stacked, multi-panel SVG line
// chart (throughput, RSS, in-flight). It has no external dependencies.
func PlotSVG(samples []Sample, title string) []byte {
	if len(samples) == 0 {
		return []byte(`<svg xmlns="http://www.w3.org/2000/svg" width="420" height="60">` +
			`<text x="10" y="34" font-family="sans-serif">no samples captured</text></svg>`)
	}

	xs := make([]float64, len(samples))
	for i, s := range samples {
		xs[i] = float64(s.TMillis) / 1000.0 // seconds
	}
	maxX := xs[len(xs)-1]
	if maxX <= 0 {
		maxX = 1
	}

	panels := []series{
		{"Throughput", "Gbps", "#2563eb", pluck(samples, func(s Sample) float64 { return s.Gbps })},
		{"RSS", "MiB", "#dc2626", pluck(samples, func(s Sample) float64 { return float64(s.RSSBytes) / (1 << 20) })},
		{"In-flight", "reqs", "#16a34a", pluck(samples, func(s Sample) float64 { return float64(s.InFlight) })},
		{"CPU", "%", "#9333ea", pluck(samples, func(s Sample) float64 { return s.CPUPct })},
	}

	height := padTop + len(panels)*panelHeight + padBottom
	var b bytes.Buffer
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" font-family="sans-serif" font-size="12">`, plotWidth, height)
	fmt.Fprintf(&b, `<rect width="%d" height="%d" fill="white"/>`, plotWidth, height)
	fmt.Fprintf(&b, `<text x="%d" y="20" font-size="15" font-weight="bold">%s</text>`, padLeft, escapeXML(title))

	for pi, ser := range panels {
		top := padTop + pi*panelHeight
		drawPanel(&b, ser, xs, maxX, top)
	}
	b.WriteString(`</svg>`)
	return b.Bytes()
}

func drawPanel(b *bytes.Buffer, ser series, xs []float64, maxX float64, top int) {
	plotW := plotWidth - padLeft - padRight
	plotH := panelHeight - 46
	baseY := top + plotH + 12

	maxV := 0.0
	for _, v := range ser.vals {
		if v > maxV {
			maxV = v
		}
	}
	if maxV <= 0 {
		maxV = 1
	}

	// axes
	fmt.Fprintf(b, `<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#bbb"/>`, padLeft, baseY, padLeft+plotW, baseY)
	fmt.Fprintf(b, `<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#bbb"/>`, padLeft, top+12, padLeft, baseY)

	// labels
	fmt.Fprintf(b, `<text x="%d" y="%d" font-weight="bold" fill="%s">%s (%s)</text>`, padLeft, top+8, ser.color, ser.name, ser.unit)
	fmt.Fprintf(b, `<text x="%d" y="%d" text-anchor="end" fill="#666">%.1f</text>`, padLeft-6, top+16, maxV)
	fmt.Fprintf(b, `<text x="%d" y="%d" text-anchor="end" fill="#666">0</text>`, padLeft-6, baseY+4)
	fmt.Fprintf(b, `<text x="%d" y="%d" text-anchor="end" fill="#666">%.1fs</text>`, padLeft+plotW, baseY+18, maxX)

	// polyline
	b.WriteString(`<polyline fill="none" stroke="` + ser.color + `" stroke-width="1.5" points="`)
	for i, v := range ser.vals {
		x := float64(padLeft) + xs[i]/maxX*float64(plotW)
		y := float64(baseY) - v/maxV*float64(plotH)
		fmt.Fprintf(b, "%.1f,%.1f ", x, y)
	}
	b.WriteString(`"/>`)
}

func pluck(samples []Sample, f func(Sample) float64) []float64 {
	out := make([]float64, len(samples))
	for i, s := range samples {
		out[i] = f(s)
	}
	return out
}

func escapeXML(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;")
	return r.Replace(s)
}
