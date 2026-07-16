// Package report captures a benchmark run into a run directory that mirrors the
// JS runner's layout (results/runs/<stamp>[-label]/) for cross-stack comparison:
// config snapshot, env.txt, <mode>-sweep.json, summary.txt, per-part CSVs, and an
// optional SVG plot — optionally mirrored to S3.
package report

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/goabv/s3-benchmark-go/internal/bench"
	"github.com/goabv/s3-benchmark-go/internal/config"
	"github.com/goabv/s3-benchmark-go/internal/metrics"
)

// Options carries everything needed to capture one run. A run may contain more
// than one sweep (e.g. mode "both" -> a download and an upload sweep sharing one
// run directory), mirroring the JS runner.
type Options struct {
	Config    *config.Config
	Mode      string // requested mode: download | upload | both
	Runs      []*bench.RunResult
	Samples   []metrics.Sample
	Stalls    int
	StartedAt time.Time
	Label     string
	Summary   string // captured console output
}

// Artifacts reports where the run was written and (if uploaded) its S3 location.
type Artifacts struct {
	Dir      string
	Files    []string
	S3Bucket string
	S3Prefix string
	Uploaded bool
}

// sweep is the top-level <mode>-sweep.json document.
type sweep struct {
	GoVersion  string      `json:"goVersion"`
	SdkVersion string      `json:"sdkVersion"`
	Config     interface{} `json:"config"`
	Results    interface{} `json:"results"`
}

// Write captures the run to disk and, when results.upload is set, mirrors the run
// directory to S3.
func Write(ctx context.Context, client *s3.Client, o Options) (*Artifacts, error) {
	cfg := o.Config
	stamp := o.StartedAt.UTC().Format("20060102T150405")
	name := stamp
	if o.Label != "" {
		name += "-" + o.Label
	}
	dir := filepath.Join(cfg.Results.Dir, "runs", name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create run dir: %w", err)
	}
	art := &Artifacts{Dir: dir}

	// Resources are sampled and applied per phase by the caller (so upload and
	// download numbers stay distinct); Write does not overwrite them.

	// 1) config.json — snapshot of the config used.
	cfgBytes, _ := json.MarshalIndent(cfg, "", "  ")
	if err := writeFile(art, dir, "config.json", cfgBytes); err != nil {
		return art, err
	}

	// 2) env.txt
	if err := writeFile(art, dir, "env.txt", []byte(buildEnvText(o.Mode, o.Label, o.StartedAt))); err != nil {
		return art, err
	}

	// 3) one <mode>-sweep.json per sweep; parttimes CSV for download sweeps.
	csvStamp := o.StartedAt.UTC().Format("2006-01-02T1504")
	for _, r := range o.Runs {
		sweepBytes, err := marshalSweep(cfg, r)
		if err != nil {
			return art, fmt.Errorf("marshal %s sweep: %w", r.Mode, err)
		}
		if err := writeFile(art, dir, r.Mode+"-sweep.json", sweepBytes); err != nil {
			return art, err
		}

		if r.Mode == "download" {
			for _, g := range r.Groups {
				if len(g.Parts_) == 0 {
					continue
				}
				csvBytes, err := buildCSV(g, "download_ms")
				if err != nil {
					return art, err
				}
				fname := fmt.Sprintf("parttimes-%s-%s.csv", sanitize(g.Label), csvStamp)
				if err := writeFile(art, dir, fname, csvBytes); err != nil {
					return art, err
				}
			}
		}
	}

	// 4) summary.txt — the captured console output (already includes the tables)
	if err := writeFile(art, dir, "summary.txt", []byte(o.Summary)); err != nil {
		return art, err
	}

	// 5) optional SVG time-series plot for the whole run
	if cfg.Results.Plot {
		svg := metrics.PlotSVG(o.Samples, fmt.Sprintf("%s  %s  (%s)", o.Mode, cfg.Bucket, stamp))
		if err := writeFile(art, dir, "timeseries.svg", svg); err != nil {
			return art, err
		}
	}

	// 7) mirror to S3
	if cfg.Results.Upload {
		if err := uploadDir(ctx, client, cfg, name, art); err != nil {
			return art, err
		}
	}
	return art, nil
}

func marshalSweep(cfg *config.Config, run *bench.RunResult) ([]byte, error) {
	return json.MarshalIndent(sweep{
		GoVersion:  runtimeVersion(),
		SdkVersion: sdkVersion(),
		Config:     cfg,
		Results:    buildViews(run),
	}, "", "  ")
}

// WriteSweep writes a single standalone sweep JSON to path (the scratch output of
// the sweep-* scripts), without the full run-directory capture. Resources should
// already be applied to the run's groups by the caller.
func WriteSweep(path string, cfg *config.Config, run *bench.RunResult) error {
	b, err := marshalSweep(cfg, run)
	if err != nil {
		return fmt.Errorf("marshal sweep: %w", err)
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, b, 0o644)
}

func writeFile(art *Artifacts, dir, name string, data []byte) error {
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	art.Files = append(art.Files, name)
	return nil
}

func uploadDir(ctx context.Context, client *s3.Client, cfg *config.Config, runName string, art *Artifacts) error {
	bucket := cfg.Results.Bucket
	if bucket == "" {
		bucket = cfg.Bucket
	}
	prefix := strings.TrimSuffix(cfg.Results.Prefix, "/") + "/" + runName + "/"
	for _, name := range art.Files {
		data, err := os.ReadFile(filepath.Join(art.Dir, name))
		if err != nil {
			return err
		}
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(prefix + name),
			Body:        bytes.NewReader(data),
			ContentType: aws.String(contentType(name)),
		}); err != nil {
			return fmt.Errorf("upload %s: %w", name, err)
		}
	}
	art.S3Bucket, art.S3Prefix, art.Uploaded = bucket, prefix, true
	return nil
}

func buildCSV(g bench.GroupResult, msCol string) ([]byte, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write([]string{"iter", "key", "part_number", "bytes", msCol, "vip", "conn_id"})
	for _, r := range g.Parts_ {
		_ = w.Write([]string{
			strconv.Itoa(r.Iter),
			r.Key,
			strconv.FormatInt(int64(r.Part), 10),
			strconv.FormatInt(r.Bytes, 10),
			strconv.FormatFloat(r.Ms, 'f', 2, 64),
			r.VIP,
			r.ConnID,
		})
	}
	w.Flush()
	return buf.Bytes(), w.Error()
}

func contentType(name string) string {
	switch {
	case strings.HasSuffix(name, ".json"):
		return "application/json"
	case strings.HasSuffix(name, ".svg"):
		return "image/svg+xml"
	case strings.HasSuffix(name, ".csv"):
		return "text/csv"
	default:
		return "text/plain"
	}
}

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.':
			return r
		default:
			return -1
		}
	}, s)
}
