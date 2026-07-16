package report

import (
	"strings"

	"github.com/goabv/s3-benchmark-go/internal/bench"
)

// downloadView is the per-group shape of a download-sweep.json result entry.
type downloadView struct {
	Label                  string              `json:"label"`
	Files                  int                 `json:"files"`
	PerFileSize            int64               `json:"perFileSize"`
	Size                   int64               `json:"size"`
	Parts                  int                 `json:"parts"`
	ChecksumAlgo           string              `json:"checksumAlgo"`
	ChecksumValidated      bool                `json:"checksumValidated"`
	DeliveryMode           string              `json:"deliveryMode"`
	PartsChecksummedPerRun int                 `json:"partsChecksummedPerRun"`
	Workers                int                 `json:"workers"`
	Concurrency            int                 `json:"concurrency"`
	TotalInFlight          int                 `json:"totalInFlight"`
	Iterations             int                 `json:"iterations"`
	Samples                []bench.Sample3     `json:"samples"`
	Median                 bench.Sample3       `json:"median"`
	Best                   bench.Sample3       `json:"best"`
	Resources              bench.Resources     `json:"resources"`
	ConnectionSpread       interface{}         `json:"connectionSpread"`
	IPThroughput           interface{}         `json:"ipThroughput"`
	PartTimeStats          bench.PartTimeStats `json:"partTimeStats"`
	TLSInfo                bench.TLSInfo       `json:"tlsInfo"`
}

// uploadView is the per-group shape of an upload-sweep.json result entry.
type uploadView struct {
	Label         string          `json:"label"`
	Files         int             `json:"files"`
	PerFileSize   int64           `json:"perFileSize"`
	Size          int64           `json:"size"`
	Parts         int             `json:"parts"`
	PartSize      int64           `json:"partSize"`
	Checksum      string          `json:"checksum"`
	UploadSource  string          `json:"uploadSource"`
	Workers       int             `json:"workers"`
	Concurrency   int             `json:"concurrency"`
	TotalInFlight int             `json:"totalInFlight"`
	Iterations    int             `json:"iterations"`
	Samples       []bench.Sample3 `json:"samples"`
	Median        bench.Sample3   `json:"median"`
	Best          bench.Sample3   `json:"best"`
	Resources     bench.Resources `json:"resources"`
	IPThroughput  interface{}     `json:"ipThroughput"`
	TLSInfo       bench.TLSInfo   `json:"tlsInfo"`
}

// buildViews projects the mode-agnostic GroupResults onto the mode-specific sweep
// JSON shapes, matching the JS runner's field sets.
func buildViews(run *bench.RunResult) interface{} {
	if strings.Contains(run.Mode, "upload") {
		out := make([]uploadView, 0, len(run.Groups))
		for _, g := range run.Groups {
			out = append(out, uploadView{
				Label:         g.Label,
				Files:         g.Files,
				PerFileSize:   g.PerFileSize,
				Size:          g.Size,
				Parts:         g.Parts,
				PartSize:      g.PartSize,
				Checksum:      g.Checksum,
				UploadSource:  g.UploadSource,
				Workers:       g.Workers,
				Concurrency:   g.Concurrency,
				TotalInFlight: g.InFlight,
				Iterations:    g.Iterations,
				Samples:       g.Samples,
				Median:        g.Median,
				Best:          g.Best,
				Resources:     g.Resources,
				TLSInfo:       g.TLS,
			})
		}
		return out
	}

	out := make([]downloadView, 0, len(run.Groups))
	for _, g := range run.Groups {
		out = append(out, downloadView{
			Label:                  g.Label,
			Files:                  g.Files,
			PerFileSize:            g.PerFileSize,
			Size:                   g.Size,
			Parts:                  g.Parts,
			ChecksumAlgo:           g.ChecksumAlgo,
			ChecksumValidated:      g.ChecksumValidated,
			DeliveryMode:           g.DeliveryMode,
			PartsChecksummedPerRun: g.PartsChecksummedPerRun,
			Workers:                g.Workers,
			Concurrency:            g.Concurrency,
			TotalInFlight:          g.InFlight,
			Iterations:             g.Iterations,
			Samples:                g.Samples,
			Median:                 g.Median,
			Best:                   g.Best,
			Resources:              g.Resources,
			PartTimeStats:          g.PartTime,
			TLSInfo:                g.TLS,
		})
	}
	return out
}
