package report

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/goabv/s3-benchmark-go/internal/metrics"
)

// buildEnvText produces the env.txt contents: timestamp, mode/label, host, Go and
// OS details, CPU/mem, EC2 instance-type/AZ (best-effort via IMDS), and the key
// network sysctls. Anything unavailable on the platform is left blank.
func buildEnvText(mode, label string, startedAt time.Time) string {
	host, _ := os.Hostname()
	var b strings.Builder
	fmt.Fprintf(&b, "timestamp:   %s\n", startedAt.UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "mode:        %s\n", mode)
	fmt.Fprintf(&b, "label:       %s\n", label)
	fmt.Fprintf(&b, "host:        %s\n", host)
	fmt.Fprintf(&b, "go:          %s\n", runtime.Version())
	fmt.Fprintf(&b, "os:          %s %s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(&b, "cpus:        %d\n", runtime.NumCPU())
	if mem := metrics.TotalMemBytes(); mem > 0 {
		fmt.Fprintf(&b, "mem:         %s\n", humanBytes(mem))
	}

	itype, az := imdsInstance()
	if itype != "" {
		fmt.Fprintf(&b, "\ninstance-type: %s\n", itype)
	}
	if az != "" {
		fmt.Fprintf(&b, "az:            %s\n", az)
	}

	if sc := networkSysctls(); sc != "" {
		fmt.Fprintf(&b, "--- sysctl (network) ---\n%s", sc)
	}
	return b.String()
}

// networkSysctls reads a handful of relevant tunables from /proc/sys (Linux only).
func networkSysctls() string {
	keys := []struct{ label, path string }{
		{"net.ipv4.tcp_congestion_control", "/proc/sys/net/ipv4/tcp_congestion_control"},
		{"net.core.default_qdisc", "/proc/sys/net/core/default_qdisc"},
		{"net.ipv4.tcp_slow_start_after_idle", "/proc/sys/net/ipv4/tcp_slow_start_after_idle"},
		{"net.core.rmem_max", "/proc/sys/net/core/rmem_max"},
		{"net.core.wmem_max", "/proc/sys/net/core/wmem_max"},
	}
	var b strings.Builder
	for _, k := range keys {
		if data, err := os.ReadFile(k.path); err == nil {
			fmt.Fprintf(&b, "  %s = %s\n", k.label, strings.TrimSpace(string(data)))
		}
	}
	return b.String()
}

// imdsInstance fetches instance-type and AZ via IMDSv2 with a short timeout. On
// non-EC2 hosts the call fails fast and returns empty strings.
func imdsInstance() (instanceType, az string) {
	client := &http.Client{Timeout: 400 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	tokReq, _ := http.NewRequestWithContext(ctx, http.MethodPut, "http://169.254.169.254/latest/api/token", nil)
	tokReq.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "60")
	tokResp, err := client.Do(tokReq)
	if err != nil {
		return "", ""
	}
	tokBytes, _ := io.ReadAll(tokResp.Body)
	tokResp.Body.Close()
	token := string(tokBytes)

	get := func(path string) string {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://169.254.169.254/latest/meta-data/"+path, nil)
		req.Header.Set("X-aws-ec2-metadata-token", token)
		resp, err := client.Do(req)
		if err != nil {
			return ""
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return ""
		}
		v, _ := io.ReadAll(resp.Body)
		return strings.TrimSpace(string(v))
	}
	return get("instance-type"), get("placement/availability-zone")
}

func runtimeVersion() string { return runtime.Version() }

// sdkVersion extracts the aws-sdk-go-v2 s3 module version from the build info.
func sdkVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/aws/aws-sdk-go-v2/service/s3" {
			return dep.Version
		}
	}
	return ""
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
