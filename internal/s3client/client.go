// Package s3client builds an S3 client tuned for high-throughput parallel ranged
// GETs: a keep-alive HTTP/1.1 transport sized for the intended concurrency, and
// an optional connection-spreading dialer that fans sockets across all of S3's
// front-end IPs (the Go analogue of the JS project's custom DNS lookup).
package s3client

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Options controls how the client (and its transport) is built.
type Options struct {
	Region            string
	MaxConns          int  // max idle keep-alive conns per host (size to your concurrency)
	SpreadConnections bool // round-robin sockets across all resolved S3 IPs
	TLS               bool // false -> plaintext HTTP endpoint (measure TLS overhead)
	// LocalIPs, when non-empty, round-robins each outbound connection's SOURCE
	// address across these local IPs to spread traffic across multiple network
	// cards (ENIs). Requires host-side source-based policy routing (see
	// scripts/setup-multinic.sh) for a connection to actually egress the matching
	// card. Empty = single default source (today's behavior).
	LocalIPs []string
}

// New constructs an *s3.Client with a transport tuned per opts.
func New(ctx context.Context, opts Options) (*s3.Client, error) {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          0, // unlimited
		MaxIdleConnsPerHost:   opts.MaxConns,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		// S3 speaks HTTP/1.1; disabling h2 keeps parity with the JS undici h1 path
		// and lets many parallel sockets fan out across front-end IPs.
		ForceAttemptHTTP2: false,
		TLSClientConfig:   &tls.Config{},
	}

	base := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	transport.DialContext = newDialer(base, parseLocalIPs(opts.LocalIPs), opts.SpreadConnections)

	httpClient := &http.Client{Transport: transport}

	loadOpts := []func(*awscfg.LoadOptions) error{
		awscfg.WithRegion(opts.Region),
		awscfg.WithHTTPClient(httpClient),
	}
	cfg, err := awscfg.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if !opts.TLS {
			o.EndpointOptions.DisableHTTPS = true
		}
	}), nil
}

// newDialer builds a DialContext that optionally (a) round-robins each
// connection's SOURCE address across localIPs (multi-NIC spreading across ENIs)
// and (b) round-robins the DESTINATION across the target host's resolved IPs (S3
// front-end spreading). Either, both, or neither may be active.
//
// TLS still runs against the original hostname: the transport derives SNI/cert
// validation from the dial address's host; this dialer only changes the source
// address and/or which resolved IP the raw socket connects to.
func newDialer(base *net.Dialer, localIPs []net.IP, spread bool) func(context.Context, string, string) (net.Conn, error) {
	var li uint64
	pickDest := newDestPicker()
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		d := *base // copy so per-connection LocalAddr doesn't race
		if len(localIPs) > 0 {
			ip := localIPs[atomic.AddUint64(&li, 1)%uint64(len(localIPs))]
			d.LocalAddr = &net.TCPAddr{IP: ip}
		}
		if !spread {
			return d.DialContext(ctx, network, addr)
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return d.DialContext(ctx, network, addr)
		}
		if ip := pickDest(host); ip != "" {
			return d.DialContext(ctx, network, net.JoinHostPort(ip, port))
		}
		return d.DialContext(ctx, network, addr)
	}
}

// newDestPicker resolves a host's A-records and round-robins across them (short
// TTL cache) so N concurrent connections fan out over many S3 front-end IPs.
func newDestPicker() func(host string) string {
	const ttl = time.Second
	type entry struct {
		ips []string
		ts  time.Time
		idx int
	}
	var mu sync.Mutex
	cache := map[string]*entry{}
	return func(host string) string {
		mu.Lock()
		defer mu.Unlock()
		e := cache[host]
		if e == nil || len(e.ips) == 0 || time.Since(e.ts) > ttl {
			ips, err := net.LookupHost(host)
			if err != nil || len(ips) == 0 {
				return "" // caller falls back to the plain dial
			}
			idx := 0
			if e != nil {
				idx = e.idx
			}
			e = &entry{ips: ips, ts: time.Now(), idx: idx}
			cache[host] = e
		}
		ip := e.ips[e.idx%len(e.ips)]
		e.idx++
		return ip
	}
}

// parseLocalIPs parses IP strings, silently dropping any that don't parse.
func parseLocalIPs(ss []string) []net.IP {
	var out []net.IP
	for _, s := range ss {
		if ip := net.ParseIP(strings.TrimSpace(s)); ip != nil {
			out = append(out, ip)
		}
	}
	return out
}

// LocalIPv4s returns the host's usable IPv4 addresses (global-unicast,
// non-loopback, non-link-local) — typically one per attached ENI. Used to
// auto-populate Options.LocalIPs for multi-NIC spreading.
func LocalIPv4s() []string {
	var out []string
	ifaces, err := net.Interfaces()
	if err != nil {
		return out
	}
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			var ip net.IP
			switch v := a.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			ip4 := ip.To4()
			if ip4 == nil || !ip.IsGlobalUnicast() || ip.IsLinkLocalUnicast() {
				continue
			}
			out = append(out, ip4.String())
		}
	}
	return out
}
