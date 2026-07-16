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
	"sync"
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
	if opts.SpreadConnections {
		transport.DialContext = spreadDialer(base)
	} else {
		transport.DialContext = base.DialContext
	}

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

// spreadDialer resolves the target host's A-records and round-robins new sockets
// across them so N concurrent connections fan out over many S3 front-ends instead
// of piling onto one IP. TLS still runs against the original hostname: the
// transport derives SNI/cert validation from the dial address's host, while this
// dialer only changes which IP the raw socket connects to.
func spreadDialer(base *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	const ttl = time.Second
	type entry struct {
		ips []string
		ts  time.Time
		idx int
	}
	var mu sync.Mutex
	cache := map[string]*entry{}

	pick := func(host string) string {
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

	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return base.DialContext(ctx, network, addr)
		}
		if ip := pick(host); ip != "" {
			return base.DialContext(ctx, network, net.JoinHostPort(ip, port))
		}
		return base.DialContext(ctx, network, addr)
	}
}
