package bench

import (
	"context"
	"crypto/tls"
	"net"
	"net/http/httptrace"
	"sync"
)

// connInfo captures which S3 front-end IP served a request, whether the TCP
// connection was reused, and the negotiated TLS parameters — the Go analogue of
// the JS runner's vip/conn-id and tlsInfo capture.
type connInfo struct {
	remoteIP  string
	reused    bool
	tlsProto  string
	tlsCipher string
}

// withConnTrace attaches an httptrace that records the served connection's remote
// address, reuse flag, and negotiated TLS into ci. The returned context must be
// used for the request.
func withConnTrace(ctx context.Context, ci *connInfo) context.Context {
	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			ci.reused = info.Reused
			if info.Conn != nil && info.Conn.RemoteAddr() != nil {
				ci.remoteIP = info.Conn.RemoteAddr().String()
			}
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			if err != nil {
				return
			}
			ci.tlsProto = tlsVersionName(state.Version)
			ci.tlsCipher = tls.CipherSuiteName(state.CipherSuite)
		},
	}
	return httptrace.WithClientTrace(ctx, trace)
}

func tlsVersionName(v uint16) string {
	switch v {
	case tls.VersionTLS13:
		return "TLSv1.3"
	case tls.VersionTLS12:
		return "TLSv1.2"
	case tls.VersionTLS11:
		return "TLSv1.1"
	case tls.VersionTLS10:
		return "TLSv1.0"
	default:
		return "unknown"
	}
}

// ipStats aggregates distinct served IPs, the connection-reuse ratio, and the
// negotiated TLS parameters across a run.
type ipStats struct {
	mu        sync.Mutex
	ips       map[string]int
	reused    int
	total     int
	tlsProto  string
	tlsCipher string
}

func newIPStats() *ipStats { return &ipStats{ips: map[string]int{}} }

func (s *ipStats) record(ci connInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.total++
	if ci.reused {
		s.reused++
	}
	if ci.remoteIP != "" {
		host := ci.remoteIP
		if h, _, err := net.SplitHostPort(ci.remoteIP); err == nil {
			host = h
		}
		s.ips[host]++
	}
	if s.tlsProto == "" && ci.tlsProto != "" {
		s.tlsProto, s.tlsCipher = ci.tlsProto, ci.tlsCipher
	}
}

// summary returns the number of distinct front-end IPs seen and the fraction of
// requests that reused an existing connection.
func (s *ipStats) summary() (distinct int, reuseRatio float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	distinct = len(s.ips)
	if s.total > 0 {
		reuseRatio = float64(s.reused) / float64(s.total)
	}
	return distinct, reuseRatio
}

// tls returns the negotiated protocol and cipher (empty when TLS is disabled).
func (s *ipStats) tls() (proto, cipher string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tlsProto, s.tlsCipher
}
