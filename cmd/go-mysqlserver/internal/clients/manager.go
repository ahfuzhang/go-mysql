package clients

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/cmd/go-mysqlserver/internal/config"
)

const defaultConnectTimeout = 10 * time.Second

// Manager builds upstream MySQL clients from config and matches incoming credentials.
type Manager struct {
	servers []config.Server
}

func NewManager(servers []config.Server) *Manager {
	copied := make([]config.Server, len(servers))
	copy(copied, servers)
	return &Manager{servers: copied}
}

func (m *Manager) Servers() []config.Server {
	copied := make([]config.Server, len(m.servers))
	copy(copied, m.servers)
	return copied
}

// Match returns the first server whose user/password match.
// If a host/port match is found, it is preferred but not required.
func (m *Manager) Match(localAddr net.Addr, user, password string) (*config.Server, bool) {
	host, port := splitHostPort(localAddr)
	var fallback *config.Server
	for _, srv := range m.servers {
		if srv.User != user || srv.Password != password {
			continue
		}
		if srv.Host != "" && host != "" && !strings.EqualFold(srv.Host, host) {
			if fallback == nil {
				match := srv
				fallback = &match
			}
			continue
		}
		if srv.Port != 0 && port != 0 && srv.Port != port {
			if fallback == nil {
				match := srv
				fallback = &match
			}
			continue
		}
		match := srv
		return &match, true
	}
	if fallback != nil {
		return fallback, true
	}
	return nil, false
}

// Connect creates a new upstream client connection for the given server.
func (m *Manager) Connect(ctx context.Context, srv config.Server) (*client.Conn, error) {
	connectTimeout, opts, err := buildOptions(srv)
	if err != nil {
		return nil, err
	}

	host := srv.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := srv.Port
	if port == 0 {
		port = 3306
	}

	addr := net.JoinHostPort(host, strconv.Itoa(port))
	conn, err := client.ConnectWithContext(ctx, addr, srv.User, srv.Password, srv.Database, connectTimeout, opts...)
	if err != nil {
		return nil, err
	}

	if srv.Charset != "" || srv.Collation != "" {
		charset := srv.Charset
		if charset == "" {
			charset = conn.GetCharset()
		}
		var stmt string
		if srv.Collation != "" {
			stmt = fmt.Sprintf("SET NAMES %s COLLATE %s", charset, srv.Collation)
		} else {
			stmt = fmt.Sprintf("SET NAMES %s", charset)
		}
		if _, err := conn.Execute(stmt); err != nil {
			conn.Close()
			return nil, err
		}
	}

	return conn, nil
}

func buildOptions(srv config.Server) (time.Duration, []client.Option, error) {
	connectTimeout := defaultConnectTimeout
	if srv.Timeout != "" {
		parsed, err := time.ParseDuration(srv.Timeout)
		if err != nil {
			return 0, nil, fmt.Errorf("invalid timeout %q: %w", srv.Timeout, err)
		}
		connectTimeout = parsed
	}

	opts := make([]client.Option, 0, 6)

	if srv.ReadTimeout != "" {
		parsed, err := time.ParseDuration(srv.ReadTimeout)
		if err != nil {
			return 0, nil, fmt.Errorf("invalid readTimeout %q: %w", srv.ReadTimeout, err)
		}
		opts = append(opts, func(c *client.Conn) error {
			c.ReadTimeout = parsed
			return nil
		})
	}

	if srv.WriteTimeout != "" {
		parsed, err := time.ParseDuration(srv.WriteTimeout)
		if err != nil {
			return 0, nil, fmt.Errorf("invalid writeTimeout %q: %w", srv.WriteTimeout, err)
		}
		opts = append(opts, func(c *client.Conn) error {
			c.WriteTimeout = parsed
			return nil
		})
	}

	if srv.Collation != "" {
		collation := srv.Collation
		opts = append(opts, func(c *client.Conn) error {
			return c.SetCollation(collation)
		})
	}

	switch normalizeTLSFlag(srv.TLS) {
	case "true":
		opts = append(opts, func(c *client.Conn) error {
			c.UseSSL(false)
			return nil
		})
	case "skip-verify":
		opts = append(opts, func(c *client.Conn) error {
			c.UseSSL(true)
			return nil
		})
	case "":
		// no TLS
	default:
		return 0, nil, fmt.Errorf("invalid tls value %q", srv.TLS)
	}

	return connectTimeout, opts, nil
}

func normalizeTLSFlag(value string) string {
	v := strings.ToLower(strings.TrimSpace(value))
	switch v {
	case "", "false", "0", "off", "disable", "disabled":
		return ""
	case "true", "1", "on", "enable", "enabled":
		return "true"
	case "skip-verify", "insecure":
		return "skip-verify"
	default:
		return v
	}
}

func splitHostPort(addr net.Addr) (string, int) {
	if addr == nil {
		return "", 0
	}
	host, portStr, err := net.SplitHostPort(addr.String())
	if err != nil {
		return "", 0
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return host, 0
	}
	return host, port
}
