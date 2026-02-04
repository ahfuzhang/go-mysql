package clients

import (
	"fmt"
	"log"
	"sort"

	"github.com/go-mysql-org/go-mysql/cmd/go-mysqlserver/internal/config"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

// Selector is implemented by handlers that can switch between proxy and fallback modes.
type Selector interface {
	SetUpstream(cfg config.Server) error
	SetFallback()
}

// AuthHandler authenticates users and selects the upstream target on success.
type AuthHandler struct {
	creds    map[string]server.Credential
	manager  *Manager
	selector Selector
}

func NewAuthHandler(manager *Manager, selector Selector, fallbackUser, fallbackPassword string) *AuthHandler {
	creds := make(map[string]server.Credential)
	addCredential := func(user, password string) {
		if user == "" {
			return
		}
		credential := creds[user]
		credential.AuthPluginName = mysql.AUTH_NATIVE_PASSWORD
		credential.Passwords = append(credential.Passwords, password)
		creds[user] = credential
	}

	for _, srv := range manager.Servers() {
		addCredential(srv.User, srv.Password)
	}
	addCredential(fallbackUser, fallbackPassword)

	for user := range creds {
		unique := make(map[string]struct{})
		passwords := creds[user].Passwords
		creds[user] = server.Credential{
			AuthPluginName: creds[user].AuthPluginName,
			Passwords:      uniqueStrings(passwords, unique),
		}
	}

	return &AuthHandler{
		creds:    creds,
		manager:  manager,
		selector: selector,
	}
}

func (h *AuthHandler) GetCredential(username string) (server.Credential, bool, error) {
	cred, ok := h.creds[username]
	if !ok {
		return server.Credential{}, false, nil
	}
	return cred, true, nil
}

func (h *AuthHandler) OnAuthSuccess(conn *server.Conn) error {
	user := conn.GetUser()
	password := conn.AuthPassword()

	if srv, ok := h.manager.Match(conn.LocalAddr(), user, password); ok {
		log.Printf("Auth matched upstream user=%s host=%s port=%d", srv.User, srv.Host, srv.Port)
		if err := h.selector.SetUpstream(*srv); err != nil {
			return fmt.Errorf("connect upstream: %w", err)
		}
		return nil
	}

	log.Printf("Auth fell back to local handler user=%s", user)
	h.selector.SetFallback()
	return nil
}

func (h *AuthHandler) OnAuthFailure(conn *server.Conn, err error) {
	log.Printf("Auth failed for user=%s from=%s: %v", conn.GetUser(), conn.RemoteAddr(), err)
}

func uniqueStrings(values []string, seen map[string]struct{}) []string {
	unique := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	sort.Strings(unique)
	return unique
}
