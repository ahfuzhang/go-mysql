package handler

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/cmd/go-mysqlserver/internal/clients"
	"github.com/go-mysql-org/go-mysql/cmd/go-mysqlserver/internal/config"
	"github.com/go-mysql-org/go-mysql/mysql"
)

// ProxyHandler forwards requests to upstream MySQL when configured, otherwise falls back to MyHandler.
type ProxyHandler struct {
	manager  *clients.Manager
	fallback *MyHandler

	mu           sync.Mutex
	upstream     *client.Conn
	upstreamCfg  *config.Server
	pendingDB    string
	proxyEnabled bool
	modeSet      bool
}

func NewProxyHandler(manager *clients.Manager) *ProxyHandler {
	return &ProxyHandler{
		manager:  manager,
		fallback: &MyHandler{},
	}
}

func (h *ProxyHandler) SetUpstream(cfg config.Server) error {
	h.mu.Lock()
	if h.proxyEnabled {
		h.mu.Unlock()
		return nil
	}
	h.modeSet = true
	h.mu.Unlock()

	conn, err := h.manager.Connect(context.Background(), cfg)
	if err != nil {
		return err
	}

	h.mu.Lock()
	h.upstream = conn
	h.upstreamCfg = &cfg
	h.proxyEnabled = true
	pendingDB := h.pendingDB
	h.mu.Unlock()

	if pendingDB != "" && pendingDB != conn.GetDB() {
		if err := conn.UseDB(pendingDB); err != nil {
			return err
		}
	}

	log.Printf("Proxy connected upstream %s:%d user=%s db=%s", cfg.Host, cfg.Port, cfg.User, cfg.Database)
	return nil
}

func (h *ProxyHandler) SetFallback() {
	h.mu.Lock()
	h.proxyEnabled = false
	h.modeSet = true
	h.upstreamCfg = nil
	pendingDB := h.pendingDB
	h.mu.Unlock()

	if pendingDB != "" {
		_ = h.fallback.UseDB(pendingDB)
	}
}

func (h *ProxyHandler) UseDB(dbName string) error {
	h.mu.Lock()
	h.pendingDB = dbName
	upstream := h.upstream
	proxyEnabled := h.proxyEnabled
	modeSet := h.modeSet
	h.mu.Unlock()

	if upstream != nil {
		log.Printf("[proxy] UseDB %s", dbName)
		return upstream.UseDB(dbName)
	}

	log.Printf("[local] UseDB %s", dbName)
	if modeSet && !proxyEnabled {
		return h.fallback.UseDB(dbName)
	}
	return nil
}

func (h *ProxyHandler) HandleQuery(query string) (*mysql.Result, error) {
	if upstream := h.getUpstream(); upstream != nil {
		log.Printf("[proxy] Query: %s", query)
		result, err := upstream.Execute(query)
		if err != nil {
			log.Printf("[proxy] Query error: %v", err)
			return nil, err
		}
		logResult("[proxy] Query result", result)
		return result, nil
	}

	log.Printf("[local] Query: %s", query)
	result, err := h.fallback.HandleQuery(query)
	if err != nil {
		log.Printf("[local] Query error: %v", err)
		return nil, err
	}
	logResult("[local] Query result", result)
	return result, nil
}

func (h *ProxyHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	if upstream := h.getUpstream(); upstream != nil {
		log.Printf("[proxy] FieldList table=%s wildcard=%s", table, fieldWildcard)
		fields, err := upstream.FieldList(table, fieldWildcard)
		if err != nil {
			log.Printf("[proxy] FieldList error: %v", err)
			return nil, err
		}
		log.Printf("[proxy] FieldList result fields=%d", len(fields))
		return fields, nil
	}

	log.Printf("[local] FieldList table=%s wildcard=%s", table, fieldWildcard)
	return h.fallback.HandleFieldList(table, fieldWildcard)
}

func (h *ProxyHandler) HandleStmtPrepare(query string) (int, int, interface{}, error) {
	if upstream := h.getUpstream(); upstream != nil {
		log.Printf("[proxy] StmtPrepare: %s", query)
		stmt, err := upstream.Prepare(query)
		if err != nil {
			log.Printf("[proxy] StmtPrepare error: %v", err)
			return 0, 0, nil, err
		}
		log.Printf("[proxy] StmtPrepare ok params=%d columns=%d", stmt.Params, stmt.Columns)
		return stmt.Params, stmt.Columns, stmt, nil
	}

	log.Printf("[local] StmtPrepare: %s", query)
	return h.fallback.HandleStmtPrepare(query)
}

func (h *ProxyHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	if upstream := h.getUpstream(); upstream != nil {
		stmt, ok := context.(*client.Stmt)
		if !ok {
			return nil, fmt.Errorf("invalid stmt context %T", context)
		}
		log.Printf("[proxy] StmtExecute: %s args=%v", query, args)
		result, err := stmt.Execute(args...)
		if err != nil {
			log.Printf("[proxy] StmtExecute error: %v", err)
			return nil, err
		}
		logResult("[proxy] StmtExecute result", result)
		return result, nil
	}

	log.Printf("[local] StmtExecute: %s args=%v", query, args)
	result, err := h.fallback.HandleStmtExecute(context, query, args)
	if err != nil {
		log.Printf("[local] StmtExecute error: %v", err)
		return nil, err
	}
	logResult("[local] StmtExecute result", result)
	return result, nil
}

func (h *ProxyHandler) HandleStmtClose(context interface{}) error {
	if upstream := h.getUpstream(); upstream != nil {
		stmt, ok := context.(*client.Stmt)
		if !ok {
			return fmt.Errorf("invalid stmt context %T", context)
		}
		log.Printf("[proxy] StmtClose")
		return stmt.Close()
	}

	log.Printf("[local] StmtClose")
	return h.fallback.HandleStmtClose(context)
}

func (h *ProxyHandler) HandleOtherCommand(cmd byte, data []byte) error {
	log.Printf("[other] cmd=%x data=%x", cmd, data)
	if cmd == mysql.COM_SET_OPTION {
		return nil
	}
	return mysql.NewError(
		mysql.ER_UNKNOWN_ERROR,
		fmt.Sprintf("command %d is not supported now", cmd),
	)
}

func (h *ProxyHandler) Close() error {
	h.mu.Lock()
	upstream := h.upstream
	h.upstream = nil
	h.mu.Unlock()

	if upstream != nil {
		log.Printf("[proxy] Closing upstream connection")
		upstream.Close()
	}
	return nil
}

func (h *ProxyHandler) getUpstream() *client.Conn {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.proxyEnabled {
		return nil
	}
	return h.upstream
}

func logResult(prefix string, result *mysql.Result) {
	if result == nil {
		log.Printf("%s: <nil>", prefix)
		return
	}
	if result.IsStreaming() {
		log.Printf("%s: streaming", prefix)
		return
	}
	if result.HasResultset() {
		rows := len(result.RowDatas)
		cols := len(result.Fields)
		log.Printf("%s: resultset rows=%d cols=%d", prefix, rows, cols)
		return
	}
	log.Printf("%s: affected=%d insertId=%d status=%d", prefix, result.AffectedRows, result.InsertId, result.Status)
}
