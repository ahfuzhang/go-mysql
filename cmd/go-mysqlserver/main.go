package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/go-mysql-org/go-mysql/cmd/go-mysqlserver/handler"
	"github.com/go-mysql-org/go-mysql/cmd/go-mysqlserver/internal/clients"
	"github.com/go-mysql-org/go-mysql/cmd/go-mysqlserver/internal/config"
	"github.com/go-mysql-org/go-mysql/server"
)

func eachConnection(c net.Conn, manager *clients.Manager, fallbackUser, fallbackPass string) {
	defer c.Close()
	log.Printf("Accepted connection from %s", c.RemoteAddr())

	proxyHandler := handler.NewProxyHandler(manager)
	authHandler := clients.NewAuthHandler(manager, proxyHandler, fallbackUser, fallbackPass)

	// Create a connection with customized authentication and proxy handler.
	srv := server.NewDefaultServer()
	conn, err := srv.NewCustomizedConn(c, authHandler, proxyHandler)
	if err != nil {
		//log.Fatal(err)
		log.Println(err)
		return
	}

	log.Println("Registered the connection with the server")

	// as long as the client keeps sending commands, keep handling them
	for {
		if err := conn.HandleCommand(); err != nil {
			log.Println(err)
			return
		}
	}
}

var (
	port   = flag.Int("port", 4000, "port to listen on")
	user   = flag.String("user", "root", "fallback mysql username")
	passwd = flag.String("passwd", "123456", "fallback mysql password")

	appConfig *config.Config
)

func main() {
	flag.Parse()

	var err error
	appConfig, err = config.Load("config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Loaded %d server config(s)", len(appConfig.Servers))

	addr := fmt.Sprintf(":%d", *port)

	// Listen for connections on localhost port
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Listening on %s, connect with 'mysql -h 127.0.0.1 -P %d -u %s'", addr, *port, *user)

	manager := clients.NewManager(appConfig.Servers)

	for {
		// Accept a new connection once
		c, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go eachConnection(c, manager, *user, *passwd)
	}
}
