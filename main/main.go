package main

import (
	"fmt"
	"server"
)

func main() {
	fmt.Println("Commit Log Data Structure")
	httpServer := server.HttpServer(":8080")
	httpServer.ListenAndServe()
}
