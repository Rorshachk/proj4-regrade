package main

import (
	"cse224/proj4/pkg/surfstore"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

// Usage String
const USAGE_STRING = "./run-server.sh -s <service_type> -p <port> -l -d (blockStoreAddr*)"

// Set of valid services
var SERVICE_TYPES = map[string]bool{"meta": true, "block": true, "both": true}

// Exit codes
const EX_USAGE int = 64

func main() {

	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%s: %v\n", f.Name, f.Usage)
		})
		fmt.Fprintf(w, "  (blockStoreAddr*): BlockStore Address (include self if service type is both)\n")
	}

	// Parse command-line argument flags
	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both")
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	// Use tail arguments to hold BlockStore address
	args := flag.Args()
	blockStoreAddr := ""
	if len(args) == 1 {
		blockStoreAddr = args[0]
	}

	// Valid service type argument
	if _, ok := SERVICE_TYPES[strings.ToLower(*service)]; !ok {
		flag.Usage()
		os.Exit(EX_USAGE)
	}

	// Add localhost if necessary
	addr := ""
	if *localOnly {
		addr += "localhost"
	}
	addr += ":" + strconv.Itoa(*port)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	} else {
		logFile, err := os.OpenFile(fmt.Sprintf("/home/rorshach/Projects/courses/CSE124/proj4-Rorshachk/logs/%v.log", service), os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		log.SetOutput(logFile)
	}
	log.Println("Hello world")
	log.Fatal(startServer(addr, strings.ToLower(*service), blockStoreAddr))
}

func startServer(hostAddr string, serviceType string, blockStoreAddr string) error {
	listen, err := net.Listen("tcp", hostAddr)
	grpc_server := grpc.NewServer()
	if err != nil {
		panic(err)
	}
	if serviceType == "block" {
		surfstore.RegisterBlockStoreServer(grpc_server, surfstore.NewBlockStore())
	} else if serviceType == "meta" {
		surfstore.RegisterMetaStoreServer(grpc_server, surfstore.NewMetaStore(blockStoreAddr))
	} else if serviceType == "both" {
		surfstore.RegisterBlockStoreServer(grpc_server, surfstore.NewBlockStore())
		surfstore.RegisterMetaStoreServer(grpc_server, surfstore.NewMetaStore(blockStoreAddr))
	} else {
		return errors.New("Unknown service type.")
	}

	return grpc_server.Serve(listen)
}
