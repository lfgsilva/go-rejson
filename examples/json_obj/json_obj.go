package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/nitishm/go-rejson/v4"
	"github.com/nitishm/go-rejson/v4/rjs"

	goredis "github.com/go-redis/redis/v8"
	"github.com/gomodule/redigo/redis"
)

var ctx = context.Background()

// ExampleJSONObj demonstrates how to write a JSON Object to Redis
func ExampleJSONObj(rh *rejson.Handler) {

	type Object struct {
		Name      string `json:"name"`
		LastSeen  int64  `json:"lastSeen"`
		LoggedOut bool   `json:"loggedOut"`
	}

	obj := Object{"Leonard Cohen", 1478476800, true}
	res, err := rh.JSONSet("obj", ".", obj)
	if err != nil {
		log.Fatalf("Failed to JSONSet")
		return
	}
	fmt.Println("obj:", res)

	res, err = rh.JSONGet("obj", ".")
	if err != nil {
		log.Fatalf("Failed to JSONGet")
		return
	}
	var objOut Object
	err = json.Unmarshal(res.([]byte), &objOut)
	if err != nil {
		log.Fatalf("Failed to JSON Unmarshal")
		return
	}
	fmt.Println("got obj:", objOut)

	res, err = rh.JSONObjLen("obj", ".")
	if err != nil {
		log.Fatalf("Failed to JSONObjLen")
		return
	}
	fmt.Println("length:", res)

	res, err = rh.JSONObjKeys("obj", ".")
	if err != nil {
		log.Fatalf("Failed to JSONObjKeys")
		return
	}
	fmt.Println("keys:", res)

	res, err = rh.JSONDebug(rjs.DebugHelpSubcommand, "obj", ".")
	if err != nil {
		log.Fatalf("Failed to JSONDebug")
		return
	}
	fmt.Println(res)
	res, err = rh.JSONDebug(rjs.DebugMemorySubcommand, "obj", ".")
	if err != nil {
		log.Fatalf("Failed to JSONDebug")
		return
	}
	fmt.Println("Memory used by obj:", res)

	res, err = rh.JSONGet("obj", ".",
		rjs.GETOptionINDENT, rjs.GETOptionNEWLINE,
		rjs.GETOptionNOESCAPE, rjs.GETOptionSPACE)
	if err != nil {
		log.Fatalf("Failed to JSONGet")
		return
	}
	err = json.Unmarshal(res.([]byte), &objOut)
	if err != nil {
		log.Fatalf("Failed to JSON Unmarshal")
		return
	}
	fmt.Println("got obj with options:", objOut)
}

func main() {
	var addr = flag.String("Server", "localhost:6379", "Redis server address")
	var addrs = []string{"localhost:7001", "localhost:7002", "localhost:7003", "localhost:7004", "localhost:7005", "localhost:7006"}

	rh := rejson.NewReJSONHandler()
	flag.Parse()

	// Redigo Client
	conn, err := redis.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to connect to redis-server @ %s", *addr)
	}
	defer func() {
		_, err = conn.Do("FLUSHALL")
		err = conn.Close()
		if err != nil {
			log.Fatalf("Failed to communicate to redis-server @ %v", err)
		}
	}()
	rh.SetRedigoClient(conn)
	fmt.Println("Executing Example_JSONSET for Redigo Client")
	ExampleJSONObj(rh)

	// GoRedis Client
	cli := goredis.NewClient(&goredis.Options{Addr: *addr})
	defer func() {
		if err := cli.FlushAll(ctx).Err(); err != nil {
			log.Fatalf("goredis - failed to flush: %v", err)
		}
		if err := cli.Close(); err != nil {
			log.Fatalf("goredis - failed to communicate to redis-server: %v", err)
		}
	}()
	rh.SetGoRedisClient(cli)
	fmt.Println("\nExecuting Example_JSONSET for goredis Client")
	ExampleJSONObj(rh)

	// GoRedis Cluster Client
	clustercli := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs: addrs,
	})
	defer func() {
		err := clustercli.ForEachMaster(clustercli.Context(), func(ctx context.Context, master *goredis.Client) error {
			master.FlushAll(ctx)
			return nil
		})
		if err != nil {
			log.Fatalf("goredis-cluster - failed to flush: %v", err)
		}

		if err := clustercli.Close(); err != nil {
			log.Fatalf("goredis-cluster - failed to communicate to redis-server: %v", err)
		}
	}()
	rh.SetGoRedisClient(clustercli)
	fmt.Println("\nExecuting Example_JSONSET for goredis Cluster Client")
	ExampleJSONObj(rh)
}
