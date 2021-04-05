package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	goredis "github.com/go-redis/redis/v8"
	"github.com/gomodule/redigo/redis"
	"github.com/nitishm/go-rejson/v4"
)

var ctx = context.Background()

// Name - student name
type Name struct {
	First  string `json:"first,omitempty"`
	Middle string `json:"middle,omitempty"`
	Last   string `json:"last,omitempty"`
}

// Student - student object
type Student struct {
	Name Name `json:"name,omitempty"`
	Rank int  `json:"rank,omitempty"`
}

// ExampleJSONSet demonstrates how to write a simpler struct to Redis
func ExampleJSONSet(rh *rejson.Handler) {

	student := Student{
		Name: Name{
			"Mark",
			"S",
			"Pronto",
		},
		Rank: 1,
	}
	res, err := rh.JSONSet("student", ".", student)
	if err != nil {
		log.Fatalf("Failed to JSONSet")
		return
	}

	if res.(string) == "OK" {
		fmt.Printf("Success: %s\n", res)
	} else {
		fmt.Println("Failed to Set: ")
	}

	studentJSON, err := redis.Bytes(rh.JSONGet("student", "."))
	if err != nil {
		log.Fatalf("Failed to JSONGet")
		return
	}

	readStudent := Student{}
	err = json.Unmarshal(studentJSON, &readStudent)
	if err != nil {
		log.Fatalf("Failed to JSON Unmarshal")
		return
	}

	fmt.Printf("Student read from redis : %#v\n", readStudent)
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
	ExampleJSONSet(rh)

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
	ExampleJSONSet(rh)

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
	ExampleJSONSet(rh)
}
