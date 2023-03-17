package redisUtil

import (
	"github.com/go-redis/redis"
	"log"
)

func createClient(addr string) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})
	_, err := client.Ping().Result()
	if err != nil {
		log.Fatalf("Can't establish connection successfully %s", err)
	}
	return client
}
