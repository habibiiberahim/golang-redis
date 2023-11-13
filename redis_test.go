package golang_redis

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

var client = redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
	DB:   0,
})

func TestConnection(t *testing.T) {
	assert.NotNil(t, client)
}

var ctx = context.Background()

func TestPing(t *testing.T) {
	result, err := client.Ping(ctx).Result()
	assert.Nil(t, err)
	assert.Equal(t, "PONG", result)
}

func TestString(t *testing.T) {
	client.SetEx(ctx, "name", "Habibi Iberahim", 1*time.Second)

	result, err := client.Get(ctx, "name").Result()
	assert.Nil(t, err)
	assert.Equal(t, "Habibi Iberahim", result)

	time.Sleep(1 * time.Second)
	result, err = client.Get(ctx, "name").Result()
	assert.NotNil(t, err)
}

func TestList(t *testing.T) {
	client.RPush(ctx, "names", "Habibi")
	client.RPush(ctx, "names", "Iberahim")

	assert.Equal(t, "Habibi", client.LPop(ctx, "names").Val())
	assert.Equal(t, "Iberahim", client.LPop(ctx, "names").Val())

	client.Del(ctx, "names")
}

func TestSet(t *testing.T) {
	client.SAdd(ctx, "employee", "Habibi")
	client.SAdd(ctx, "employee", "Habibi")
	client.SAdd(ctx, "employee", "Iberahim")
	client.SAdd(ctx, "employee", "Iberahim")

	assert.Equal(t, int64(2), client.SCard(ctx, "employee").Val())
	assert.Equal(t, []string{"Habibi", "Iberahim"}, client.SMembers(ctx, "employee").Val())
}

func TestSortedSet(t *testing.T) {
	client.ZAdd(ctx, "scores", redis.Z{
		Score:  90,
		Member: "Habibi",
	})

	client.ZAdd(ctx, "scores", redis.Z{
		Score:  100,
		Member: "Iberahim",
	})

	client.ZAdd(ctx, "scores", redis.Z{
		Score:  80,
		Member: "Golang",
	})

	assert.Equal(t, []string{"Golang", "Habibi", "Iberahim"}, client.ZRange(ctx, "scores", 0, 2).Val())
	assert.Equal(t, "Iberahim", client.ZPopMax(ctx, "scores").Val()[0].Member)
	assert.Equal(t, "Habibi", client.ZPopMax(ctx, "scores").Val()[0].Member)
	assert.Equal(t, "Golang", client.ZPopMax(ctx, "scores").Val()[0].Member)
}

func TestHash(t *testing.T) {
	client.HSet(ctx, "hashKey", "id", "1")
	client.HSet(ctx, "hashKey", "name", "Habibi Iberahim")
	client.HSet(ctx, "hashKey", "email", "example@example.com")

	user := client.HGetAll(ctx, "hashKey").Val()
	assert.Equal(t, "1", user["id"])
	assert.Equal(t, "Habibi Iberahim", user["name"])
	assert.Equal(t, "example@example.com", user["email"])
}

func TestGeoPoint(t *testing.T) {
	client.GeoAdd(ctx, "city",
		&redis.GeoLocation{
			Name:      "Banjarmasin",
			Longitude: 114.590111,
			Latitude:  -3.316694,
		})

	client.GeoAdd(ctx, "city",
		&redis.GeoLocation{
			Name:      "Banjarbaru",
			Longitude: 114.810318,
			Latitude:  -3.457242,
		})

	distance := client.GeoDist(ctx, "city", "Banjarmasin", "Banjarbaru", "km").Val()
	assert.Equal(t, float64(29.0206), distance)

	cities := client.GeoSearch(ctx, "city", &redis.GeoSearchQuery{
		Longitude:  114.76082010000003,
		Latitude:   -3.4359874165775253,
		Radius:     50,
		RadiusUnit: "km",
	}).Val()

	assert.Equal(t, []string{"Banjarbaru", "Banjarmasin"}, cities)
}

func TestHyperLogLog(t *testing.T) {
	client.PFAdd(ctx, "visitors", "Habibi", "Iberahim")
	client.PFAdd(ctx, "visitors", "Habibi", "Iberahim")
	client.PFAdd(ctx, "visitors", "Habibi", "Iberahim")

	count := client.PFCount(ctx, "visitors").Val()
	assert.Equal(t, int64(2), count)
}

func TestPipeline(t *testing.T) {
	client.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SetEx(ctx, "name", "habibi", 5*time.Second)
		pipeliner.SetEx(ctx, "address", "banjarmasin", 5*time.Second)
		return nil
	})

	assert.Equal(t, "habibi", client.Get(ctx, "name").Val())
	assert.Equal(t, "banjarmasin", client.Get(ctx, "address").Val())
}

func TestTransaction(t *testing.T) {
	client.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SetEx(ctx, "name", "habibi", 5*time.Second)
		pipeliner.SetEx(ctx, "address", "banjarmasin", 5*time.Second)
		return nil
	})

	assert.Equal(t, "habibi", client.Get(ctx, "name").Val())
	assert.Equal(t, "banjarmasin", client.Get(ctx, "address").Val())
}

func TestPublishStream(t *testing.T) {
	for i := 0; i < 10; i++ {
		err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: "members",
			Values: map[string]interface{}{
				"name":    "Habibi",
				"address": "Banjarmasin",
			},
		}).Err()
		assert.Nil(t, err)
	}
}

func TestCreateConsumeGroup(t *testing.T) {
	client.XGroupCreate(ctx, "members", "group-1", "0")
	client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-1")
	client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-2")
}

func TestGetStream(t *testing.T) {
	result := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "group-1",
		Consumer: "consumer-1",
		Streams:  []string{"members", ">"},
		Count:    10,
		Block:    time.Second * 5,
	}).Val()

	for _, stream := range result {
		for _, message := range stream.Messages {
			fmt.Println(message.ID)
			fmt.Println(message.Values)
		}
	}
}

func TestSubscribePubSub(t *testing.T) {
	subscriber := client.Subscribe(ctx, "chanel-1")

	defer subscriber.Close()

	for i := 0; i < 10; i++ {
		message, _ := subscriber.ReceiveMessage(ctx)
		fmt.Println(message.Payload)
	}
}

func TestPublishPubSub(t *testing.T) {
	for i := 0; i < 10; i++ {
		err := client.Publish(ctx, "channel-1", "Hello "+strconv.Itoa(i)).Err()
		assert.Nil(t, err)
	}
}
