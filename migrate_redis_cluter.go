package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"sort"
	"strings"
)
func createClient(addr string) *redis.Client {
	fmt.Println(addr)
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})
	_, err := client.Ping().Result()
	if err != nil {
		log.Fatal("Can't establish connection successfully")
	}
	return client
}

type SlotNodeMap struct {
	addr string
	start int
	end int
}

func GetSlotDistribute(addr string) []SlotNodeMap {
	client := createClient(addr)
	var slotNode []SlotNodeMap
	clusterSlot,err := client.ClusterSlots().Result()
	if err !=nil {
		log.Fatal("Can't get the ClusterSlot info")
	}
	for _,each_node := range clusterSlot {
		slotNode= append(slotNode, SlotNodeMap{each_node.Nodes[0].Addr,each_node.Start,each_node.End} )
	}
	sort.Slice(slotNode, func(i, j int) bool {
		return slotNode[i].addr < slotNode[j].addr
	})
	return slotNode
}

func GetMasterSlaveInfo(addr string) {
	client := createClient(addr)
	result,err := client.ClusterNodes().Result()
	if err != nil {
		log.Fatal("Can't get the ClusterNode info")
	}

	nodes := make(map[string]map[string]string)
	for _,line := range strings.Split(result,"\n"){
		if len(line) == 0 {
			continue
		}
		nodeInfo :=strings.Split(line," ")
		id := nodeInfo[0]
		addr := nodeInfo[1]
		masterFlag := nodeInfo[2]
		masterId := nodeInfo[3]
		nodes[id] = map[string]string {
			"addr":addr,
			"masterFlag":masterFlag,
			"masterId":masterId,
		}
	}
    masterSlaveMap := make([]map[string]string,0)
	for _,node := range nodes {
		if node["masterFlag"] == "slave" {
			masterId := node["masterId"]
			masterAddr := nodes[masterId]["addr"]
			m := map[string]string{
				"master": masterAddr,
				"slave": node["addr"],
			}
			masterSlaveMap= append(masterSlaveMap, m)
		}
	}
	for _,masterslave := range masterSlaveMap {
		fmt.Println(masterslave)
	}
}

func main(){
     destAddr :="192.168.244.20:6379"
	 GetMasterSlaveInfo(destAddr)
}
