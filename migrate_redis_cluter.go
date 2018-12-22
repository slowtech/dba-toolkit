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
		log.Fatalf("Can't establish connection successfully %s", err)
	}
	return client
}

type SlotNodeMap struct {
	addr  string
	start int
	end   int
}

func GetSlotDistribute(addr string) []SlotNodeMap {
	client := createClient(addr)
	var slotNode []SlotNodeMap
	clusterSlot, err := client.ClusterSlots().Result()
	if err != nil {
		log.Fatal("Can't get the ClusterSlot info")
	}
	for _, each_node := range clusterSlot {
		slotNode = append(slotNode, SlotNodeMap{each_node.Nodes[0].Addr, each_node.Start, each_node.End})
	}
	sort.Slice(slotNode, func(i, j int) bool {
		return slotNode[i].addr < slotNode[j].addr
	})
	return slotNode
}

func GetMasterSlaveMap(addr string) map[string]string {
	client := createClient(addr)
	result, err := client.ClusterNodes().Result()
	if err != nil {
		log.Fatal("Can't get the ClusterNode info")
	}
	nodes := make(map[string]map[string]string)
	for _, line := range strings.Split(result, "\n") {
		if len(line) == 0 {
			continue
		}
		nodeInfo := strings.Split(line, " ")
		id := nodeInfo[0]
		addr := nodeInfo[1]
		masterFlag := nodeInfo[2]
		masterId := nodeInfo[3]
		nodes[id] = map[string]string{
			"addr":       addr,
			"masterFlag": masterFlag,
			"masterId":   masterId,
		}
	}
	masterSlaveMap := make(map[string]string)
	for _, node := range nodes {
		if node["masterFlag"] == "slave" {
			masterId := node["masterId"]
			masterAddr := nodes[masterId]["addr"]
			masterSlaveMap[masterAddr] = node["addr"]
		}
	}
	for master, slave := range masterSlaveMap {
		fmt.Println(master, slave)
	}
	return masterSlaveMap
}

func ClusterReset(masterSlaveMap map[string]string) {
	nodes := getNodes(masterSlaveMap)
    for _,each_node := range  nodes {
		client := createClient(each_node)
		_, err := client.ClusterResetSoft().Result()
		if err != nil {
			log.Println(err)
		}
	}
}

func getNodes(masterSlaveMap map[string]string) []string {
	var nodes []string
	for master, slave := range masterSlaveMap {
		nodes = append(nodes, master, slave)
	}
	return nodes
}

func CreateCluser(masterSlaveMap map[string]string) {
	nodes := getNodes(masterSlaveMap)
	fmt.Println(nodes)
}
func main() {
	destAddr := "192.168.244.20:6379"
	masterSlaveMap := GetMasterSlaveMap(destAddr)
	CreateCluser(masterSlaveMap)
	//ClusterReset(masterSlaveMap)
}

