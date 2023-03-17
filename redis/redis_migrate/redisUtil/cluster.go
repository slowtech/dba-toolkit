package redisUtil

import (
	"time"
	"log"
	"strings"
	"sort"
)

type cluster map[string]map[string]string

// 	cluster["192.168.244.20:6379"] = map[string]string{
//		"id":    90a2b0a0453847dcd29be0a6e4dc86a574383ee2,
//		"slave": 192.168.244.20:6382,
//	}
//  192.168.244.20:6379 is the master addr

func clusterMap(addr string) cluster {
	client := createClient(addr)
	defer client.Close()
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
	clustermap := make(cluster)
	for _, node := range nodes {
		if node["masterFlag"] == "slave" {
			masterId := node["masterId"]
			masterAddr := nodes[masterId]["addr"]
			clustermap[masterAddr] = map[string]string{
				"id":    masterId,
				"slave": node["addr"],
			}
		}
	}
	return clustermap
}

func (c cluster) resetClusterInfo() {
	nodes := c.getNodes()
	for _, each_node := range nodes {
		client := createClient(each_node)
		defer client.Close()
		_, err := client.ClusterResetSoft().Result()
		if err != nil {
			log.Fatalln(err)
		}
	}
}

func (c cluster) getNodes() []string {
	var nodes []string
	for k, v := range c {
		nodes = append(nodes, k, v["slave"])
	}
	return nodes
}

func (c cluster) createCluster() {
	nodes := c.getNodes()
	firstNode, otherNode := nodes[0], nodes[1:]
	client := createClient(firstNode)
	defer client.Close()
	for _, each_node := range otherNode {
		go func(node string) {
			ipPort := strings.Split(node, ":")
			ip, port := ipPort[0], ipPort[1]
			_, err := client.ClusterMeet(ip, port).Result()
			if err != nil {
				log.Fatalln(err)
			}
		}(each_node)
	}
	for _, each_node := range nodes {
		client := createClient(each_node)
		defer client.Close()
		for {
			result, err := client.ClusterNodes().Result()
			if err != nil {
				log.Fatal(err)
			}
			if strings.Count(result, "master") == len(nodes) {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
	for _, v := range c {
		go func(master map[string]string) {
			slaveIp := master["slave"]
			masterId := master["id"]
			client := createClient(slaveIp)
			defer client.Close()
			for {
				_, err := client.ClusterReplicate(masterId).Result()
				if err != nil {
					time.Sleep(time.Millisecond * 100)
					continue
				}
				break
			}
		}(v)
	}
	for _, each_node := range nodes {
		client := createClient(each_node)
		defer client.Close()
		for _, v := range c {
			for {
				result, err := client.ClusterSlaves(v["id"]).Result()
				if err == nil && len(result) == 1 {
					break
				}
				time.Sleep(time.Millisecond * 10)
			}
		}
	}
}

type slotMap map[string]map[string]int

func getSlotDistribute(addr string) slotMap {
	client := createClient(addr)
	defer client.Close()
	s := make(slotMap)
	clusterSlot, err := client.ClusterSlots().Result()
	if err != nil {
		log.Fatal("Can't get the ClusterSlot info")
	}
	for _, each_node := range clusterSlot {
		s[each_node.Nodes[0].Addr] = map[string]int{
			"start": each_node.Start,
			"end":   each_node.End,
		}
	}
	//sort.Slice(slotNode, func(i, j int) bool {
	//	return slotNode[i].addr < slotNode[j].addr
	//})
	return s
}

func addSlots(sourceAddr string, destAddr string) {
	links := linkMaster(sourceAddr, destAddr)
	var sourceMaster string
	for k, _ := range links {
		sourceMaster = k
		break
	}
	slotmap := getSlotDistribute(sourceMaster)
	for source, dest := range links {
		client := createClient(dest)
		defer client.Close()
		_, err := client.ClusterAddSlotsRange(slotmap[source]["start"], slotmap[source]["end"]).Result()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func resetCluster(addr string) {
	c := clusterMap(addr)
	c.resetClusterInfo()
	c.createCluster()
}

func getMaster(addr string) []string {
	c := clusterMap(addr)
	var master []string
	for k, _ := range c {
		master = append(master, k)
	}
	sort.Slice(master, func(i, j int) bool {
		return master[i] < master[j]
	})
	return master
}

func linkMaster(sourceAddr string, destAddr string) map[string]string {
	sourceMasters := getMaster(sourceAddr)
	destMasters := getMaster(destAddr)

	if len(sourceMasters) != len(destMasters) {
		log.Fatal("The number of nodes is not equal")
	}
	masterLink := make(map[string]string)
	for i := 0; i < len(sourceMasters); i++ {
		masterLink[sourceMasters[i]] = destMasters[i]
	}
	return masterLink
}

func CopySlotInfo(sourceAddr string, destAddr string) {
	resetCluster(destAddr)
	addSlots(sourceAddr, destAddr)
}
