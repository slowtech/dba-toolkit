package redisUtil

import (
	"fmt"
	"log"
	"time"
	"path/filepath"
)

func BgSaveAndCheck(addr string) {
	client := createClient(addr)
	lastSaveTime ,err := client.LastSave().Result()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(lastSaveTime)
	_,err = client.BgSave().Result()
	if err != nil {
		log.Fatal(err)
	}
	for {
		saveTime,_ := client.LastSave().Result()
		if saveTime != lastSaveTime {
			break
		}
		time.Sleep(time.Second*1)
	}
}

func GetRDBPath(addr string) string {
	client := createClient(addr)
    result,_ := client.ConfigGet("dir").Result()
    dir := result[1].(string)
	result,_ = client.ConfigGet("dbfilename").Result()
	dbfilename := result[1].(string)
	return  filepath.Join(dir,dbfilename)
}
