package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"github.com/olekukonko/tablewriter"
	"github.com/siddontang/go-log/log"
	"golang.org/x/crypto/ssh/terminal"
	"golang.org/x/net/context"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type BinlogInfo struct {
	LogName              string
	FileSize             string
	StartTime            uint32
	EndTime              uint32
	PreviousGTIDs        string
	NextLogPreviousGTIDs string
}

type ConcurrentResult struct {
	StartTime     uint32
	PreviousGTIDs string
	Index         int
	Err           error
}

func GetGTIDSubtract(gtid1, gtid2 string) (string, error) {
	// 解析 GTID
	parsedGTID1, err := mysql.ParseGTIDSet("mysql", gtid1)
	if err != nil {
		return "", fmt.Errorf("error parsing GTID1: %v", err)
	}
	m1 := *parsedGTID1.(*mysql.MysqlGTIDSet)
	parsedGTID2, err := mysql.ParseGTIDSet("mysql", gtid2)
	if err != nil {
		return "", fmt.Errorf("error parsing GTID2: %v", err)
	}

	m2 := *parsedGTID2.(*mysql.MysqlGTIDSet)
	// 计算差值
	err = m1.Minus(m2)
	if err != nil {
		return "", fmt.Errorf("error calculating GTID difference: %v", err)
	}

	return m1.String(), nil
}

func ExtractGTIDSuffix(gtidStr string) string {
	if !strings.Contains(gtidStr, ",") && strings.Contains(gtidStr, ":") {
		parts := strings.Split(gtidStr, ":")
		if len(parts) == 2 {
			return parts[1]
		}
	}
	return gtidStr
}

func ConvertUnixTimestampToFormattedTime(unixTimestamp int64) (string, error) {
	// 转换为时间格式
	t := time.Unix(unixTimestamp, 0)

	// 格式化为默认的日期时间格式
	formattedTime := t.Format("2006-01-02 15:04:05")

	return formattedTime, nil
}

// ConvertBytesToHumanReadable 将 uint64 类型的字节大小转换为人类可读的单位
func ConvertBytesToHumanReadable(bytes uint64) string {
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
	)

	unit := "bytes"
	divisor := uint64(1)

	switch {
	case bytes >= gib:
		divisor = gib
		unit = "GB"
	case bytes >= mib:
		divisor = mib
		unit = "MB"
	case bytes >= kib:
		divisor = kib
		unit = "KB"
	}

	value := float64(bytes) / float64(divisor)
	format := "%.2f %s"
	result := fmt.Sprintf(format, value, unit)
	return result
}

func getBinaryLogs(dsn string) ([][]string, error) {
	// 连接 MySQL 数据库
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("error connecting to MySQL: %v", err)
	}
	defer db.Close()

	// 执行 SQL 查询
	rows, err := db.Query("SHOW BINARY LOGS;")
	if err != nil {
		return nil, fmt.Errorf("error executing SHOW BINARY LOGS: %v", err)
	}
	defer rows.Close()

	// 存储二进制日志文件名的切片
	var binaryLogs [][]string

	// 遍历结果集并将日志文件名存储到切片中
	for rows.Next() {
		columns, err := rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("error fetching columns: %v", err)
		}

		// 创建与列数相同的切片，用来接收扫描后的数据
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(string) // 使用 *string 来接收每列的值
		}

		// 执行扫描
		if err := rows.Scan(values...); err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}

		// 提取前两列的数据并加入结果
		logName := *(values[0].(*string))
		fileSize := *(values[1].(*string))

		binaryLogs = append(binaryLogs, []string{logName, fileSize})
	}

	// 检查是否遍历过程中有错误
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %v", err)
	}

	// 返回二进制日志文件名切片
	return binaryLogs, nil
}

func getFormatAndPreviousGTIDs(host string, port int, user string, password string, binlogFilename string, index int, ch chan<- ConcurrentResult, wg *sync.WaitGroup) (uint32, string, error) {
	// 创建 BinlogSyncer 实例
	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(index + 33061),
		Flavor:   "mysql",
		Host:     host,
		Port:     uint16(port),
		User:     user,
		Password: password,
	}

	cfg.Logger = log.NewDefault(&log.NullHandler{})

	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	streamer, err := syncer.StartSync(mysql.Position{Name: binlogFilename, Pos: 4})
	if err != nil {
		return 0, "", fmt.Errorf("error starting binlog syncer: %v", err)
	}

	var formatTimestamp uint32
	var previousGTIDs string

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		// 读取事件
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			return 0, "", fmt.Errorf("error getting binlog event: %v", err)
		}

		// 如果是 FORMAT_DESCRIPTION_EVENT，则记录时间戳
		if ev.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			formatTimestamp = ev.Header.Timestamp
		}

		// 如果是 PREVIOUS_GTIDS_EVENT，则记录其内容并跳出循环
		if ev.Header.EventType == replication.PREVIOUS_GTIDS_EVENT {
			previousGTIDsEvent := ev.Event.(*replication.PreviousGTIDsEvent)
			previousGTIDs = previousGTIDsEvent.GTIDSets
			break
		}
	}

	return formatTimestamp, previousGTIDs, nil
}

func main() {
	// Parse command line arguments
	host := flag.String("h", "localhost", "MySQL host")
	port := flag.Int("P", 3306, "MySQL port")
	user := flag.String("u", "root", "MySQL user")
	password := flag.String("p", "", "MySQL password")
	var verbose bool
	flag.BoolVar(&verbose, "v", false, "Enable verbose logging")
	numParallel := flag.Int("n", 5, "Number of goroutines to run concurrently")
	flag.Parse()
	if *password == "" {
		fmt.Print("Enter MySQL password: ")
		bytePassword, err := terminal.ReadPassword(int(os.Stdin.Fd()))
		fmt.Println()
		if err != nil {
			log.Fatalf("Error: Failed to read the password - %v", err)
		}
		*password = string(bytePassword)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/mysql", *user, *password, *host, *port)

	// 调用获取二进制日志文件名的函数
	binaryLogs, err := getBinaryLogs(dsn)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	if verbose {
		timestamp := time.Now().Format("2006/01/02 15:04:05")
		fmt.Printf("[%s] [info] get_binlog_timestamp_info.go SHOW BINARY LOGS done, %d binlogs to analyze\n", timestamp, len(binaryLogs))

	}

	// Create wait group and result channel
	var wg sync.WaitGroup
	ch := make(chan ConcurrentResult, len(binaryLogs))

	// Limit parallelism
	sem := make(chan struct{}, *numParallel)

	// Iterate over binary logs and fetch format timestamp and previous GTIDs concurrently
	for i := len(binaryLogs) - 1; i >= 0; i-- {
		sem <- struct{}{}
		wg.Add(1)
		go func(index int) {
			defer func() {
				<-sem
				wg.Done()
			}()
			logName := binaryLogs[index][0]
			startTime, previousGTIDs, err := getFormatAndPreviousGTIDs(*host, *port, *user, *password, logName, index, ch, &wg)
			ch <- ConcurrentResult{StartTime: startTime, PreviousGTIDs: previousGTIDs, Index: index, Err: err}
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	close(ch)

	// Collect results from channel
	results := make([]ConcurrentResult, len(binaryLogs))
	for r := range ch {
		results[r.Index] = r
	}
	originalBinlogs := make([]BinlogInfo, len(binaryLogs))
	for _, result := range results {
		logName := binaryLogs[result.Index][0]
		fileSize := binaryLogs[result.Index][1]
		binlog := BinlogInfo{
			LogName:       logName,
			FileSize:      fileSize,
			StartTime:     result.StartTime,
			PreviousGTIDs: result.PreviousGTIDs,
		}
		// fmt.Println(result.Index, logName,fileSize, result.StartTime, result.PreviousGTIDs)
		originalBinlogs[result.Index] = binlog
	}

	var logEndTime uint32
	var nextLogPreviousGTIDs string
	var processedBinlogs []BinlogInfo
	for i := len(binaryLogs) - 1; i >= 0; i-- {
		log := originalBinlogs[i]
		logName, fileSize, startTime, previousGTIDs := log.LogName, log.FileSize, log.StartTime, log.PreviousGTIDs
		if verbose {
			timestamp := time.Now().Format("2006/01/02 15:04:05")
			fmt.Printf("[%s] [info] get_binlog_timestamp_info.go %s done, still %d binlogs to analyze\n", timestamp, logName, i)
		}
		processedBinlogs = append(processedBinlogs, BinlogInfo{logName, fileSize, startTime, logEndTime, previousGTIDs, nextLogPreviousGTIDs})
		logEndTime = startTime
		nextLogPreviousGTIDs = previousGTIDs

		if err != nil {
			fmt.Println("Error:", err)
			os.Exit(1)
		}
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoFormatHeaders(false)
	table.SetHeader([]string{"Log_name", "File_size", "Start_time", "End_time", "Duration", "GTID"})

	for i := len(processedBinlogs) - 1; i >= 0; i-- {
		binlog := processedBinlogs[i]
		fileSize, err := strconv.ParseUint(binlog.FileSize, 10, 64)
		if err != nil {
			fmt.Println("Error parsing string to uint64:", err)
			return
		}
		startUnixTimestamp := int64(binlog.StartTime)
		startTime := time.Unix(startUnixTimestamp, 0)
		startFormattedTime, err := ConvertUnixTimestampToFormattedTime(startUnixTimestamp)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		endUnixTimestamp := int64(binlog.EndTime)
		endTime := time.Unix(endUnixTimestamp, 0)
		endFormattedTime, err := ConvertUnixTimestampToFormattedTime(endUnixTimestamp)

		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		duration := endTime.Sub(startTime)
		durationFormatted := fmt.Sprintf("%02d:%02d:%02d", int(duration.Hours()), int(duration.Minutes())%60, int(duration.Seconds())%60)

		if endUnixTimestamp == 0 {
			endFormattedTime, durationFormatted = "", ""
		}
		gtidDifference, err := GetGTIDSubtract(binlog.NextLogPreviousGTIDs, binlog.PreviousGTIDs)
		if err != nil {
			fmt.Println("Error:", err)
			return

		}

		table.Append([]string{binlog.LogName, fmt.Sprintf("%d (%s)", fileSize, ConvertBytesToHumanReadable(fileSize)), startFormattedTime, endFormattedTime, durationFormatted, ExtractGTIDSuffix(gtidDifference)})
	}
	table.Render()

}
