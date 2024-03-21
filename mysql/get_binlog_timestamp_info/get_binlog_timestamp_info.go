package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-log/log"
	"golang.org/x/net/context"
	"os"
	"strconv"
	"text/tabwriter"
	"time"
	"golang.org/x/crypto/ssh/terminal"
)

type BinlogInfo struct {
	LogName   string
	FileSize  string
	StartTime uint32
	EndTime   uint32
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
		return nil, fmt.Errorf("error executing query: %v", err)
	}
	defer rows.Close()

	// 存储二进制日志文件名的切片
	var binaryLogs [][]string

	// 遍历结果集并将日志文件名存储到切片中
	for rows.Next() {
		var logName, fileSize, encrypted string
		if err := rows.Scan(&logName, &fileSize, &encrypted); err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}
		binaryLogs = append(binaryLogs, []string{logName, fileSize})
	}

	// 检查是否遍历过程中有错误
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %v", err)
	}

	// 返回二进制日志文件名切片
	return binaryLogs, nil
}

func getFormatDescriptionEventTimestamp(cfg replication.BinlogSyncerConfig, binlogFilename string) (uint32, error) {
	// 创建 BinlogSyncer 实例
	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	// 设置起始位置，获取指定 binlog 文件的第一个事件
	streamer, err := syncer.StartSync(mysql.Position{Name: binlogFilename, Pos: 4})
	if err != nil {
		return 0, fmt.Errorf("error starting binlog syncer: %v", err)
	}

	// 读取 binlog 的第一个事件
	ctx := context.Background()
	for {
		// 读取事件
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			return 0, fmt.Errorf("error getting binlog event: %v", err)
		}

		// 如果是 FORMAT_DESCRIPTION_EVENT，则返回时间戳并跳出循环
		if ev.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			return ev.Header.Timestamp, nil
		}
	}
}

func main() {
	// Parse command line arguments
	host := flag.String("h", "localhost", "MySQL host")
	port := flag.Int("P", 3306, "MySQL port")
	user := flag.String("u", "", "MySQL user")
	password := flag.String("p", "", "MySQL password")
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

	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     *host,
		Port:     uint16(*port),
		User:     *user,
		Password: *password,
	}
	cfg.Logger = log.NewDefault(&log.NullHandler{})

	var binlogs []BinlogInfo
	// 输出存储的日志文件名列表
	var logEndTime uint32
	for i := len(binaryLogs) - 1; i >= 0; i-- {
		log := binaryLogs[i]
		logName, fileSize := log[0], log[1]
		startTime, err := getFormatDescriptionEventTimestamp(cfg, logName)
		binlogs = append(binlogs, BinlogInfo{logName, fileSize, startTime, logEndTime})
		logEndTime = startTime
		if err != nil {
			fmt.Println("Error:", err)
			os.Exit(1)
		}
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.TabIndent)
	fmt.Fprintf(w, "Log File\t\tFile Size\t\tStart Time\t\tEnd Time\t\tDuration\n")

	for i := len(binlogs) - 1; i >= 0; i-- {
		binlog := binlogs[i]
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
		fmt.Fprintf(w, "%s\t\t%d(%s)\t\t%s\t\t%s\t\t%s\n", binlog.LogName, fileSize, ConvertBytesToHumanReadable(fileSize), startFormattedTime, endFormattedTime, durationFormatted)
	}
	w.Flush()

}
