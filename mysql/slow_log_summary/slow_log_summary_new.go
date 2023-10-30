package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"golang.org/x/crypto/ssh/terminal"
	"html/template"
	"log"
	"math"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const temp = `
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Slow Log</title>
    <style>
        body {
            font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
            font-size: 14px;
            line-height: 1.5;
            color: #333;
            background-color: #f5f5f5;
        }
        h2 {
            font-weight: bold;
            font-size: 28px;
            margin: 20px auto;
            text-align: center;
        }
        table {
            font-size: 14px;
            width: 120%;
            max-width: 150%;
            margin-bottom: 20px;
            border-collapse: collapse;
            border-spacing: 0;
            background-color: transparent;
        }
        th,
        td {
            padding: 8px;
            vertical-align: top;
            border-top: 1px solid #ddd;
        }
        th {
            font-weight: bold;
            text-align: left;
            background-color: #f9f9f9;
            border-bottom: 2px solid #ddd;
            color: #0074a3;
            background-color:#e5eefd;
            white-space: nowrap;
        }
        td:hover {
            background-color: #ddd;
         }
        .table-bordered {
            border: 1px solid #ddd;
            border-collapse: separate;
            border-left: 0;
            border-radius: 4px;
            overflow: hidden;
        }
        .table-bordered th,
        .table-bordered td {
            border-left: 1px solid #ddd;
        }
        .table-bordered > thead > tr > th,
        .table-bordered > tbody > tr > th,
        .table-bordered > tfoot > tr > th,
        .table-bordered > thead > tr > td,
        .table-bordered > tbody > tr > td,
        .table-bordered > tfoot > tr > td {
            border: 1px solid #ddd;
        }
        .table-hover > tbody > tr:hover {
            background-color: #f5f5f5;
        }
        .table-striped > tbody > tr:nth-of-type(odd) {
            background-color: #f9f9f9;
        }
        .table-hover .table-striped > tbody > tr:hover {
            background-color: #e8e8e8;
        }
        .text-center {
            text-align: center;
        }
        .text-right {
            text-align: right;
        }
        .text-left {
            text-align: left;
        }
        .bold {
            font-weight: bold;
        }
        .float-right {
            float: right;
        }
        .generated-time {
            font-size: 12px;
            font-weight: bold;
            float: right;
            margin-top: 5px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="row">
            <div class="col-md-12">
                <h2>Slow Log Summary</h2>
                {{if eq .slow_log_source "performance_schema"}}
                    <span class="generated-time">慢日志来源：performance_schema 实例地址：{{.ip_port}} 生成时间：{{.now}}</span>
                {{else}}
                    <span class="generated-time">慢日志来源：{{.slow_log_file}} 生成时间：{{.now}}</span>
                {{end}}
                <div class="table-responsive">
                    <table class="table table-bordered table-hover table-striped">
                        <thead>
                            <tr>
                            {{if eq .slow_log_source "performance_schema"}}
                                <th class="text-center">排名</th>
                                <th class="text-center">总耗时</th>
                                <th class="text-center">总执行次数</th>
                                <th class="text-center">平均耗时</th>
                                <th class="text-left">平均扫描行数</th>
                                <th class="text-left">平均发送行数</th>
                                {{with index .slowlogs 0}}
                                   {{if .CpuTimeAvg}}
                                       <th class="text-left">CPU平均耗时</th>
                                   {{end}}
                                   {{if .MaxTotalMemory}}
                                       <th class="text-center">最大内存使用量</th>
                                   {{end}}
                                {{end}}
                                <th class="text-left">第一次出现时间</th>
                                <th class="text-left">最近一次出现时间</th>
                                <th class="text-center">数据库名</th>
                                <th style="width:50%" class="text-center">SQL语句</th>
                            {{else}}
                                <th style="width:5%">Rank</th>
                                <th style="width:5%">Response time</th>
                                <th style="width:5%">Response ratio</th>
                                <th style="width:5%">Calls</th>
                                <th style="width:5%">R/Call</th>
                                <th style="width:15%">QueryId</th>
                                <th style="width:60%">Example</th>
                            {{end}}
                            </tr>
                        </thead>
                        <tbody>
                            {{if eq .slow_log_source "performance_schema"}}
                                {{range .slowlogs}}
                                <tr>
                                    <td class="text-center">{{ .RowNum}}</td>
                                    <td class="text-center">{{ .TotalLatency}}</td>
                                    <td class="text-center">{{ .ExecutionCount}}</td>
                                    <td class="text-center">{{ .AvgLatency}}</td>
                                    <td class="text-center">{{ .RowsExaminedAvg}}</td>
                                    <td class="text-center">{{ .RowsSentAvg}}</td>
                                    {{if .CpuTimeAvg}}
                                        <td class="text-center">{{ .CpuTimeAvg }}</td>
                                    {{end}}
                                    {{if .MaxTotalMemory}}
                                        <td class="text-center">{{ .MaxTotalMemory }}</td>
                                    {{end}}
                                    <td class="text-left">{{ .FirstSeen}}</td>
                                    <td class="text-left">{{ .LastSeen}}</td>
                                    <td class="text-center">{{ .Database}}</td>
                                    <td style="width:40%" class="text-left">{{ .SampleQuery}}</td>
                                </tr>
                               {{end}}
                            {{else}}
                                {{range .slowlogs}}
                                <tr>
                                    <td style="width:5%">{{ .Rank}}</td>        
                                    <td style="width:5%">{{ .Response_time}}</td>
                                    <td style="width:5%">{{ .Response_ratio}}</td>
                                    <td style="width:5%">{{ .Calls}}</td>        
                                    <td style="width:5%">{{ .R_Call}}</td>
                                    <td style="width:15%">{{ .QueryId}}</td>
                                    <td style="width:60%">{{ .Example}}</td>
                                </tr>
                                {{end}}
                            {{end}}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>
</body>
</html>
`

var currentTime time.Time

type Config struct {
	Help       bool
	Source     string
	Host       string
	Username   string
	Password   string
	Database   string
	Port       int
	PtCmd      string
	Slowlog    string
	All        bool
	Since      string
	Until      string
	Yday       bool
	ResultFile string
}

func customUsage() {
	fmt.Fprintf(os.Stdout, `slow_log_summary version: 1.0.0
Usage:
slow_log_summary -source <source_type> -r <output_file> [other options]

Common Options:
  -help
    Display usage

Source Type Options:
  -source string
    Slow log source: 'perf' or 'slowlog' (default "perf")

Output File Options:
  -r string
    Direct output to a given file (default "/tmp/slow_log_summary_2023_10_26_22_02_52.html")

Options when source is 'perf':
  -h string
    MySQL host (default "localhost")
  -P int
    MySQL port (default 3306)
  -u string
    MySQL username (default "root")
  -p string
    MySQL password
  -D string
    MySQL database (default "performance_schema")

Options when source is 'slowlog':
  -pt string
    Absolute path for pt-query-digest. Example: /usr/local/percona-toolkit/bin/pt-query-digest
  -slowlog string
    Absolute path for slowlog. Example: /var/log/mysql/node1-slow.log
  -since string
    Parse only queries newer than this value, YYYY-MM-DD [HH:MM:SS]
  -until string
    Parse only queries older than this value, YYYY-MM-DD [HH:MM:SS]
  -all
    Parse the whole slowlog
  -yday
    Parse yesterday's slowlog (default true)
`)
}

func (c *Config) ParseFlags() {
	resultFileName := fmt.Sprintf("/tmp/slow_log_summary_%s.html", currentTime.Format("2006_01_02_15_04_05"))
	f := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	f.BoolVar(&c.Help, "help", false, "Display usage")
	f.StringVar(&c.Source, "source", "", "Slow log source")
	f.StringVar(&c.Host, "h", "localhost", "MySQL host")
	f.IntVar(&c.Port, "P", 3306, "MySQL port")
	f.StringVar(&c.Username, "u", "root", "MySQL username")
	f.StringVar(&c.Password, "p", "", "MySQL password")
	f.StringVar(&c.Database, "D", "performance_schema", "MySQL database")
	f.StringVar(&c.PtCmd, "pt", "", "Absolute path for pt-query-digest. Example:/usr/local/percona-toolkit/bin/pt-query-digest")
	f.StringVar(&c.Slowlog, "slowlog", "", "Absolute path for slowlog. Example:/var/log/mysql/node1-slow.log")
	f.StringVar(&c.Since, "since", "", "Parse only queries newer than this value,YYYY-MM-DD [HH:MM:SS]")
	f.StringVar(&c.Until, "until", "", "Parse only queries older than this value,YYYY-MM-DD [HH:MM:SS]")
	f.BoolVar(&c.All, "all", false, "Parse the whole slowlog")
	f.BoolVar(&c.Yday, "yday", true, "Parse yesterday's slowlog")
	f.StringVar(&c.ResultFile, "r", resultFileName, "Direct output to a given file")
	f.Parse(os.Args[1:])
	if c.Help {
		customUsage()
		os.Exit(0)
	}
}

func executeCommand(command string, args []string) (string, error) {
	cmd := exec.Command(command, args...)

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		return "", fmt.Errorf("command %s %v failed with %s", command, args, stderr.String())
	}

	return stdout.String(), nil
}

func getSlowLogSummaryByPtQueryDigest(ptQueryDigestCmd []string, now string) map[string]interface{} {
	slowLog, err := executeCommand("perl", ptQueryDigestCmd)
	if err != nil {
		log.Fatalf("Error: Failed to execute the Perl command for pt-query-digest: %v", err)
	}
	lines := strings.Split(string(slowLog), "\n")
	linesNums := len(lines)
	profileFlag := false
	exampleFlag := false
	exampleSQL := []string{}
	slowLogProfile := [][]string{}
	exampleSQLs := make(map[string]string)
	var queryID string
	for k, line := range lines {
		if strings.Contains(line, "# Profile") {
			profileFlag = true
			continue
		} else if profileFlag && (len(line) == 0 || strings.HasPrefix(line, "# MISC 0xMISC")) {
			profileFlag = false
			continue
		}
		if profileFlag {
			if strings.HasPrefix(line, "# Rank") || strings.HasPrefix(line, "# ====") {
				continue
			}
			re, _ := regexp.Compile(" +")
			rowToArray := re.Split(line, 9)
			slowLogProfile = append(slowLogProfile, rowToArray)
		} else if strings.Contains(line, "concurrency, ID 0x") {
			re := regexp.MustCompile(`(?U)ID (0x.*) `)
			queryID = re.FindStringSubmatch(line)[1]
			exampleFlag = true
			exampleSQL = []string{}
		} else if exampleFlag && (!strings.HasPrefix(line, "#")) && len(line) != 0 {
			exampleSQL = append(exampleSQL, line)
		} else if exampleFlag && (len(line) == 0 || k == (linesNums-1)) {
			exampleFlag = false
			exampleSQLs[queryID] = strings.Join(exampleSQL, "\n")
		}
	}

	for _, v := range slowLogProfile {
		for key := range exampleSQLs {
			miniQueryID := strings.Trim(v[2], ".")
			if strings.Contains(key, miniQueryID) {
				v[8] = exampleSQLs[key]
				v[2] = key
				break
			}
		}
	}

	type slowlog struct {
		Rank           string
		Response_time  string
		Response_ratio string
		Calls          string
		R_Call         string
		QueryId        string
		Example        string
	}

	slowlogs := []slowlog{}
	for _, value := range slowLogProfile {
		slowlogrecord := slowlog{value[1], value[3], value[4], value[5], value[6], value[2], value[8]}
		slowlogs = append(slowlogs, slowlogrecord)
	}
	//var report = template.Must(template.New("slowlog").Parse(temp))
	//report.Execute(os.Stdout, map[string]interface{}{"slowlogs": slowlogs, "now": now})
	fmt.Println(slowlogs)
	return map[string]interface{}{"slowlogs": slowlogs, "now": now}
}

func formatPicoTime(val string) string {
	timeVal, err := strconv.ParseFloat(val, 64)
	if err != nil {
		log.Fatalf("Error: Conversion failed: %v\n", err)
	}

	if math.IsNaN(timeVal) {
		return "null"
	}

	const (
		nano  = 1000
		micro = 1000 * nano
		milli = 1000 * micro
		sec   = 1000 * milli
		min   = 60 * sec
		hour  = 60 * min
		day   = 24 * hour
	)

	var divisor uint64
	var unit string

	timeAbs := math.Abs(timeVal)

	if timeAbs >= float64(day) {
		divisor = day
		unit = "d"
	} else if timeAbs >= float64(hour) {
		divisor = hour
		unit = "h"
	} else if timeAbs >= float64(min) {
		divisor = min
		unit = "min"
	} else if timeAbs >= float64(sec) {
		divisor = sec
		unit = "s"
	} else if timeAbs >= float64(milli) {
		divisor = milli
		unit = "ms"
	} else if timeAbs >= float64(micro) {
		divisor = micro
		unit = "us"
	} else if timeAbs >= float64(nano) {
		divisor = nano
		unit = "ns"
	} else {
		divisor = 1
		unit = "ps"
	}

	var result string

	if divisor == 1 {
		result = fmt.Sprintf("%.3f %s", timeVal, unit)
	} else {
		value := timeVal / float64(divisor)
		if math.Abs(value) >= 100000.0 {
			result = fmt.Sprintf("%.2e %s", value, unit)
		} else {
			result = fmt.Sprintf("%.2f %s", value, unit)
		}
	}

	return result
}

func IsMySQLVersionGreaterOrEqual(version1, version2 string) bool {
	parts1 := strings.Split(version1, ".")
	parts2 := strings.Split(version2, ".")

	for i := 0; i < len(parts1) && i < len(parts2); i++ {
		v1, _ := strconv.Atoi(parts1[i])
		v2, _ := strconv.Atoi(parts2[i])

		if v1 > v2 {
			return true
		} else if v1 < v2 {
			return false
		}
	}

	return true
}

func getSlowLogSummaryFromPerformanceSchema(username string, password string, host string, database string, port int, now string) map[string]interface{} {
	// 创建数据库连接
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, host, port, database)
	db, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		log.Fatalf("Error: Failed to establish a connection to the database: %v", err)
	}
	defer db.Close()
	var statementAnalysisSQL string

	var mysqlVersion string
	err = db.Get(&mysqlVersion, "SELECT VERSION()")

	if err != nil {
		log.Fatalf("Error: Failed to retrieve MySQL version: %v", err)
	}

	// 检查 MySQL 版本
	var queryColumn string
	if strings.HasPrefix(mysqlVersion, "8") {
		queryColumn = "QUERY_SAMPLE_TEXT AS query"
		if IsMySQLVersionGreaterOrEqual(mysqlVersion, "8.0.28") {
			// 查询 performance_schema.setup_consumers 中 'events_statements_cpu' 记录的 enabled 列
			var eventsStatementsCPUEnabled string
			err = db.Get(&eventsStatementsCPUEnabled, "SELECT enabled FROM performance_schema.setup_consumers WHERE name='events_statements_cpu'")
			if err != nil {
				log.Fatalf("Error: Failed to retrieve events_statements_cpu enabled status: %v", err)
			}
			if ventsStatementsCPUEnabled == "YES" {
				queryColumn += ", ROUND(IFNULL(SUM_CPU_TIME / NULLIF(COUNT_STAR, 0), 0), 0) AS cup_time_avg"
			}
		}
		if IsMySQLVersionGreaterOrEqual(mysqlVersion, "8.0.31") {
			queryColumn += ", sys.format_bytes(MAX_TOTAL_MEMORY) AS max_total_memory"
		}
	} else if strings.HasPrefix(mysqlVersion, "5.7") || strings.HasPrefix(mysqlVersion, "5.6") {
		queryColumn = "DIGEST_TEXT AS query"
	} else {
		log.Fatalf("Error: MySQL version %s is not supported. This tool only supports MySQL 5.6 and above.", mysqlVersion)
	}

	statementAnalysisSQL = fmt.Sprintf(`
    SELECT
        IFNULL(SCHEMA_NAME,'') AS db,
        IF(SUM_NO_GOOD_INDEX_USED > 0 OR SUM_NO_INDEX_USED > 0, 'Y', 'N') AS full_scan,
        COUNT_STAR AS exec_count,
        SUM_ERRORS AS err_count,
        SUM_WARNINGS AS warn_count,
        SUM_TIMER_WAIT AS total_latency,
        MAX_TIMER_WAIT AS max_latency,
        AVG_TIMER_WAIT AS avg_latency,
        SUM_LOCK_TIME AS lock_latency,
        SUM_ROWS_SENT AS rows_sent,
        ROUND(IFNULL(SUM_ROWS_SENT / NULLIF(COUNT_STAR, 0), 0), 0) AS rows_sent_avg,
        SUM_ROWS_EXAMINED AS rows_examined,
        ROUND(IFNULL(SUM_ROWS_EXAMINED / NULLIF(COUNT_STAR, 0), 0), 0) AS rows_examined_avg,
        SUM_ROWS_AFFECTED AS rows_affected,
        ROUND(IFNULL(SUM_ROWS_AFFECTED / NULLIF(COUNT_STAR, 0), 0), 0) AS rows_affected_avg,
        SUM_CREATED_TMP_TABLES AS tmp_tables,
        SUM_CREATED_TMP_DISK_TABLES AS tmp_disk_tables,
        SUM_SORT_ROWS AS rows_sorted,
        SUM_SORT_MERGE_PASSES AS sort_merge_passes,
        DIGEST AS digest,
        DATE_FORMAT(FIRST_SEEN, '%%Y-%%m-%%d %%H:%%i:%%s') AS first_seen,
        DATE_FORMAT(LAST_SEEN, '%%Y-%%m-%%d %%H:%%i:%%s') AS last_seen,
        %s
    FROM performance_schema.events_statements_summary_by_digest
    ORDER BY total_latency DESC
`, queryColumn)
	fmt.Println(statementAnalysisSQL)

	type QuerySummary struct {
		RowNum          int
		SampleQuery     string `db:"query"`
		Database        string `db:"db"`
		FullScan        string `db:"full_scan"`
		ExecutionCount  int    `db:"exec_count"`
		ErrorCount      int    `db:"err_count"`
		WarningCount    int    `db:"warn_count"`
		TotalLatency    string `db:"total_latency"`
		MaxLatency      string `db:"max_latency"`
		AvgLatency      string `db:"avg_latency"`
		LockLatency     string `db:"lock_latency"`
		RowsSent        int    `db:"rows_sent"`
		RowsSentAvg     int    `db:"rows_sent_avg"`
		RowsExamined    int    `db:"rows_examined"`
		RowsExaminedAvg int    `db:"rows_examined_avg"`
		RowsAffected    int    `db:"rows_affected"`
		RowsAffectedAvg int    `db:"rows_affected_avg"`
		TmpTables       int    `db:"tmp_tables"`
		TmpDiskTables   int    `db:"tmp_disk_tables"`
		RowsSorted      int    `db:"rows_sorted"`
		SortMergePasses int    `db:"sort_merge_passes"`
		Digest          string `db:"digest"`
		FirstSeen       string `db:"first_seen"`
		LastSeen        string `db:"last_seen"`
		CpuTimeAvg      string `db:"cup_time_avg"`
		MaxTotalMemory  string `db:"max_total_memory"`
	}
	var QuerySummaries []QuerySummary
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = db.SelectContext(ctx, &QuerySummaries, statementAnalysisSQL)
	if err != nil {
		log.Fatalf("Error: Failed to retrieve query summaries from the database: %v", err)
	}
	for i, summary := range QuerySummaries {
		summary.RowNum = i + 1
		summary.TotalLatency = formatPicoTime(summary.TotalLatency)
		summary.MaxLatency = formatPicoTime(summary.MaxLatency)
		summary.AvgLatency = formatPicoTime(summary.AvgLatency)
		summary.LockLatency = formatPicoTime(summary.LockLatency)
		QuerySummaries[i] = summary
	}
	return map[string]interface{}{"slow_log_source": "performance_schema", "slowlogs": QuerySummaries, "now": now, "ip_port": fmt.Sprintf("%s:%d", host, port)}
}

func validateAndConstructCmd(pt, slowlog, since, until string, all, yday bool) string {

	if len(pt) == 0 || len(slowlog) == 0 {
		log.Fatalf("Error: Both --pt and --slowlog parameters are required.")
	}

	if all && (len(since) != 0 || len(until) != 0) {
		log.Fatalf("Error: The --all and --since (or --until) parameters are mutually exclusive.")

	}

	today := time.Now().Format("2006-01-02")
	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")

	parameter := make(map[string]string)
	if all {
		parameter["since"] = ""
		parameter["until"] = ""
	} else if len(since) != 0 || len(until) != 0 {
		if len(since) != 0 {
			parameter["since"] = "--since " + since
		}
		if len(until) != 0 {
			parameter["until"] = "--until " + until
		}
	} else if yday {
		parameter["since"] = "--since " + yesterday
		parameter["until"] = "--until " + today
	}
	ptQueryDigestCmd := strings.Join([]string{pt, parameter["since"], parameter["until"], slowlog}, " ")
	fmt.Println(ptQueryDigestCmd)
	return ptQueryDigestCmd

}

func main() {
	cst := time.FixedZone("CST", 8*60*60)
	currentTime = time.Now().In(cst)
	conf := Config{}
	conf.ParseFlags()
	report_content := make(map[string]interface{})
	now := currentTime.Format("2006-01-02 15:04:05")
	log.SetFlags(log.Lshortfile)
	if len(conf.Source) == 0 {
		log.Fatalf("Error: The --source parameter is required.")
	}
	if conf.Source == "perf" {
		if conf.Password == "" {
			fmt.Print("Enter MySQL password: ")
			bytePassword, err := terminal.ReadPassword(int(os.Stdin.Fd()))
			fmt.Println()
			if err != nil {
				log.Fatalf("Error: Failed to read the password - %v", err)
			}
			conf.Password = string(bytePassword)
		}
		report_content = getSlowLogSummaryFromPerformanceSchema(conf.Username, conf.Password, conf.Host, conf.Database, conf.Port, now)
	}

	if conf.Source == "slowlog" {
		query_cmd := validateAndConstructCmd(conf.PtCmd, conf.Slowlog, conf.Since, conf.Until, conf.All, conf.Yday)
		parts := strings.Fields(query_cmd)
		report_content = getSlowLogSummaryByPtQueryDigest(parts, now)
	}
	var report = template.Must(template.New("slowlog").Parse(temp))
	file, err := os.Create(conf.ResultFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	report.Execute(file, report_content)
	fmt.Println(fmt.Sprintf("Output written to file %s", conf.ResultFile))
}
