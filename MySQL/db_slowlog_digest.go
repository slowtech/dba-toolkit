package main

import (
	"strings"
	"fmt"
	"os/exec"
	"regexp"
        "html/template"
        "os"
        "flag"
        "time"
)

const temp = `
<!DOCTYPE html>
<html>
<head>
    <title>Slow Log</title>
<style>

body {
     font-family: Arial,sans-serif;
     background: #e8eaee;
     font-size: 14px;
}
.d1 {
    background: #fff;
    padding: 10px 30px 40px;
    width: 83.33333333%;
}
.d2 {
    align-items: center;
    justify-content: center;
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;
    margin: 0;
    padding: 0;
}

table {
    *border-collapse: collapse; /* IE7 and lower */
    border-spacing: 0;
    width: 100%;    
}

.bordered {
    border: solid #ccc 1px;
    -moz-border-radius: 6px;
    -webkit-border-radius: 6px;
    border-radius: 6px;
    -webkit-box-shadow: 0 1px 1px #ccc; 
    -moz-box-shadow: 0 1px 1px #ccc; 
    box-shadow: 0 1px 1px #ccc;         
    table-layout: fixed; 
    width:100%;  
}

.bordered tr:hover {
    background: #fbf8e9;
    -o-transition: all 0.1s ease-in-out;
    -webkit-transition: all 0.1s ease-in-out;
    -moz-transition: all 0.1s ease-in-out;
    -ms-transition: all 0.1s ease-in-out;
    transition: all 0.1s ease-in-out;     
}    
    
.bordered td, .bordered th {
    border-left: 1px solid #ccc;
    border-top: 1px solid #ccc;
    padding: 10px;
    text-align: left;    
    word-wrap:break-word;   

}

.bordered th {
    background-color: #dce9f9;
    background-image: -webkit-gradient(linear, left top, left bottom, from(#ebf3fc), to(#dce9f9));
    background-image: -webkit-linear-gradient(top, #ebf3fc, #dce9f9);
    background-image:    -moz-linear-gradient(top, #ebf3fc, #dce9f9);
    background-image:     -ms-linear-gradient(top, #ebf3fc, #dce9f9);
    background-image:      -o-linear-gradient(top, #ebf3fc, #dce9f9);
    background-image:         linear-gradient(top, #ebf3fc, #dce9f9);
    -webkit-box-shadow: 0 1px 0 rgba(255,255,255,.8) inset; 
    -moz-box-shadow:0 1px 0 rgba(255,255,255,.8) inset;  
    box-shadow: 0 1px 0 rgba(255,255,255,.8) inset;        
    border-top: none;
    text-shadow: 0 1px 0 rgba(255,255,255,.5); 
}

.bordered td:first-child, .bordered th:first-child {
    border-left: none;
}

.bordered th:first-child {
    -moz-border-radius: 6px 0 0 0;
    -webkit-border-radius: 6px 0 0 0;
    border-radius: 6px 0 0 0;
}

.bordered th:last-child {
    -moz-border-radius: 0 6px 0 0;
    -webkit-border-radius: 0 6px 0 0;
    border-radius: 0 6px 0 0;
}

.bordered th:only-child{
    -moz-border-radius: 6px 6px 0 0;
    -webkit-border-radius: 6px 6px 0 0;
    border-radius: 6px 6px 0 0;
}

.bordered tr:last-child td:first-child {
    -moz-border-radius: 0 0 0 6px;
    -webkit-border-radius: 0 0 0 6px;
    border-radius: 0 0 0 6px;
}

.bordered tr:last-child td:last-child {
    -moz-border-radius: 0 0 6px 0;
    -webkit-border-radius: 0 0 6px 0;
    border-radius: 0 0 6px 0;
}
 
</style>
</head>

<body>
<div class="d2">
<div class="d1">
<h2 style="text-align: center;margin-bottom:0px">Slow Log</h2>
<span style="font-weight: bold;float:right;font-size:12px;margin-bottom:15px">生成时间：{{.now}}</span> 
<table class="bordered">
    <thead>
    <tr>
        <th style="width:4%">Rank</th>        
        <th style="width:7%">Response time</th>
        <th style="width:6%">Response ratio</th>
        <th style="width:5%">Calls</th>        
        <th style="width:6%">R/Call</th>
        <th style="width:15%">QueryId</th>
        <th style="width:44%">Example</th>
	<th style="width:13%">Remark</th>
    </tr>
    </thead>
	{{range .slowlogs}}
    <tr>
        <td style="width:4%">{{ .Rank}}</td>        
        <td style="width:7%">{{ .Response_time}}</td>
        <td style="width:6%">{{ .Response_ratio}}</td>
	<td style="width:5%">{{ .Calls}}</td>        
        <td style="width:6%">{{ .R_Call}}</td>
        <td style="width:15%">{{ .QueryId}}</td>
        <td style="width:44%">{{ .Example}}</td>
	<td style="width:13%"> </td>   
    </tr>  
    {{end}}	
</table>
</div>
</div>
</body>
</html>
`
var  (
    help bool
    since string
    until string
    all bool
    pt string
    slowlog string
    yday bool
)

func init() {
    flag.BoolVar(&help,"help",false, "Display usage")
    flag.StringVar(&since,"since","","Parse only queries newer than this value,YYYY-MM-DD [HH:MM:SS]")
    flag.StringVar(&until,"until","","Parse only queries older than this value,YYYY-MM-DD [HH:MM:SS]")
    flag.BoolVar(&all,"all",false,"Parse the whole slowlog")
    flag.BoolVar(&yday,"yday",true,"Parse yesterday's slowlog")
    flag.StringVar(&pt,"pt","","Absolute path for pt-query-digest. Example:/usr/local/percona-toolkit/bin/pt-query-digest")
    flag.StringVar(&slowlog,"slowlog","","Absolute path for slowlog. Example:/var/log/mysql/node1-slow.log")
}

func main() {
        flag.Parse()
        if help {
              fmt.Fprintf(os.Stdout, `db-slowlog-digest version: 1.0.0
Usage: 
db-slowlog-digest --pt /usr/bin/pt-query-digest --slowlog /var/log/mysql/node1-slow.log
 Or
db-slowlog-digest --pt /usr/bin/pt-query-digest --slowlog /var/log/mysql/node1-slow.log --all
 Or   
db-slowlog-digest --pt /usr/bin/pt-query-digest --slowlog /var/log/mysql/node1-slow.log --since "20180101" --until "20180108"

Options:
`)
         flag.PrintDefaults()
         return
}
  
        if len(pt) ==0 || len(slowlog)==0 {
           fmt.Println("--pt and --slowlog are both required")
           return
        }
        if all && (len(since) !=0 || len(until) !=0)  {
            fmt.Println("--all and --since(--until) are mutually exclusive")
            return
        }
              
        today := time.Now().Format("2006-01-02")
        yesterday := time.Now().AddDate(0,0,-1).Format("2006-01-02")

        parameter := make(map[string]string)
        if all {
            parameter["since"]=""
            parameter["until"]=""
        } else if len(since) !=0 || len(until) !=0 { 
            if len(since) !=0 {
               parameter["since"]="--since "+since
            }
            if len(until) !=0 {
               parameter["until"]="--until "+until
            }
        } else {
            parameter["since"]="--since "+yesterday
            parameter["until"]="--until "+today
        }
        ptQueryDigestCmd :=  strings.Join([]string{"perl",pt,parameter["since"],parameter["until"],slowlog}," ")
        //fmt.Println(ptQueryDigestCmd)
	parseSlowLog(ptQueryDigestCmd)
}

func parseSlowLog(ptQueryDigestCmd string) {
        slowLog := execCmd("perl", ptQueryDigestCmd)
	lines := strings.Split(string(slowLog), "\n")
	linesNums := len(lines)
	profileFlag := false
	exampleFlag := false
	exampleSQL := []string{}
	slowLogProfile := [][]string{}
	exampleSQLs := make(map[string]string)
	var queryID string
	for k,line := range lines {
		if strings.Contains(line,"# Profile"){
			profileFlag = true
                        continue
		} else if profileFlag && (len(line) == 0 || strings.HasPrefix(line,"# MISC 0xMISC")) {
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
		} else if strings.Contains(line,"concurrency, ID 0x"){
			re := regexp.MustCompile(`(?U)ID (0x.*) `)
			queryID = re.FindStringSubmatch(line)[1]
			exampleFlag = true
			exampleSQL = []string{}
		}else if exampleFlag && (! strings.HasPrefix(line,"#")) && len(line) !=0 {
			exampleSQL=append(exampleSQL,line)
		}else if exampleFlag && (len(line) == 0 || k == (linesNums-1)){
			exampleFlag = false
			exampleSQLs[queryID] = strings.Join(exampleSQL,"\n")
		}
	}

        for _,v := range slowLogProfile {
            v[8] = exampleSQLs[v[2]]
           }

        type slowlog struct {
                Rank string
                Response_time string
                Response_ratio string
                Calls string
                R_Call string
                QueryId string
                Example string
        }
        
	now := time.Now().Format("2006-01-02 15:04:05")
        slowlogs := []slowlog{}
        for _,value := range slowLogProfile {
            slowlogrecord := slowlog{value[1],value[3],value[4],value[5],value[6],value[2],value[8]}
            slowlogs = append(slowlogs,slowlogrecord)
        }
        var report = template.Must(template.New("slowlog").Parse(temp))
        report.Execute(os.Stdout,map[string]interface{}{"slowlogs":slowlogs,"now":now})

}


func execCmd(cmd_type string, cmd string) string {
	out, err := exec.Command("bash", "-c", cmd).Output()
	if cmd_type != "shell" {
		parts := strings.Fields(cmd)
		head := parts[0]
		parts = parts[1:len(parts)]
		out, err = exec.Command(head, parts...).Output()
	}
	if err != nil {
		fmt.Println("Failed to execute command:", cmd)
                os.Exit(1)
	}
	return string(out)
}
