package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	//	"github.com/davecgh/go-spew/spew"
	"io"
	"os/signal"
	"regexp"

	"github.com/davecgh/go-spew/spew"
	"github.com/satori/go.uuid" // generate sessionID and queryID
	// "io/ioutil"
	"log"
	//"math"
	"net"
	"net/http"
	"os"
	//	"net/url"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

var sessionID = uuid.NewV4().String()

type progressInfo struct {
	Elapsed         float64
	ReadRows        uint64
	ReadBytes       uint64
	TotalRowsApprox uint64
	WrittenRows     uint64
	WrittenBytes    uint64
	MemoryUsage     int64
}

type queryStats struct {
	QueryDurationMs uint64
	ReadRows        uint64
	ReadBytes       uint64
	WrittenRows     uint64
	WrittenBytes    uint64
	ResultRows      uint64
	ResultBytes     uint64
	MemoryUsage     uint64
	Exception       string
	StackTrace      string
}

func getServerVersion() (version string, err error) {
	data, err := serviceRequest("SELECT version()")
	if err != nil {
		return
	}
	version = data[0][0]
	return
}

func getProgressInfo(queryID string) (pi progressInfo, err error) {
	pi = progressInfo{}
	query := fmt.Sprintf("select elapsed,read_rows,read_bytes,total_rows_approx,written_rows,written_bytes,memory_usage from system.processes where query_id='%s'", queryID)
	data, err := serviceRequest(query)

	if err != nil {
		return
	}
	if len(data) != 1 || len(data[0]) != 7 {
		err = errors.New("Bad response dimensions")
		return
	}

	pi.Elapsed, _ = strconv.ParseFloat(data[0][0], 64)
	pi.ReadRows, _ = strconv.ParseUint(data[0][1], 10, 64)
	pi.ReadBytes, _ = strconv.ParseUint(data[0][2], 10, 64)
	pi.TotalRowsApprox, _ = strconv.ParseUint(data[0][3], 10, 64)
	pi.WrittenRows, _ = strconv.ParseUint(data[0][4], 10, 64)
	pi.WrittenBytes, _ = strconv.ParseUint(data[0][5], 10, 64)
	pi.MemoryUsage, _ = strconv.ParseInt(data[0][6], 10, 64)
	//	spew.Dump(pi)
	return
}

func getQueryStats(queryID string) (qs queryStats, err error) {

	query := fmt.Sprintf("select query_duration_ms,read_rows,read_bytes,written_rows,written_bytes,result_rows,result_bytes,memory_usage,exception,stack_trace,type from system.query_log where query_id='%s' and type>1", queryID)

	data, err := serviceRequest(query)

	if err != nil {
		return
	}
	if len(data) != 1 || len(data[0]) != 7 {
		err = errors.New("Bad response dimensions")
		return
	}

	qs.QueryDurationMs, _ = strconv.ParseUint(data[0][0], 10, 64)
	qs.ReadRows, _ = strconv.ParseUint(data[0][1], 10, 64)
	qs.ReadBytes, _ = strconv.ParseUint(data[0][2], 10, 64)
	qs.WrittenRows, _ = strconv.ParseUint(data[0][3], 10, 64)
	qs.WrittenBytes, _ = strconv.ParseUint(data[0][4], 10, 64)
	qs.ResultRows, _ = strconv.ParseUint(data[0][5], 10, 64)
	qs.ResultBytes, _ = strconv.ParseUint(data[0][6], 10, 64)
	qs.MemoryUsage, _ = strconv.ParseUint(data[0][7], 10, 64)
	qs.Exception = data[0][8]
	qs.StackTrace = data[0][9]
	return
}

func countRows(line, format string, counterState *int, count *int64) {
	switch format {
	case formatTabSeparated, "TSV", "CSV", "TSKV", "JSONEachRow", "TabSeparatedRaw", "TSVRaw":
		*count++
	case "TabSeparatedWithNames", "TSVWithNames", "CSVWithNames":
		if *counterState > 0 {
			*count++
		} else {
			*counterState++
		}
	case "TabSeparatedWithNamesAndTypes", "TSVWithNamesAndTypes", "PrettySpace":
		if *counterState > 1 {
			*count++
		} else {
			*counterState++
		}
	case "BlockTabSeparated":
		if *counterState == 0 {
			*count = int64(strings.Count(line, "\t")) + 1
			*counterState = 1
		}
	case "Pretty", "PrettyCompact", "PrettyCompactMonoBlock", "PrettyNoEscapes", "PrettyCompactNoEscapes", "PrettySpaceNoEscapes":
		if strings.HasPrefix(line, "│") {
			*count++
		}
	case formatVertical, "VerticalRaw":
		if strings.HasPrefix(line, "───") {
			*count++
		}
	case "JSON", "JSONCompact":
		lineTrimmed := strings.TrimSpace(line)
		switch *counterState {
		case 0: // waiting for data start
			if lineTrimmed == "\"data\":" {
				*counterState = 1
			}
		case 1: // waiting for </data> start
			if lineTrimmed == "]," {
				*counterState = 2
			}
		case 2: // waiting for <rows>
			if strings.HasPrefix(lineTrimmed, "\"rows\":") {
				lineTrimmed = strings.TrimPrefix(lineTrimmed, "\"rows\": ")
				lineTrimmed = strings.TrimSuffix(lineTrimmed, ",")
				*count, _ = strconv.ParseInt(lineTrimmed, 10, 64)
				*counterState = 3
			}
		}
	case "XML":
		lineTrimmed := strings.TrimSpace(line)
		switch *counterState {
		case 0: // waiting for <data> start
			if strings.HasPrefix(lineTrimmed, "<data>") {
				*counterState = 1
			}
		case 1: // waiting for </data> start
			if strings.HasPrefix(lineTrimmed, "</data>") {
				*counterState = 2
			}
		case 2: // waiting for <rows>
			if strings.HasPrefix(lineTrimmed, "<rows>") {
				lineTrimmed = strings.TrimPrefix(lineTrimmed, "<rows>")
				lineTrimmed = strings.TrimSuffix(lineTrimmed, "</rows>")
				*count, _ = strconv.ParseInt(lineTrimmed, 10, 64)
				*counterState = 3
			}
		}
	default:
		// case "Null","Native","RowBinary","Values","CapnProto","ODBCDriver":
		*count = -1
	}
}

func hasDataInStdin() bool {
	fi, err := os.Stdin.Stat()
	if err == nil {
		if fi.Mode()&os.ModeNamedPipe != 0 {
			return true
		}
	}
	return false
}

var useCmdRegexp = regexp.MustCompile("^\\s*(?i)use\\s+(\"\\w+\"|\\w+|`\\w+`)\\s*$")

// it will not match SET GLOBAL as set global not affect current session, according to docs
var setCmdRegexp = regexp.MustCompile("^\\s*(?i)set\\s+(?:\"\\w+\"|\\w+|\\`\\w+\\`)\\s*=\\s*(?:'([^']+)'|[0-9]+|NULL)")
var settingsRegexp = regexp.MustCompile("\\s*(\"\\w+\"|\\w+|\\`\\w+\\`)\\s*=\\s*('[^']+'|[0-9]+|NULL)\\s*,?")

func fireQuery(sqlToExequte, format string, interactive bool) {

	signalCh := make(chan os.Signal, 1)

	signal.Notify(signalCh, os.Interrupt)

	cx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queryFinished := make(chan bool)
	go func() {
		for {
			select {
			case <-signalCh:
				cancel()
			case <-queryFinished:
				return
			}
		}
	}()

	chcOutput.setupOutput(cancel)
	res := queryToStdout(cx, sqlToExequte, format, interactive)
	chcOutput.releaseOutput()
	if res == 200 {
		useCmdMatches := useCmdRegexp.FindStringSubmatch(sqlToExequte)
		if useCmdMatches != nil {
			opts.Database = strings.Trim(useCmdMatches[1], "\"`")
			chcOutput.printServiceMsg("Database changed to " + opts.Database + "\n")
		}
		if setCmdRegexp.MatchString(sqlToExequte) {
			settings := sqlToExequte[4:]
			settingsMatched := settingsRegexp.FindAllStringSubmatch(settings, -1)
			for _, match := range settingsMatched {
				clickhouseSetting[strings.Trim(match[1], "\"`")] = strings.Trim(match[2], "'")
			}
			spew.Dump(clickhouseSetting)
		}

	}
	queryFinished <- true
}

const (
	dataPacket = iota
	errPacket = iota
	donePacket = iota
	statusPacket = iota
	progressPacket = iota
)

type queryExecution struct {
	Err error
	Data string
	Progress progressInfo
	StatusCode int
	PacketType int
}

func queryToStdout(cx context.Context, query, format string, interactive bool) int {

	queryID := uuid.NewV4().String()
	var counterState int // = 0
	var count int64      // = 0
	status := -1

	queryExecutionChannel := make(chan queryExecution,2048)

	initProgress()

	start := time.Now()
	finishTickerChannel := make(chan bool, 3)

	if opts.Progress {

		go func() {

			ticker := time.NewTicker(time.Millisecond * 125)

		Loop3:
			for {
				select {
				case <-ticker.C:
					pi, err := getProgressInfo(queryID)
					if err == nil {
						qe := queryExecution{ Progress: pi, PacketType: progressPacket }
						queryExecutionChannel <- qe
					}
				case <-finishTickerChannel:
					break Loop3
				}
			}
			ticker.Stop()

			// println("ticker funct finished")
		}()

	}

	go func() {
		extraSettings := map[string]string{"log_queries": "1", "query_id": queryID, "session_id": sessionID}
		var req *http.Request
		var err error
		if interactive || !hasDataInStdin() {
			req, err = prepareRequest(query, format, extraSettings)
		} else {
			if len(query) > 0 {
				extraSettings["query"] = query

			}
			req, err = prepareRequestReader(os.Stdin, format, extraSettings)
		}
		if err != nil {
			qe := queryExecution{ Err: err, PacketType: errPacket }
			queryExecutionChannel <- qe
		}
		req = req.WithContext(cx)

		response, err := http.DefaultClient.Do(req)
		select {
		case <-cx.Done():
			// Already timedout
		default:
			if err != nil {
				qe := queryExecution{ Err: err, PacketType: errPacket }
				queryExecutionChannel <- qe
			} else {
				defer response.Body.Close()
				qe := queryExecution{ StatusCode: response.StatusCode, PacketType: statusPacket }
				queryExecutionChannel <- qe
				reader := bufio.NewReader(response.Body)
			Loop:
				for {
					//						io.WriteString(stdErr, "Debug P__\n\n" );
					select {
					case <-cx.Done():
						break Loop
					default:
						msg, err := reader.ReadString('\n')
						//spew.Dump(err)
						//spew.Dump(msg)
						if err == io.EOF {
							qe := queryExecution{ PacketType: donePacket }
							queryExecutionChannel <- qe
							break Loop
						} else if err == nil {
							qe := queryExecution{ PacketType: dataPacket, Data: msg }
							queryExecutionChannel <- qe
						} else {
							qe := queryExecution{ PacketType: errPacket, Err: err }
							queryExecutionChannel <- qe
							break Loop
						}
					}
				}
			}

		}
		//     println("do funct finished")
	}()
Loop2:
	for {
		select {
		case qe := <-queryExecutionChannel:
			switch qe.PacketType {
			case dataPacket:
				data := qe.Data
				clearProgress(chcOutput.StdErr)
				countRows(data, format, &counterState, &count)
				io.WriteString(chcOutput.StdOut, data)
			case errPacket:
				log.Fatalln(qe.Err)
			case donePacket:
				finishTickerChannel <- true // we use buffered channel here to avoid deadlocks
				clearProgress(chcOutput.StdErr)
				if opts.Time {
					if status == 200 {
						chcOutput.printServiceMsg(fmt.Sprintf("\n%v rows in set. Elapsed: %v\n\n", count, time.Since(start)))
					} else {
						chcOutput.printServiceMsg(fmt.Sprintf("\nElapsed: %v\n\n", time.Since(start)))
					}
				}
				break Loop2
			case statusPacket:
				status = qe.StatusCode
			case progressPacket:
				pi := qe.Progress
				writeProgres(chcOutput.StdErr, pi.ReadRows, pi.ReadBytes, pi.TotalRowsApprox, uint64(pi.Elapsed*1000000000))
			}
		case <-cx.Done():
			finishTickerChannel <- true // aware deadlocks here, we uses buffered channel here
			clearProgress(chcOutput.StdErr)
			chcOutput.printServiceMsg(fmt.Sprintf("\nKilling query (id: %v)... ", queryID))
			if killQuery(queryID) {
				chcOutput.printServiceMsg("killed!\n\n")
			} else {
				chcOutput.printServiceMsg("failure!\n\n")
			}
			break Loop2
		}
	}
	return status
	// io.WriteString(stdErr, "queryToStdout finished" );
}

// another options - with progress in heeaders
func queryToStdout2(query string) {
	stdOutBuffered := bufio.NewWriter(chcOutput.StdOut)
	stdErrBuffered := bufio.NewWriter(chcOutput.StdErr)

	extraSettings := map[string]string{"send_progress_in_http_headers": "1"}

	req, _ := prepareRequest(query, opts.Format, extraSettings)

	initProgress()
	start := time.Now()

	// connect to this socket
	conn, err := net.Dial("tcp", getHost())
	if err != nil {
		log.Fatalln(err) // TODO - process that / retry?
	}

	err = req.Write(conn)
	if err != nil {
		log.Fatalln(err) // TODO - process that / retry?
	}
	var requestBeginning bytes.Buffer
	tee := io.TeeReader(conn, &requestBeginning)
	reader := bufio.NewReader(tee)
	for {
		msg, err2 := reader.ReadString('\n')
		if err2 == io.EOF {
			break
			// Ups... We have EOF before HTTP headers finished...
			// TODO - process that / retry?
		}
		if err2 != nil {
			log.Fatalln(err2) // TODO - process that / retry?
		}
		message := strings.TrimSpace(msg)
		if message == "" {
			break // header finished
		}

		//	fmt.Print(message)
		if strings.HasPrefix(message, "X-ClickHouse-Progress:") {
			type ProgressData struct {
				ReadRows  uint64 `json:"read_rows,string"`
				ReadBytes uint64 `json:"read_bytes,string"`
				TotalRows uint64 `json:"total_rows,string"`
			}

			progressDataJSON := strings.TrimSpace(message[22:])
			var pd ProgressData
			err3 := json.Unmarshal([]byte(progressDataJSON), &pd)
			if err3 != nil {
				log.Fatal(err3)
			}

			writeProgres(stdErrBuffered, pd.ReadRows, pd.ReadBytes, pd.TotalRows, uint64(time.Since(start)))
			stdErrBuffered.Flush()
		}
	}

	reader2 := io.MultiReader(&requestBeginning, conn)
	reader3 := bufio.NewReader(reader2)
	res, err := http.ReadResponse(reader3, req)
	if err != nil {
		log.Fatal(err)
	}

	clearProgress(stdErrBuffered)
	stdErrBuffered.Flush()

	//fmt.Println(res.StatusCode)
	//fmt.Println(res.ContentLength)
	defer res.Body.Close()
	_, err = io.Copy(stdOutBuffered, res.Body)
	if err != nil {
		log.Fatal(err)
	}
	stdOutBuffered.Flush()
	//  fmt.Println(res.Body.Read())
}
