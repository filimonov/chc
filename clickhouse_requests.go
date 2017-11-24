package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	//	"github.com/davecgh/go-spew/spew"
	"github.com/satori/go.uuid" // generate sessionID and queryID
	"io"
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

var sessionID string = uuid.NewV4().String()

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

func countRows(line, format string, counter_state *int, count *int64) {
	switch format {
	case "TabSeparated", "TSV", "CSV", "TSKV", "JSONEachRow", "TabSeparatedRaw", "TSVRaw":
		*count++
	case "TabSeparatedWithNames", "TSVWithNames", "CSVWithNames":
		if *counter_state > 0 {
			*count++
		} else {
			*counter_state++
		}
	case "TabSeparatedWithNamesAndTypes", "TSVWithNamesAndTypes", "PrettySpace":
		if *counter_state > 1 {
			*count++
		} else {
			*counter_state++
		}
	case "BlockTabSeparated":
		if *counter_state == 0 {
			*count = int64(strings.Count(line, "\t")) + 1
			*counter_state = 1
		}
	case "Pretty", "PrettyCompact", "PrettyCompactMonoBlock", "PrettyNoEscapes", "PrettyCompactNoEscapes", "PrettySpaceNoEscapes":
		if strings.HasPrefix(line, "│") {
			*count++
		}
	case "Vertical", "VerticalRaw":
		if strings.HasPrefix(line, "───") {
			*count++
		}
	case "JSON", "JSONCompact":
		line_trimmed := strings.TrimSpace(line)
		switch *counter_state {
		case 0: // waiting for data start
			if line_trimmed == "\"data\":" {
				*counter_state = 1
			}
		case 1: // waiting for </data> start
			if line_trimmed == "]," {
				*counter_state = 2
			}
		case 2: // waiting for <rows>
			if strings.HasPrefix(line_trimmed, "\"rows\":") {
				line_trimmed = strings.TrimPrefix(line_trimmed, "\"rows\": ")
				line_trimmed = strings.TrimSuffix(line_trimmed, ",")
				*count, _ = strconv.ParseInt(line_trimmed, 10, 64)
				*counter_state = 3
			}
		}
	case "XML":
		line_trimmed := strings.TrimSpace(line)
		switch *counter_state {
		case 0: // waiting for <data> start
			if strings.HasPrefix(line_trimmed, "<data>") {
				*counter_state = 1
			}
		case 1: // waiting for </data> start
			if strings.HasPrefix(line_trimmed, "</data>") {
				*counter_state = 2
			}
		case 2: // waiting for <rows>
			if strings.HasPrefix(line_trimmed, "<rows>") {
				line_trimmed = strings.TrimPrefix(line_trimmed, "<rows>")
				line_trimmed = strings.TrimSuffix(line_trimmed, "</rows>")
				*count, _ = strconv.ParseInt(line_trimmed, 10, 64)
				*counter_state = 3
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

func queryToStdout(cx context.Context, query, format string, interactive bool) int {

	queryID := uuid.NewV4().String()
	var counter_state int = 0
	var count int64 = 0
	status := -1

	errorChannel := make(chan error)
	dataChannel := make(chan string)
	doneChannel := make(chan bool)
	statusCodeChannel := make(chan int)
	progressChannel := make(chan progressInfo)

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
						progressChannel <- pi
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
			errorChannel <- err
			return
		}
		req = req.WithContext(cx)

		response, err := http.DefaultClient.Do(req)
		select {
		case <-cx.Done():
			// Already timedout
		default:
			if err != nil {
				errorChannel <- err
			} else {
				defer response.Body.Close()

				statusCodeChannel <- response.StatusCode
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
							doneChannel <- true
							break Loop
						} else if err == nil {
							dataChannel <- msg
						} else {
							errorChannel <- err
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
		case st := <-statusCodeChannel:
			status = st
		case <-cx.Done():
			finishTickerChannel <- true // aware deadlocks here, we uses buffered channel here
			clearProgress(stdErr)
			printServiceMsg(fmt.Sprintf("\nKilling query (id: %v)... ", queryID))
			if killQuery(queryID) {
				printServiceMsg("killed!\n\n")
			} else {
				printServiceMsg("failure!\n\n")
			}
			break Loop2
		case err := <-errorChannel:
			log.Fatalln(err)
		case pi := <-progressChannel:
			writeProgres(stdErr, pi.ReadRows, pi.ReadBytes, pi.TotalRowsApprox, uint64(pi.Elapsed*1000000000))
		case <-doneChannel:
			finishTickerChannel <- true // we use buffered channel here to avoid deadlocks
			clearProgress(stdErr)
			if opts.Progress {
				if status == 200 {
					printServiceMsg(fmt.Sprintf("\n%v row(s) in %v\n\n", count, time.Since(start)))
				} else {
					printServiceMsg(fmt.Sprintf("\nElapsed: %v\n\n", time.Since(start)))
				}
			}
			break Loop2
		case data := <-dataChannel:
			clearProgress(stdErr)
			countRows(data, format, &counter_state, &count)
			io.WriteString(stdOut, data)
		}
	}
	return status
	// io.WriteString(stdErr, "queryToStdout finished" );
}

func queryToStdout2(query string, stdOut, stdErr io.Writer) {
	stdOutBuffered := bufio.NewWriter(stdOut)
	stdErrBuffered := bufio.NewWriter(stdErr)

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
		msg, err := reader.ReadString('\n')
		if err == io.EOF {
			break
			// Ups... We have EOF before HTTP headers finished...
			// TODO - process that / retry?
		}
		if err != nil {
			log.Fatalln(err) // TODO - process that / retry?
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
			err := json.Unmarshal([]byte(progressDataJSON), &pd)
			if err != nil {
				log.Fatal(err)
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
