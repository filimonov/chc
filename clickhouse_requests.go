package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	//	"github.com/davecgh/go-spew/spew"
	"github.com/satori/go.uuid" // generate session_id and query_id
	"io"
	// "io/ioutil"
	"log"
	//"math"
	"net"
	"net/http"
	//	"net/url"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

var session_id string = uuid.NewV4().String()

type ProgressInfo struct {
	Elapsed         float64
	ReadRows        uint64
	ReadBytes       uint64
	TotalRowsApprox uint64
	WrittenRows     uint64
	WrittenBytes    uint64
	MemoryUsage     int64
}

type QueryStats struct {
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

func get_server_version() (version string, err error) {
	data, err := service_request("SELECT version()")
	if err != nil {
		return
	}
	version = data[0][0]
	return
}

func get_progress_info(query_id string) (pi ProgressInfo, err error) {
	pi = ProgressInfo{}
	query := fmt.Sprintf("select elapsed,read_rows,read_bytes,total_rows_approx,written_rows,written_bytes,memory_usage from system.processes where query_id='%s'", query_id)

	data, err := service_request(query)

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
	//spew.Dump(pi)
	return
}

func get_query_stats(query_id string) (qs QueryStats, err error) {

	query := fmt.Sprintf("select query_duration_ms,read_rows,read_bytes,written_rows,written_bytes,result_rows,result_bytes,memory_usage,exception,stack_trace,type from system.query_log where query_id='%s' and type>1", query_id)

	data, err := service_request(query)

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

func query_to_stdout(query string, stdOut, stdErr io.Writer, cx context.Context) int {

	query_id := uuid.NewV4().String()
	status := -1

	error_channel := make(chan error)
	data_channel := make(chan string)
	done_channel := make(chan bool)
	statuscode_channel := make(chan int)
	progress_channel := make(chan ProgressInfo)

	initProgress()

	start := time.Now()
	finish_ticker_channel := make(chan bool, 3)

	go func() {

		ticker := time.NewTicker(time.Millisecond * 125)

	Loop3:
		for {
			select {
			case <-ticker.C:
				pi, err := get_progress_info(query_id)
				if err == nil {
					progress_channel <- pi
				}
			case <-finish_ticker_channel:
				break Loop3
			}
		}
		ticker.Stop()

		// println("ticker funct finished")
	}()

	go func() {
		extra_settings := map[string]string{"log_queries": "1", "query_id": query_id, "session_id": session_id}
		req := prepare_request(query, opts.Format, extra_settings).WithContext(cx)

		response, err := http.DefaultClient.Do(req)
		select {
		case <-cx.Done():
			// Already timedout
		default:
			if err != nil {
				error_channel <- err
			} else {
				defer response.Body.Close()

				statuscode_channel <- response.StatusCode
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
							done_channel <- true
							break Loop
						} else if err == nil {
							data_channel <- msg
						} else {
							error_channel <- err
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
		case st := <-statuscode_channel:
			status = st
		case <-cx.Done():
			finish_ticker_channel <- true // aware deadlocks here, we uses buffered channel here
			clearProgress(stdErr)
			io.WriteString(stdErr, fmt.Sprintf("\nKilling query (id: %v)... ", query_id))
			if kill_query(query_id) {
				io.WriteString(stdErr, "killed!\n\n")
			} else {
				io.WriteString(stdErr, "failure!\n\n")
			}
			break Loop2
		case err := <-error_channel:
			log.Fatalln(err)
		case pi := <-progress_channel:
			writeProgres(stdErr, pi.ReadRows, pi.ReadBytes, pi.TotalRowsApprox, uint64(pi.Elapsed*1000000000))
		case <-done_channel:
			finish_ticker_channel <- true // aware deadlocks here, we uses buffered channel here
			clearProgress(stdErr)
			io.WriteString(stdErr, fmt.Sprintf("\nElapsed: %v\n\n", time.Since(start)))
			break Loop2
		case data := <-data_channel:
			clearProgress(stdErr)
			io.WriteString(stdOut, data)
		}
	}
	return status
	// io.WriteString(stdErr, "query_to_stdout finished" );
}

func query_to_stdout2(query string, stdOut, stdErr io.Writer) {
	stdOut_buffered := bufio.NewWriter(stdOut)
	stdErr_buffered := bufio.NewWriter(stdErr)

	extra_settings := map[string]string{"send_progress_in_http_headers": "1"}

	req := prepare_request(query, opts.Format, extra_settings)

	initProgress()
	start := time.Now()

	// connect to this socket
	conn, err := net.Dial("tcp", get_host())
	if err != nil {
		log.Fatalln(err) // TODO - process that / retry?
	}

	err = req.Write(conn)
	if err != nil {
		log.Fatalln(err) // TODO - process that / retry?
	}

	var request_begining bytes.Buffer
	tee := io.TeeReader(conn, &request_begining)
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

			progress_data_json := strings.TrimSpace(message[22:])
			var pd ProgressData
			err := json.Unmarshal([]byte(progress_data_json), &pd)
			if err != nil {
				log.Fatal(err)
			}

			writeProgres(stdErr_buffered, pd.ReadRows, pd.ReadBytes, pd.TotalRows, uint64(time.Since(start)))
			stdErr_buffered.Flush()
		}
	}

	reader2 := io.MultiReader(&request_begining, conn)
	reader3 := bufio.NewReader(reader2)
	res, err := http.ReadResponse(reader3, req)
	if err != nil {
		log.Fatal(err)
	}

	clearProgress(stdErr_buffered)
	stdErr_buffered.Flush()

	//fmt.Println(res.StatusCode)
	//fmt.Println(res.ContentLength)
	defer res.Body.Close()
	_, err = io.Copy(stdOut_buffered, res.Body)
	if err != nil {
		log.Fatal(err)
	}
	stdOut_buffered.Flush()
	//  fmt.Println(res.Body.Read())
}
