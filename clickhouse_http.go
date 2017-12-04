package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"
	// "log"
	//	"net"
	"context"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

func getHost() string {
	return fmt.Sprintf("%s:%d", opts.Host, opts.Port)
}

func prepareRequestReader(query io.Reader, format string, extraSettings map[string]string) (req *http.Request, err error) {
	chURL := url.URL{}
	chURL.Scheme = opts.Protocol
	chURL.Host = getHost()
	chURL.Path = "/"

	qsParams := url.Values{}
	qsParams.Set("default_format", format)
	qsParams.Set("database", opts.Database)

	if opts.Stacktrace {
		qsParams.Set("stacktrace", "1")
	}

	for k, v := range extraSettings {
		qsParams.Set(k, v) // TODO: for readonly mode we can set up only few parameters
	}

	chURL.RawQuery = qsParams.Encode()

	req, err = http.NewRequest("POST", chURL.String(), query)
	if err != nil {
		return
	}

	req.Header.Set("User-Agent", "chc/"+versionString)
	req.SetBasicAuth(opts.User, opts.Password)
	return
}

func prepareRequest(query, format string, extraSettings map[string]string) (req *http.Request, err error) {
	return prepareRequestReader(strings.NewReader(query), format, extraSettings)
}

// TODO: context with timeout
func serviceRequestWithExtraSetting(query string, extraSettings map[string]string) (data [][]string, err error) {

	timeout := time.Duration(3 * time.Second)
	cx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err0 := prepareRequest(query, formatTabSeparated, extraSettings)

	if err0 != nil {
		err = err0
		return
	}

	req = req.WithContext(cx)

	response, err2 := http.DefaultClient.Do(req)
	if err2 != nil {
		err = err2
		return
	}

	defer response.Body.Close()

	if response.StatusCode != 200 {
		v, err3 := ioutil.ReadAll(response.Body)
		if err3 != nil {
			err = err3
			return
		}
		err = errors.New(strings.TrimSpace(string(v)))
		return
	}

	data, err = readTabSeparated(response.Body)
	return
}

func serviceRequest(query string) (data [][]string, err error) {
	extraSettings := map[string]string{"log_queries": "0"}
	return serviceRequestWithExtraSetting(query, extraSettings)
}

func killQuery(queryID string) bool {
	query := fmt.Sprintf("SELECT 'query_id %s killed by replace'", queryID)
	extraSettings := map[string]string{"log_queries": "0", "replace_running_query": "1", "query_id": queryID}

	_, err := serviceRequestWithExtraSetting(query, extraSettings)
	if err != nil {
		return false
	}

	return true
}

type queryExecutionChan chan queryExecution

func makeQuery(cx context.Context, query, queryID, format string, interactive bool) queryExecutionChan {

	queryExecutionChannel := make(chan queryExecution, 2048)
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
						qe := queryExecution{Progress: pi, PacketType: progressPacket}
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
		start := time.Now()
		var count uint64 // = 0
		countRows := getRowsCounter(format)
		extraSettings := map[string]string{"log_queries": "1", "query_id": queryID, "session_id": sessionID}
		defer func() { finishTickerChannel <- true }()
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
			qe := queryExecution{Err: err, PacketType: errPacket}
			queryExecutionChannel <- qe
			return
		}
		req = req.WithContext(cx)

		response, err := http.DefaultClient.Do(req)
		select {
		case <-cx.Done():
			// Already timedout
		default:
			if err != nil {
				qe := queryExecution{Err: err, PacketType: errPacket}
				queryExecutionChannel <- qe
			} else {
				defer response.Body.Close()
				qe := queryExecution{StatusCode: response.StatusCode, PacketType: statusPacket}
				queryExecutionChannel <- qe
				bodyReader := bufio.NewReader(response.Body)
			Loop:
				for {
					//						io.WriteString(stdErr, "Debug P__\n\n" );
					select {
					case <-cx.Done():
						break Loop
					default:
						msg, err := bodyReader.ReadString('\n')
						count = countRows(msg)
						//spew.Dump(err)
						//spew.Dump(msg)
						if err == io.EOF {
							stats := queryStats{QueryDuration: time.Since(start), ResultRows: count}
							qe := queryExecution{PacketType: donePacket, Stats: stats}
							queryExecutionChannel <- qe
							break Loop
						} else if err == nil {
							qe := queryExecution{PacketType: dataPacket, Data: msg}
							queryExecutionChannel <- qe
						} else {
							qe := queryExecution{PacketType: errPacket, Err: err}
							queryExecutionChannel <- qe
							break Loop
						}
					}
				}
			}

		}
		//     println("do funct finished")
	}()
	return queryExecutionChannel

}

// another options - with progress in heeaders
func makeQuery2(cx context.Context, query, queryID, format string, interactive bool) queryExecutionChan {

	queryExecutionChannel := make(chan queryExecution, 2048)

	go func() {
		start := time.Now()
		var count uint64 // = 0
		countRows := getRowsCounter(format)

		extraSettings := map[string]string{"send_progress_in_http_headers": "1"}

		req, err := prepareRequest(query, opts.Format, extraSettings)
		if err != nil {
			qe := queryExecution{Err: err, PacketType: errPacket}
			queryExecutionChannel <- qe
			return
		}

		conn, err := net.Dial("tcp", getHost()) // todo: ssl
		defer conn.Close()                      // todo: keepalive
		if err != nil {
			qe := queryExecution{Err: err, PacketType: errPacket}
			queryExecutionChannel <- qe
			return
		}

		err = req.Write(conn)
		if err != nil {
			qe := queryExecution{Err: err, PacketType: errPacket}
			queryExecutionChannel <- qe
			return
		}

		var requestBeginning bytes.Buffer
		tee := io.TeeReader(conn, &requestBeginning)
		reader := bufio.NewReader(tee)

	HeadersReadLoop:
		for {
			select {
			case <-cx.Done():
				return // Already timedout
			default:
				msg, err2 := reader.ReadString('\n')
				if err2 != nil {
					qe := queryExecution{Err: err2, PacketType: errPacket}
					queryExecutionChannel <- qe
					return
					// Ups... We have error/EOF before HTTP headers finished...
				}
				message := strings.TrimSpace(msg)
				if message == "" {
					break HeadersReadLoop // header finished
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
					if err3 == nil { // just ignore error here
						pi := progressInfo{ReadRows: pd.ReadRows, Elapsed: float64(time.Since(start) / time.Second), ReadBytes: pd.ReadBytes, TotalRowsApprox: pd.TotalRows}
						qe := queryExecution{Progress: pi, PacketType: progressPacket}
						queryExecutionChannel <- qe
					}
				}
			}
		}

		reader2 := io.MultiReader(&requestBeginning, conn)
		reader3 := bufio.NewReader(reader2)
		res, err := http.ReadResponse(reader3, req)
		if err != nil {
			qe := queryExecution{Err: err, PacketType: errPacket}
			queryExecutionChannel <- qe
			return
		}
		qe := queryExecution{StatusCode: res.StatusCode, PacketType: statusPacket}
		queryExecutionChannel <- qe
		defer res.Body.Close()
		bodyReader := bufio.NewReader(res.Body)

	Loop:
		for {
			select {
			case <-cx.Done():
				return // Already timedout
			default:
				msg, err := bodyReader.ReadString('\n')
				count = countRows(msg)
				//spew.Dump(err)
				//spew.Dump(msg)
				if err == io.EOF {
					stats := queryStats{QueryDuration: time.Since(start), ResultRows: count}
					qe := queryExecution{PacketType: donePacket, Stats: stats}
					queryExecutionChannel <- qe
					break Loop
				} else if err == nil {
					qe := queryExecution{PacketType: dataPacket, Data: msg}
					queryExecutionChannel <- qe
				} else {
					qe := queryExecution{PacketType: errPacket, Err: err}
					queryExecutionChannel <- qe
					break Loop
				}
			}
		}
	}()
	return queryExecutionChannel
}
