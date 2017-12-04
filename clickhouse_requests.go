package main

import (
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

	"os"
	//	"net/url"

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
	QueryDuration time.Duration
	ReadRows      uint64
	ReadBytes     uint64
	WrittenRows   uint64
	WrittenBytes  uint64
	ResultRows    uint64
	ResultBytes   uint64
	MemoryUsage   uint64
	Exception     string
	StackTrace    string
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

	duration_ms, _ := strconv.ParseUint(data[0][0], 10, 64)
	qs.QueryDuration = time.Duration(duration_ms) * time.Millisecond
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
	dataPacket     = iota
	errPacket      = iota
	donePacket     = iota
	statusPacket   = iota
	progressPacket = iota
)

type queryExecution struct {
	Err        error
	Data       string
	Progress   progressInfo
	Stats      queryStats
	StatusCode int
	PacketType int
}

func queryToStdout(cx context.Context, query, format string, interactive bool) int {
	queryID := uuid.NewV4().String()

	status := -1

	initProgress()

	queryExecutionChannel := makeQuery(cx, query, queryID, format, interactive)

Loop2:
	for {
		select {
		case qe := <-queryExecutionChannel:
			switch qe.PacketType {
			case dataPacket:
				data := qe.Data
				clearProgress(chcOutput.StdErr)
				io.WriteString(chcOutput.StdOut, data)
			case errPacket:
				log.Fatalln(qe.Err)
			case donePacket:
				count := qe.Stats.ResultRows
				duration := qe.Stats.QueryDuration
				clearProgress(chcOutput.StdErr)
				if opts.Time {
					if status == 200 {
						chcOutput.printServiceMsg(fmt.Sprintf("\n%v rows in set. Elapsed: %v\n\n", count, duration))
					} else {
						chcOutput.printServiceMsg(fmt.Sprintf("\nElapsed: %v\n\n", duration))
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
