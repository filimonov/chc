package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
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
	cx, _ := context.WithTimeout(context.Background(), timeout)

	req, err0 := prepareRequest(query, "TSV", extraSettings)

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

	tsvReader := csv.NewReader(response.Body)
	tsvReader.Comma = '\t'
	data, err = tsvReader.ReadAll()
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
