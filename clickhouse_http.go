package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	//	"net"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

func getHost() string {
	return fmt.Sprintf("%s:%d", opts.Host, opts.Port)
}

func prepareRequestReader(query io.Reader, format string, extraSettings map[string]string) *http.Request {
	chURL := url.URL{}
	chURL.Scheme = "http"
	chURL.Host = getHost()
	chURL.Path = "/"

	qsParams := url.Values{}
	qsParams.Set("default_format", format)
	qsParams.Set("database", opts.Database)

	for k, v := range extraSettings {
		qsParams.Set(k, v) // TODO: for readonly mode we can set up only few parameters
	}

	chURL.RawQuery = qsParams.Encode()

	req, err := http.NewRequest("POST", chURL.String(), query)
	if err != nil {
		log.Fatalln(err)
	}

	req.Header.Set("User-Agent", "chc/"+versionString)
	req.SetBasicAuth(opts.User, opts.Password)
	return req
}

func prepareRequest(query, format string, extraSettings map[string]string) *http.Request {
	return prepareRequestReader(strings.NewReader(query), format, extraSettings)
}

func serviceRequestWithExtraSetting(query string, extraSettings map[string]string) (data [][]string, err error) {
	response, err2 := http.DefaultClient.Do(prepareRequest(query, "TSV", extraSettings))
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
	query := fmt.Sprintf("SELECT 'queryID %s killed by replace'", queryID)
	extraSettings := map[string]string{"log_queries": "0", "replace_running_query": "1", "queryID": queryID}

	_, err := serviceRequestWithExtraSetting(query, extraSettings)
	if err != nil {
		return false
	}

	return true
}
