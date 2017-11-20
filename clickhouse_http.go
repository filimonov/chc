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

func get_host() string {
	return fmt.Sprintf("%s:%d", opts.Host, opts.Port)
}

func prepare_request_reader(query io.Reader, format string, extra_settings map[string]string) *http.Request {
	ch_url := url.URL{}
	ch_url.Scheme = "http"
	ch_url.Host = get_host()
	ch_url.Path = "/"

	qs_params := url.Values{}
	qs_params.Set("default_format", format)
	qs_params.Set("database", opts.Database)

	for k, v := range extra_settings {
		qs_params.Set(k, v) // TODO: for readonly mode we can set up only few parameters
	}

	ch_url.RawQuery = qs_params.Encode()

	req, err := http.NewRequest("POST", ch_url.String(), query)
	if err != nil {
		log.Fatalln(err)
	}

	req.Header.Set("User-Agent", "chc/"+VERSION_STRING)
	req.SetBasicAuth(opts.User, opts.Password)
	return req
}

func prepare_request(query, format string, extra_settings map[string]string) *http.Request {
	return prepare_request_reader(strings.NewReader(query), format, extra_settings)
}

func service_request_with_extra_setting(query string, extra_settings map[string]string) (data [][]string, err error) {
	response, err2 := http.DefaultClient.Do(prepare_request(query, "TSV", extra_settings))
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

	tsv_reader := csv.NewReader(response.Body)
	tsv_reader.Comma = '\t'
	data, err = tsv_reader.ReadAll()
	return
}

func service_request(query string) (data [][]string, err error) {
	extra_settings := map[string]string{"log_queries": "0"}
	return service_request_with_extra_setting(query, extra_settings)
}

func kill_query(query_id string) bool {
	query := fmt.Sprintf("SELECT 'query_id %s killed by replace'", query_id)
	extra_settings := map[string]string{"log_queries": "0", "replace_running_query": "1", "query_id": query_id}

	_, err := service_request_with_extra_setting(query, extra_settings)
	if err != nil {
		return false
	}

	return true
}
