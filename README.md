[![Go Report Card](https://goreportcard.com/badge/github.com/filimonov/chc)](https://goreportcard.com/report/github.com/filimonov/chc)

# chc
chc: ClickHouse portable command line client. 

Download [release](https://github.com/filimonov/chc/releases) for your platform (Linux, MacOS and Windows).
Or build your own for any of [golang supported platforms](https://golang.org/doc/install/source#environment) just as simple as `go install github.com/filimonov/chc`

## Features:
* Progressbar from native client ported
* Colors / highlights works on Windows 
* Autocompletion for SQL syntax, table names, column names, dictionaries.
* Pager support 
* Sessions support

Currently it works via http interface. Should also work via https (untested).

Should work when readonly = 0 or readonly = 2.

## Known issues

Working via http interface have a certain limitations.

Progress info - there is an option to turn on send_progress_in_http_headers, but in that case it start sending data only then all the data is ready. For command line interface that caching is not always nice (example: when you run "select * from numbers" and hit Ctrl+C after a while - with send_progress_in_http_headers you get nothing in the output). So instead of send_progress_in_http_headers chc sends a lot of requests "SELECT ... FROM system.processes where query_id = ..." in background to get query execution progress. Those small queries will create some extra load, and if you use [quotas](https://clickhouse.yandex/docs/en/operations/quotas.html) for queries count then  that quota can be exceeded because of thouse backgroud selects. 

Currently there are no option to get query stats after execution. That data can be extracted from query_log but it will make a delay (by default up to 7.5 seconds). So for now only some client-estimated stats are printed after request.

