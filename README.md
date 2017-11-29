[![Go Report Card](https://goreportcard.com/badge/github.com/filimonov/chc)](https://goreportcard.com/report/github.com/filimonov/chc)

# chc
chc: ClickHouse portable command line client.

Just works on Windows / MacOS / Linux / other [golang supported platforms](https://golang.org/doc/install/source#environment).

## Install 
1. Download latest [release](https://github.com/filimonov/chc/releases) for your platform.
1. Unpack
1. Try `chc --help`

## Build
1. Install go 1.8 (or newer): https://golang.org/doc/install 
1. `go get -u github.com/filimonov/chc`
1. cd $GOPATH/src/github.com/filimonov/chc && go build

## Features:
* Progressbar from native client ported
* Colors / highlights works on Windows 
* Autocompletion for SQL syntax, table names, column names, dictionaries.
* Pager support 
* Sessions support
* Reacts on Ctrl+C without delays (native client sometimes have problems with that)

Currently it works via http interface. Should also work via https (untested).

Should work when readonly = 0 or readonly = 2.

## Known issues and limitations

Working via http interface have a certain limitations.

*Progress info*: HTTP protocol does not have any standart support to send execution progress. Since v1.1.54159 ClickHouse support  an option called send_progress_in_http_headers, but in that case server caches the responce, and start sending data only then all the data is ready. For command line interface that caching is not always nice (when you run a query and hit Ctrl+C after a while with send_progress_in_http_headers you get nothing in the output). So instead of send_progress_in_http_headers chc sends a lot of requests "SELECT ... FROM system.processes where query_id = ..." in background to get query execution progress. Those small queries will create some extra load, and if you use [quotas](https://clickhouse.yandex/docs/en/operations/quotas.html) for queries count then that quota can be exceeded because of thouse backgroud selects. 

*Query statistics*: Currently there are no option to get query stats after execution. That data can be extracted from query_log but it will make a delay (by default up to 7.5 seconds). So for now only some client-estimated stats are printed after request.

*Echo of formatted and parsed query*: Currently there is no any option to get formatted query from the server, the only option is to parse it on the client. 
