package main

// some inspirations:

// native clickhouse cli
// https://github.com/yandex/ClickHouse/blob/master/dbms/src/Server/Client.cpp

// python unofficial cli
// https://github.com/hatarist/clickhouse-cli

// https://github.com/kshvakov/clickhouse - binary, native protocol

// Other Go-based cli for (other ) databases
// https://github.com/influxdata/influxdb/blob/master/cmd/influx/main.go
// https://github.com/influxdata/influxdb/blob/master/cmd/influx/cli/cli.go
// https://github.com/rqlite/rqlite/blob/master/cmd/rqlite/main.go
// https://github.com/cockroachdb/cockroach/blob/master/pkg/cli/start.go

// TODO:  promptForPassword , password from env
// TTY, configs
// replace regexps with strings.Fields ?

import (
	"fmt"
	"github.com/jessevdk/go-flags"
	"github.com/mattn/go-isatty"
	"os"
	// "io/ioutil"
	"log"
	//"math"
	//	"net/url"
)

var opts struct {
	Help       bool   `long:"help"                                      description:"produce help message"`
	Host       string `long:"host"       short:"h"  default:"localhost" description:"server host"`
	Port       uint   `long:"port"                  default:"8123"      description:"server port"`
	User       string `long:"user"       short:"u"  default:"default"   description:"user"`
	Password   string `long:"password"                                  description:"password"`
	Query      string `long:"query"      short:"q"                      description:"query"`
	Database   string `long:"database"   short:"d"  default:"default"   description:"database"`
	Pager      string `long:"pager"                                     description:"pager"`
	Multiline  bool   `long:"multiline"  short:"m"                      description:"multiline"`
	Multiquery bool   `long:"multiquery" short:"n"                      description:"multiquery"`
	Format     string `long:"format"     short:"f"                      description:"default output format"`
	Vertical   bool   `long:"vertical"   short:"E"                      description:"vertical output format, same as\n--format=Vertical or FORMAT Vertical or\n\\G at end of commanddefault output format"`
	Time       bool   `long:"time"       short:"t"                      description:"print query execution time to stderr in\nnon-interactive mode (for benchmarks)"`
	Stacktrace bool   `long:"stacktrace"                                description:"print stack traces of exceptions"`
	Progress   bool   `long:"progress"                                  description:"print progress even in non-interactive\nmode"`
	Version    bool   `long:"version"    short:"V"                      description:"print version information and exit"`
	Echo       bool   `long:"echo"                                      description:"in batch mode, print query before execution"`
}

const VERSION_STRING = "v0.1.0"

func parse_args() {
	args_parser := flags.NewNamedParser("chc (ClickHouse CLI portable)", flags.Default&^flags.HelpFlag) // , HelpFlag
	args_parser.ShortDescription = "Unofficial portable ClickHouse CLI"
	args_parser.LongDescription = "works with ClickHouse from MacOS/Windows/Linux without extra dependencies"
	args_parser.AddGroup("Main Options", "Main Options", &opts)
	_, err := args_parser.Parse()
	if err != nil {
		panic(err)
	}

	if opts.Help {
		args_parser.WriteHelp(os.Stdout)
		os.Exit(1)
	}

	if opts.Version {
		println("chc " + VERSION_STRING)
		os.Exit(1)
	}

	if opts.Format == "" {
		if isatty.IsTerminal(os.Stdout.Fd()) {
			opts.Format = "PrettyCompact"
		} else {
			opts.Format = "TabSeparated"
		}
	}
	/*
		fmt.Printf("Remaining args: %s\n", strings.Join(args, " "))
	*/
}

/// TODO - process settings

func main() {

	parse_args()

	fmt.Printf("chc (ClickHouse CLI portable) %s\n", VERSION_STRING)
	fmt.Printf("Connecting to database %s at %s as user %s.\n", opts.Database, get_host(), opts.User)

	server_version, err := get_server_version()
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Printf("Connected to ClickHouse server version %s.\n\n", server_version)

	prompt_loop()

}