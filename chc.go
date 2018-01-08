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

// TODO:
// promptForPassword, password from env
// configs?
// multiline / multiquery mode
// settings from command line
// native protocol support

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jessevdk/go-flags"
	"github.com/mattn/go-isatty"
)

const (
	formatTabSeparated = "TabSeparated"
	formatVertical     = "Vertical"
)

var opts struct {
	Help       bool   `long:"help"                                      description:"produce help message"`
	Host       string `long:"host"       short:"h"  default:"localhost" description:"server host"`
	Port       uint   `long:"port"                  default:"8123"      description:"server port"`
	Protocol   string `long:"protocol"              default:"http"      description:"protocol (http or https are supported)"`
	User       string `long:"user"       short:"u"  default:"default"   description:"user"`
	Password   string `long:"password"                                  description:"password"`
	Query      string `long:"query"      short:"q"                      description:"query"`
	Database   string `long:"database"   short:"d"  default:"default"   description:"database"`
	Pager      string `long:"pager"                                     description:"pager"`
	Multiline  bool   `long:"multiline"  short:"m"                      description:"multiline"`
	Multiquery bool   `long:"multiquery" short:"n"                      description:"multiquery"`
	Format     string `long:"format"     short:"f"                      description:"default output format"`
	Vertical   bool   `long:"vertical"   short:"E"                      description:"vertical output format, same as\n--format=Vertical or FORMAT Vertical or\n\\G at end of command"`
	Time       bool   `long:"time"       short:"t"                      description:"print query execution time to stderr in\nnon-interactive mode (for benchmarks)"`
	Stacktrace bool   `long:"stacktrace"                                description:"print stack traces of exceptions"`
	Progress   bool   `long:"progress"                                  description:"print progress even in non-interactive\nmode"`
	Version    bool   `long:"version"    short:"V"                      description:"print version information and exit"`
	Echo       bool   `long:"echo"                                      description:"in batch mode, print query before execution"`
}

var clickhouseSetting = make(map[string]string)

const versionString = "v0.1.5"

func parseArgs() {
	argsParser := flags.NewNamedParser("chc (ClickHouse CLI portable)", flags.Default&^flags.HelpFlag) // , HelpFlag
	argsParser.ShortDescription = "Unofficial portable ClickHouse CLI"
	argsParser.LongDescription = "works with ClickHouse from MacOS/Windows/Linux without extra dependencies"
	argsParser.AddGroup("Main Options", "Main Options", &opts)
	args, err := argsParser.Parse()

	if err != nil {
		panic(err)
	}

	if opts.Help {
		argsParser.WriteHelp(os.Stdout)
		os.Exit(1)
	}

	if opts.Version {
		println("chc " + versionString)
		os.Exit(1)
	}

	if opts.Vertical && len(opts.Format) == 0 {
		opts.Format = formatVertical
	}

	// if !opts.Multiline {
	// 	chcOutput.printServiceMsg("Only multiline mode is currently supported\n")
	// }

	if opts.Multiquery {
		chcOutput.printServiceMsg("Multiquery mode is not supported yet\n")
	}

	switch opts.Protocol {
	case "https":
		if argsParser.FindOptionByLongName("port").IsSetDefault() {
			opts.Port = 8443
		}
	case "http":
	default:
		chcOutput.printServiceMsg("Protocol " + opts.Protocol + " is not supported.\n")
		os.Exit(1)
	}

	if len(args) > 0 {
		chcOutput.printServiceMsg("Following arguments were ignored:" + strings.Join(args, " ") + "\n")
	}
}

/// TODO - process settings

func main() {

	parseArgs()

	if isatty.IsTerminal(os.Stdin.Fd()) && isatty.IsTerminal(os.Stdout.Fd()) && len(opts.Query) == 0 {
		opts.Progress = true
		opts.Time = true
		fmt.Printf("chc (ClickHouse CLI portable) %s\n", versionString)
		fmt.Printf("Connecting to database %s at %s as user %s.\n", opts.Database, getHost(), opts.User)

		serverVersion, err := getServerVersion()
		if err != nil {
			log.Fatalln(err)
		}

		fmt.Printf("Connected to ClickHouse server version %s.\n\n", serverVersion)

		if len(opts.Pager) > 0 {
			chcOutput.setPager(opts.Pager)
		}

		if opts.Format == "" {
			opts.Format = "PrettyCompact"
		}

		promptLoop()
		fmt.Println("Bye.")
	} else {
		if opts.Format == "" {
			opts.Format = formatTabSeparated
		}

		fireQuery(opts.Query, opts.Format, false)
	}

}
