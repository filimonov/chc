package main

import (
	"context"
	"github.com/davecgh/go-spew/spew"
	"github.com/peterh/liner" // there is also github.com/chzyer/readline and https://github.com/Bowery/prompt
	//"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
)

var prompt = ":) "
var promptNextLines = ":-] "
var historyFn = filepath.Join(homedir(), ".clickhouse_history")

func promptLoop() {
	linerCtrl := liner.NewLiner()
	defer linerCtrl.Close()

	initAutocomlete()

	linerCtrl.SetMultiLineMode(true)
	linerCtrl.SetCtrlCAborts(true)
	linerCtrl.SetCompleter(clickhouseComleter)

	readHistoryFromFile(linerCtrl, historyFn)

	var cmds []string

	currentPrompt := prompt
promptLoop:
	for {
		line, err := linerCtrl.Prompt(currentPrompt)
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)

		resStatus := executeOrContinue(cmds, line)

		switch resStatus {
		case resExecuted:
			cmds = append(cmds, line)
			sql := strings.Join(cmds, " ")
			cmds = cmds[:0]
			currentPrompt = prompt
			writeUpdatedHistory(linerCtrl, historyFn, sql)
		case resSkipAndContinue:
			continue promptLoop
		case resContinuePrompting:
			cmds = append(cmds, line)
			currentPrompt = promptNextLines
			continue promptLoop
		case resBreak:
			break promptLoop
		}
	}

}

const ( // iota is reset to 0
	resExecuted          = iota
	resSkipAndContinue   = iota
	resContinuePrompting = iota
	resBreak             = iota
)

var exitRegexp = regexp.MustCompile("(?i)(?:^\\s*(?:exit|quit|logout)\\s*;?|^\\s*(?:учше|йгше|дщпщге)\\s*ж?|^\\s*q|^\\s*й|^\\s*:q|^\\s*Жй|\\\\[qй])\\s*$")
var helpRegexp = regexp.MustCompile("(?:(?i)^\\s*help|\\\\[\\?h]|^\\s*\\?)\\s*$")
var pagerRegexp = regexp.MustCompile("(?i)^\\s*pager\\s+(.+)\\s*$")
var nopagerRegexp = regexp.MustCompile("(?i)^\\s*nopager\\s*$")

var formatRegexp = regexp.MustCompile("(?i)FORMAT\\s+(\\w+|\"\\w+\"|`\\w+`)\\s*$")
var intoOutfileRegexp = regexp.MustCompile("(?i)INTO\\s+OUTFILE\\s+'([^']+)'\\s*$")

func executeOrContinue(prevLines []string, line string) int {

	sqlToExequte := strings.Join(prevLines, " ") + " " + line
	format := ""

	// exit
	switch {
	case len(line) == 0:
		return resSkipAndContinue

	case exitRegexp.MatchString(line) || exitRegexp.MatchString(sqlToExequte):
		return resBreak

	case helpRegexp.MatchString(line) || helpRegexp.MatchString(sqlToExequte):
		printHelp()
		return resExecuted

	case pagerRegexp.MatchString(line) || pagerRegexp.MatchString(sqlToExequte):
		matches := pagerRegexp.FindStringSubmatch(line)
		printServiceMsg("Setting pager to: " + matches[1] + "\n") // TODO stderr
		setPager(matches[1])
		return resExecuted

	case nopagerRegexp.MatchString(line) || nopagerRegexp.MatchString(sqlToExequte):
		printServiceMsg("Resetting pager" + "\n")
		setNoPager()
		return resExecuted

	case strings.HasSuffix(line, "\\#"):
		initAutocomlete()
		printServiceMsg("autocomplete keywords reloaded" + "\n")
		return resExecuted

	case strings.HasSuffix(line, "\\c"):
		return resExecuted

	case strings.HasSuffix(line, "\\g"):
		sqlToExequte = strings.TrimSuffix(sqlToExequte, "\\g")

	case strings.HasSuffix(line, ";"):
		sqlToExequte = strings.TrimSuffix(sqlToExequte, ";")

	case strings.HasSuffix(line, "\\G"):
		format = "Vertical"
		sqlToExequte = strings.TrimSuffix(sqlToExequte, "\\G")

	case strings.HasSuffix(line, "\\s"):
		sqlToExequte = `SELECT * FROM (
						SELECT name, value FROM system.build_options WHERE name in ('VERSION_FULL', 'VERSION_DESCRIBE', 'SYSTEM')
						UNION ALL
						SELECT 'currentDatabase', currentDatabase()
						UNION ALL
						SELECT 'hostName', hostName()
						UNION ALL
						SELECT 'timezone', timezone()
						UNION ALL
						SELECT 'uptime', toString(uptime())
					) ORDER BY name`

	case strings.HasSuffix(line, "\\l"):
		sqlToExequte = "SHOW DATABASES"

	case strings.HasSuffix(line, "\\d"):
		sqlToExequte = "SHOW TABLES"

	case strings.HasSuffix(line, "\\p"):
		sqlToExequte = "SELECT query_id, user, address, elapsed, read_rows, memory_usage FROM system.processes"

	default:
		return resContinuePrompting
	}

	sqlToExequte, format = parseFormatAndOutfile(sqlToExequte, format)
	fireQuery(sqlToExequte, format, true)
	return resExecuted
}

func parseFormatAndOutfile(sqlToExequte, format string) (string, string) {
	formatMatch := formatRegexp.FindStringSubmatch(sqlToExequte)

	if formatMatch != nil {
		format = strings.Trim(formatMatch[1], "\"`")
		//println("Format:" + format)
		sqlToExequte = formatRegexp.ReplaceAllString(sqlToExequte, "")
		//println("SQL:" + sqlToExequte)
	}

	intoOutfileMatch := intoOutfileRegexp.FindStringSubmatch(sqlToExequte)
	if intoOutfileMatch != nil {
		setOutfile(strings.Trim(intoOutfileMatch[1], "'"))
		sqlToExequte = intoOutfileRegexp.ReplaceAllString(sqlToExequte, "")

		// for INTO OUTFILE default format is TabSeparated
		if len(format) == 0 {
			format = "TabSeparated"
		}

	}

	if len(format) == 0 {
		format = opts.Format
	}
	return sqlToExequte, format
}

var useCmdRegexp = regexp.MustCompile("^\\s*(?i)use\\s+(\"\\w+\"|\\w+|`\\w+`)\\s*$")

// it will not match SET GLOBAL as set global not affect current session, according to docs
var setCmdRegexp = regexp.MustCompile("^\\s*(?i)set\\s+(?:\"\\w+\"|\\w+|\\`\\w+\\`)\\s*=\\s*(?:'([^']+)'|[0-9]+|NULL)")
var settingsRegexp = regexp.MustCompile("\\s*(\"\\w+\"|\\w+|\\`\\w+\\`)\\s*=\\s*('[^']+'|[0-9]+|NULL)\\s*,?")

func fireQuery(sqlToExequte, format string, interactive bool) {

	signalCh := make(chan os.Signal, 1)

	signal.Notify(signalCh, os.Interrupt)

	cx, cancel := context.WithCancel(context.Background())
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

	setupOutput()
	res := queryToStdout(cx, sqlToExequte, format, interactive)
	releaseOutput()
	if res == 200 {
		useCmdMatches := useCmdRegexp.FindStringSubmatch(sqlToExequte)
		if useCmdMatches != nil {
			opts.Database = strings.Trim(useCmdMatches[1], "\"`")
			printServiceMsg("Database changed to " + opts.Database)
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

// wrapper for ReadHistory. Returns the number of lines read, and any read error (except io.EOF).
func readHistoryFromFile(s *liner.State, historyFn string) (num int, err error) {
	f, fileErr := os.Open(historyFn)
	if fileErr != nil {
		err = fileErr
		return
	}
	defer f.Close()
	return s.ReadHistory(f)
}

func writeUpdatedHistory(s *liner.State, historyFn string, newHistoryLine string) (num int, err error) {
	s.AppendHistory(newHistoryLine)

	f, fileErr := os.Create(historyFn)

	if fileErr != nil {
		err = fileErr
		return
	}
	defer f.Close()
	return s.WriteHistory(f)
}

func printHelp() {
	printServiceMsg(`
Hotkeys:
Ctrl-A, Home      Move cursor to beginning of line
Ctrl-E, End       Move cursor to end of line
Ctrl-B, Left      Move cursor one character left
Ctrl-F, Right     Move cursor one character right
Ctrl-Left, Alt-B  Move cursor to previous word
Ctrl-Right, Alt-F Move cursor to next word
Ctrl-D, Del       Delete character under cursor (if line is not empty)
Ctrl-D            End of File - usually quits application (if line is empty)
Ctrl-L            Clear screen (line is unmodified)
Ctrl-T            Transpose previous character with current character
Ctrl-H, BackSpace Delete character before cursor
Ctrl-W            Delete word leading up to cursor
Ctrl-K            Delete from cursor to end of line
Ctrl-U            Delete from start of line to cursor
Ctrl-P, Up        Previous match from history
Ctrl-N, Down      Next match from history
Ctrl-R            Reverse Search history (Ctrl-S forward, Ctrl-G cancel)
Tab               Next completion
Shift-Tab         (after Tab) Previous completion


Following commands are supported (can be changed in further versions).
?    - help
help - help
exit - exit (also understands "quit", "logout", "q")
pager - set pager (for example "pager less -S -R")
nopager - disable pager


Mysql/psql-alike commands
\? - help
\h - help
\# - rebuild autocomplete
\g - execute command (same as semicolon)
\G - execute in Vertical mode
\c - clear statement
\s - status
\l - list databases
\d - show tables
\p - processlist
\q - quit
`)

}

func homedir() string {
	home := os.Getenv("HOME") // *nix style

	if home != "" {
		return home
	}

	home = os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH") // Windows style

	if home != "" {
		return home
	}

	home = os.Getenv("USERPROFILE") // Windows option 2

	if home != "" {
		return home
	}

	return os.Getenv("TEMP") // may be at least temp exists?
}
