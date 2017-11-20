package main

import (
	"context"
	"github.com/peterh/liner" // there is also github.com/chzyer/readline and https://github.com/Bowery/prompt
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
)

var prompt = ":) "
var promptNextLines = ":-] "

var exitRegexp = regexp.MustCompile("^(?i)\\s*((exit|quit|logout)\\s*;?|(учше|йгше|дщпщге)\\s*ж?|q|й|:q|Жй)\\s*$")
var cmdFinish = regexp.MustCompile("(^.*)(;|\\\\\\S)\\s*$")
var helpCmd = regexp.MustCompile("^(?i)\\s*(help|\\?)\\s*;?\\s*$")
var useCmd = regexp.MustCompile("^(?i)\\s*(use)\\s*(\"\\w+\"|\\w+|`\\w+`)\\s*;?\\s*$")
var historyFn = filepath.Join(homedir(), ".clickhouse_history")

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

func promptLoop() {
	linerCtrl := liner.NewLiner()
	defer linerCtrl.Close()

	initAutocomlete()

	linerCtrl.SetMultiLineMode(true)
	linerCtrl.SetCtrlCAborts(true)
	linerCtrl.SetCompleter(clickhouseComleter)

	if f, err := os.Open(historyFn); err == nil {
		linerCtrl.ReadHistory(f)
		f.Close()
	}

	var cmds []string

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	//	setPager("more")

	currentPrompt := prompt
promptLoop:
	for {
		line, err := linerCtrl.Prompt(currentPrompt)
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if exitRegexp.MatchString(line) {
			break
		}

		if helpCmd.MatchString(line) {
			printHelp()
			continue promptLoop
		}

		matches := useCmd.FindStringSubmatch(line)

		sqlToExequte := ""
		newDbName := ""

		if matches != nil {
			newDbName = strings.Trim(matches[2], "\"`")
			sqlToExequte = line
		}

		if len(sqlToExequte) > 0 {
			cmds = append(cmds, line)
		} else {
			matches := cmdFinish.FindStringSubmatch(line)

			if matches == nil {
				cmds = append(cmds, line)
				currentPrompt = promptNextLines
				continue
			} else {
				cmds = append(cmds, matches[1])
				suffixCmd := matches[2]
				switch suffixCmd {
				case "\\q":
					break promptLoop
				case "\\Q":
					break promptLoop
				case "\\й":
					break promptLoop
				case "\\Й":
					break promptLoop
				case "\\?":
					printHelp()
					continue promptLoop
				case "\\h":
					printHelp()
					continue promptLoop
				case "\\#":
					initAutocomlete()
					println("autocomplete keywords reloaded")
					continue promptLoop
				case "\\c":
					sqlToExequte = ""
				case ";":
					sqlToExequte = strings.Join(cmds, " ")
				case "\\g":
					sqlToExequte = strings.Join(cmds, " ")
				case "\\G":
					sqlToExequte = strings.Join(cmds, " ") + " FORMAT Vertical"
				case "\\s":
					sqlToExequte = `SELECT * FROM (
													SELECT name, value FROM system.build_options WHERE name in ('VERSION_FULL', 'VERSION_DESCRIBE', 'SYSTEM')
													UNION ALL
													SELECT 'currentDatabase', currentDatabase()
													UNION ALL
													SELECT 'timezone', timezone()
													UNION ALL
													SELECT 'uptime', toString(uptime())) ORDER BY name`
				case "\\l":
					sqlToExequte = "SHOW DATABASES"
				case "\\d":
					sqlToExequte = "SHOW TABLES"
				case "\\p":
					sqlToExequte = "SELECT queryID, user, address, elapsed, read_rows, memory_usage FROM system.processes"
				default:
					println("Ignoring unknown command " + suffixCmd)
					continue promptLoop
				}
				cmds[len(cmds)-1] += suffixCmd
			}
		}

		sql := strings.Join(cmds, " ")
		cmds = cmds[:0]

		currentPrompt = prompt

		linerCtrl.AppendHistory(sql)

		if f, err := os.Create(historyFn); err != nil {
			log.Print("Error writing history file: ", err)
		} else {
			linerCtrl.WriteHistory(f)
			f.Close()
		}

		if len(sqlToExequte) > 0 {
			//
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
			res := queryToStdout(cx, sqlToExequte, stdOut, stdErr)
			releaseOutput()
			if len(newDbName) > 0 && res == 200 {
				opts.Database = newDbName
			}
			queryFinished <- true
			if err != nil {
				log.Fatal("Unable to start PAGER: ", err)
			}
		}

		//queryToStdout2(cmd, stdOut, stdErr)
	}

}

func printHelp() {
	println(`
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
USE  - change database


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
