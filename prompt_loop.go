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

var prompt string = ":) "
var promptNextLines string = ":-] "

var exit_regexp *regexp.Regexp = regexp.MustCompile("^(?i)\\s*((exit|quit|logout)\\s*;?|(учше|йгше|дщпщге)\\s*ж?|q|й|:q|Жй)\\s*$")
var cmd_finish *regexp.Regexp = regexp.MustCompile("(^.*)(;|\\\\\\S)\\s*$")
var help_cmd *regexp.Regexp = regexp.MustCompile("^(?i)\\s*(help|\\?)\\s*;?\\s*$")
var use_cmd *regexp.Regexp = regexp.MustCompile("^(?i)\\s*(use)\\s*(\"\\w+\"|\\w+|`\\w+`)\\s*;?\\s*$")
var history_fn string = filepath.Join(homedir(), ".clickhouse_history")

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

func prompt_loop() {
	liner_ctrl := liner.NewLiner()
	defer liner_ctrl.Close()

	init_autocomplete()

	liner_ctrl.SetMultiLineMode(true)
	liner_ctrl.SetCtrlCAborts(true)
	liner_ctrl.SetCompleter(clickhouse_comleter)

	if f, err := os.Open(history_fn); err == nil {
		liner_ctrl.ReadHistory(f)
		f.Close()
	}

	var cmds []string

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	//	set_pager("more")

	current_prompt := prompt
prompt_loop:
	for {
		line, err := liner_ctrl.Prompt(current_prompt)
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if exit_regexp.MatchString(line) {
			break
		}

		if help_cmd.MatchString(line) {
			print_help()
			continue prompt_loop
		}

		matches := use_cmd.FindStringSubmatch(line)

		sql_to_exequte := ""
		new_db_name := ""

		if matches != nil {
			new_db_name = strings.Trim(matches[2], "\"`")
			sql_to_exequte = line
		}

		if len(sql_to_exequte) > 0 {
			cmds = append(cmds, line)
		} else {
			matches := cmd_finish.FindStringSubmatch(line)

			if matches == nil {
				cmds = append(cmds, line)
				current_prompt = promptNextLines
				continue
			} else {
				cmds = append(cmds, matches[1])
				suffix_cmd := matches[2]
				switch suffix_cmd {
				case "\\q":
					break prompt_loop
				case "\\Q":
					break prompt_loop
				case "\\й":
					break prompt_loop
				case "\\Й":
					break prompt_loop
				case "\\?":
					print_help()
					continue prompt_loop
				case "\\h":
					print_help()
					continue prompt_loop
				case "\\#":
					init_autocomplete()
					println("autocomplete keywords reloaded")
					continue prompt_loop
				case "\\c":
					sql_to_exequte = ""
				case ";":
					sql_to_exequte = strings.Join(cmds, " ")
				case "\\g":
					sql_to_exequte = strings.Join(cmds, " ")
				case "\\G":
					sql_to_exequte = strings.Join(cmds, " ") + " FORMAT Vertical"
				case "\\s":
					sql_to_exequte = `SELECT * FROM (
													SELECT name, value FROM system.build_options WHERE name in ('VERSION_FULL', 'VERSION_DESCRIBE', 'SYSTEM')
													UNION ALL
													SELECT 'currentDatabase', currentDatabase()
													UNION ALL
													SELECT 'timezone', timezone()
													UNION ALL
													SELECT 'uptime', toString(uptime())) ORDER BY name`
				case "\\l":
					sql_to_exequte = "SHOW DATABASES"
				case "\\d":
					sql_to_exequte = "SHOW TABLES"
				case "\\p":
					sql_to_exequte = "SELECT query_id, user, address, elapsed, read_rows, memory_usage FROM system.processes"
				default:
					println("Ignoring unknown command " + suffix_cmd)
					continue prompt_loop
				}
				cmds[len(cmds)-1] += suffix_cmd
			}
		}

		sql := strings.Join(cmds, " ")
		cmds = cmds[:0]

		current_prompt = prompt

		liner_ctrl.AppendHistory(sql)

		if f, err := os.Create(history_fn); err != nil {
			log.Print("Error writing history file: ", err)
		} else {
			liner_ctrl.WriteHistory(f)
			f.Close()
		}

		if len(sql_to_exequte) > 0 {
			//
			cx, cancel := context.WithCancel(context.Background())
			query_finished := make(chan bool)
			go func() {
				for {
					select {
					case <-signalCh:
						cancel()
					case <-query_finished:
						return
					}
				}
			}()

			setup_output()
			res := query_to_stdout(sql_to_exequte, stdOut, stdErr, cx)
			release_output()
			if len(new_db_name) > 0 && res == 200 {
				opts.Database = new_db_name
			}
			query_finished <- true
			if err != nil {
				log.Fatal("Unable to start PAGER: ", err)
			}
		}

		//query_to_stdout2(cmd, stdOut, stdErr)
	}

}

func print_help() {
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
