package main

import (
	"github.com/mattn/go-colorable" // make colors work on windows
	"io"
	"os"
	"os/exec"
	"strings"
)

var colorableStdout io.Writer = colorable.NewColorableStdout() // os.Stdout
var colorableStderr io.Writer = colorable.NewColorableStderr() // os.Stderr

var stdOut io.Writer = colorableStdout
var stdErr io.Writer = colorableStderr

var PagerWriter io.WriteCloser

var pager_executable string
var pager_params []string

var waiting_pager chan struct{}

func set_pager(cmd string) {
	parts := strings.Split(cmd, " ")
	pager_executable, pager_params = parts[0], parts[1:]
}

func set_nopager() {
	pager_executable = ""
	pager_params = []string{}
}

func use_std_output() {
	stdOut = colorableStdout
	stdErr = colorableStderr
}

func setup_output() {
	if len(pager_executable) > 0 {
		//			cmd := exec.Command("less", "-R -S")
		cmd := exec.Command(pager_executable, pager_params...)
		// create a pipe (blocking)
		//r, stdin := io.Pipe()
		// Set your i/o's
		PagerWriter, _ = cmd.StdinPipe()
		stdOut = PagerWriter
		//cmd.Stdin = r
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		// Create a blocking chan, Run the pager and unblock once it is finished
		waiting_pager = make(chan struct{})
		go func() {
			defer close(waiting_pager)
			cmd.Run()
		}()
	} else {
		use_std_output()
	}
}

func release_output() {
	if len(pager_executable) > 0 {
		// Pass anything to your pipe
		//  fmt.Fprintf(stdin, "hello world\n")

		// Close stdin (result in pager to exit)
		PagerWriter.Close()

		// Wait for the pager to be finished
		<-waiting_pager

		// lessCmd := exec.Command("/usr/bin/vim","-")
		// lessIn, _ := lessCmd.StdinPipe()
		// lessCmd.Stdout = os.Stdout
		// err := lessCmd.Start()

		// lessIn.Close();
		// lessCmd.Stdin = os.Stdin

		// lessCmd.Wait();
	}
	use_std_output()
}
