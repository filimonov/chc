package main

import (
	"fmt"
	"github.com/mattn/go-colorable" // make colors work on windows
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
)

var colorableStdout = colorable.NewColorableStdout() // os.Stdout
var colorableStderr = colorable.NewColorableStderr() // os.Stderr

var stdOut = colorableStdout
var stdErr = colorableStderr

var pagerWriter io.WriteCloser

var pagerExecutable string
var pagerParams []string

var waitingPager chan struct{}

func printServiceMsg(str string) {
	fmt.Fprint(stdErr, str)
}

func setPager(cmd string) {
	parts := strings.Split(cmd, " ")
	pagerExecutable, pagerParams = parts[0], parts[1:]
}

func setOutfile(filename string) {
	println("TODO - setOutfile:" + filename)
}

func setNoPager() {
	pagerExecutable = ""
	pagerParams = []string{}
}

func useStdOutput() {
	stdOut = colorableStdout
	stdErr = colorableStderr
}

func setupOutput() {
	if len(pagerExecutable) > 0 {
		//			cmd := exec.Command("less", "-R -S")
		cmd := exec.Command(pagerExecutable, pagerParams...)
		// create a pipe (blocking)
		//r, stdin := io.Pipe()
		// Set your i/o's
		pagerWriter, _ = cmd.StdinPipe()
		stdOut = pagerWriter
		//cmd.Stdin = r
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		// Create a blocking chan, Run the pager and unblock once it is finished
		waitingPager = make(chan struct{})
		go func() {
			defer close(waitingPager)
			err := cmd.Run()
			if err != nil {
				log.Fatal("Unable to start PAGER: ", err)
				useStdOutput()
			}
		}()
	} else {
		useStdOutput()
	}
}

func releaseOutput() {
	if len(pagerExecutable) > 0 {
		// Pass anything to your pipe
		//  fmt.Fprintf(stdin, "hello world\n")

		// Close stdin (result in pager to exit)
		pagerWriter.Close()

		// Wait for the pager to be finished
		<-waitingPager

		// lessCmd := exec.Command("/usr/bin/vim","-")
		// lessIn, _ := lessCmd.StdinPipe()
		// lessCmd.Stdout = os.Stdout
		// err := lessCmd.Start()

		// lessIn.Close();
		// lessCmd.Stdin = os.Stdin

		// lessCmd.Wait();
	}
	useStdOutput()
}
