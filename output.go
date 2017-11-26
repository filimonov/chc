package main

import (
	"bufio"
	"fmt"
	"github.com/mattn/go-colorable" // make colors work on windows
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
)

// TODO: errors

const ( // iota is reset to 0
	omStd   = iota
	omPager = iota
	omFile  = iota
)

// do we need to make it thread-safe?
type outputStruct struct {
	outputMode uint

	StdOut io.Writer
	StdErr io.Writer

	colorableStdOut io.Writer // caching

	pagerWriter     io.WriteCloser
	pagerExecutable string
	pagerParams     []string
	waitingPager    chan struct{}

	fileHandle         *os.File
	fileBufferedWriter *bufio.Writer
	fileName           string
}

var chcOutput = newOutput()

func newOutput() outputStruct {
	output := outputStruct{
		outputMode: omStd,
		// NewColarableXXX if will return usual os.Stdout if not windows or if not terminal
		colorableStdOut: colorable.NewColorableStdout(),
		StdErr:          colorable.NewColorableStderr(),
	}
	output.StdOut = output.colorableStdOut
	return output
}

func (output *outputStruct) printServiceMsg(str string) {
	fmt.Fprint(output.StdErr, str)
}

func (output *outputStruct) setPager(cmd string) {
	parts := strings.Split(cmd, " ")
	output.outputMode = omPager
	output.pagerExecutable, output.pagerParams = parts[0], parts[1:]
}

func (output *outputStruct) setOutfile(filename string) {
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		output.fileHandle, err = os.Create(filename)
		if err == nil {
			output.fileName = filename
			output.outputMode = omFile
			output.fileBufferedWriter = bufio.NewWriter(output.fileHandle)
		}

	}

}

func (output *outputStruct) reset() {
	output.outputMode = omStd
	output.StdOut = output.colorableStdOut
	output.pagerExecutable = ""
	output.fileName = ""
	output.pagerParams = []string{}
}

func (output *outputStruct) setupOutput() {
	switch output.outputMode {
	case omStd:
		output.StdOut = output.colorableStdOut
	case omPager:
		cmd := exec.Command(output.pagerExecutable, output.pagerParams...)
		output.pagerWriter, _ = cmd.StdinPipe()
		output.StdOut = output.pagerWriter

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		// Create a blocking chan, Run the pager and unblock once it is finished
		output.waitingPager = make(chan struct{})

		go func() {
			defer close(output.waitingPager)
			err := cmd.Run()
			if err != nil {
				log.Fatal("Unable to start PAGER: ", err)
				output.reset()
			}
		}()
	case omFile:
		output.StdOut = output.fileBufferedWriter
	}
}

func (output *outputStruct) releaseOutput() {
	switch output.outputMode {
	case omStd:
	case omPager:

		// Close stdin (result in pager to exit)
		output.pagerWriter.Close()

		// Wait for the pager to be finished
		<-output.waitingPager

	case omFile:
		output.fileBufferedWriter.Flush()
		output.fileHandle.Close()
		output.reset()
	}
}
