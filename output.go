package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/mattn/go-colorable" // make colors work on windows
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
	prevMode   uint

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
	output.outputMode = omPager
	parts := strings.Split(cmd, " ")
	output.pagerExecutable, output.pagerParams = parts[0], parts[1:]
}

func (output *outputStruct) setOutfile(filename string) {
	output.prevMode = output.outputMode
	output.fileName = filename
	output.outputMode = omFile
}

func (output *outputStruct) resetOutfile() {
	output.outputMode = output.prevMode
	output.fileName = ""
}

func (output *outputStruct) reset() {
	output.outputMode = omStd
	output.StdOut = output.colorableStdOut
	output.pagerExecutable = ""
	output.fileName = ""
	output.pagerParams = []string{}
}

func (output *outputStruct) startOutput() {
	switch output.outputMode {
	case omStd:
		output.StdOut = output.colorableStdOut
	case omPager:
		output.StdOut = output.pagerWriter
	case omFile:
		output.StdOut = output.fileBufferedWriter
	}
}

func (output *outputStruct) setupOutput(cancel context.CancelFunc) bool {
	switch output.outputMode {
	case omStd:
	case omPager:
		cmd := exec.Command(output.pagerExecutable, output.pagerParams...)
		pagerWriter, err := cmd.StdinPipe()
		if err != nil {
			output.printServiceMsg(fmt.Sprintf("Can't make stdin pipe: %s\n", err))
			return false
		}

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		err = cmd.Start()
		if err != nil {
			output.printServiceMsg(fmt.Sprintf("PAGER error: %s\n", err))
			return false
		}
		output.pagerWriter = pagerWriter
		output.waitingPager = make(chan struct{})
		go func() {
			defer close(output.waitingPager)
			defer cancel()
			cmd.Wait()
		}()
	case omFile:
		filehandle, err := os.OpenFile(output.fileName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0755)
		if err != nil {
			output.printServiceMsg(fmt.Sprintf("Unable to %s\n", err))
			output.resetOutfile()
			return false
		}
		output.fileHandle = filehandle
		output.fileBufferedWriter = bufio.NewWriter(filehandle)
	}
	return true
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
		output.resetOutfile()
	}
}
