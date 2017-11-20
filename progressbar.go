package main

import (
	"fmt"
	"github.com/chzyer/readline" // only console width used from there...
	"io"
	"math"
	"strings"
)

/// http://en.wikipedia.org/wiki/ANSI_escape_code

/// Similar codes \e[s, \e[u don't work in VT100 and Mosh.
const saveCursorPosition string = "\033" + "7"
const restoreCursorPosition string = "\033" + "8"
const clearToEndOfLine string = "\033[K"

/// This codes are possibly not supported everywhere.
const disableLineWrapping string = "\033[?7l"
const enableLineWrapping string = "\033[?7h"

// const disableLineWrapping string = ""
// const enableLineWrapping string = ""

// var indicators = [...]string{"\033[1;30m→\033[0m", "\033[1;31m↘\033[0m", "\033[1;32m↓\033[0m", "\033[1;33m↙\033[0m", "\033[1;34m←\033[0m", "\033[1;35m↖\033[0m", "\033[1;36m↑\033[0m", "\033[1m↗\033[0m"}
var indicators = [...]string{"\033[1;30m→\033[0m", "\033[1;31m\\\033[0m", "\033[1;32m↓\033[0m", "\033[1;33m/\033[0m", "\033[1;34m←\033[0m", "\033[1;35m\\\033[0m", "\033[1;36m↑\033[0m", "\033[1m/\033[0m"}

var quantityUnits = [...]string{"", " thousand", " million", " billion", " trillion", " quadrillion"}
var sizeUnitsDecimal = [...]string{" B", " KB", " MB", " GB", " TB", " PB", " EB", " ZB", " YB"}

// formatReadableQuantity
// formatReadableSizeWithDecimalSuffix
func formatReadableQuantity(value float64) string {
	i := 0
	for ; value >= 1000 && i <= 5; i++ {
		value /= 1000
	}
	return fmt.Sprintf("%.2f%s", value, quantityUnits[i])
}

func formatReadableSizeWithDecimalSuffix(value float64) string {
	i := 0
	for ; value >= 1000 && i <= 8; i++ {
		value /= 1000
	}
	return fmt.Sprintf("%.2f%s", value, sizeUnitsDecimal[i])
}

var increment int
var writtenProgressChars int
var showProgressBar = false

func initProgress() {
	increment = 0
	writtenProgressChars = 0
	showProgressBar = false
}

func clearProgress(w io.Writer) {
	if writtenProgressChars > 0 {
		w.Write([]byte(restoreCursorPosition + clearToEndOfLine))
		writtenProgressChars = 0
	}
}

func writeProgres(w io.Writer, progressRows, progressBytes, totalRows, elapsedNanoseconds uint64) {
	clearProgress(w)
	// println(progressRows, progressBytes, totalRows, elapsedNanoseconds)
	timing := ". "
	if elapsedNanoseconds != 0 {
		timing = fmt.Sprintf(" (%s rows/s., %s/s.) ", formatReadableQuantity(float64(progressRows)*float64(1000000000.0)/float64(elapsedNanoseconds)), formatReadableSizeWithDecimalSuffix(float64(progressBytes)*float64(1000000000.0)/float64(elapsedNanoseconds)))
	}

	str := fmt.Sprintf(" Progress: %s rows, %s%s", formatReadableQuantity(float64(progressRows)), formatReadableSizeWithDecimalSuffix(float64(progressBytes)), timing)

	writtenProgressChars = len(str) + 1

	progressBarStr := ""

	/// If the approximate number of rows to process is known, we can display a progress bar and percentage.
	if totalRows > 0 {
		if progressRows > totalRows {
			totalRows = progressRows
		}

		widthOfProgressBar := readline.GetScreenWidth() - writtenProgressChars - len(" 99%")

		/// To avoid flicker, display progress bar only if .5 seconds have passed since query execution start
		///  and the query is less than halfway done.
		if elapsedNanoseconds > 500000000 {
			/// Trigger to start displaying progress bar. If query is mostly done, don't display it.
			if progressRows*2 < totalRows {
				showProgressBar = true
			}
			if showProgressBar {
				if widthOfProgressBar > 0 {
					charsWidth := float64(widthOfProgressBar) * float64(progressRows) / float64(totalRows)
					fullBarsWidth := int(math.Floor(charsWidth))
					progressBarStr = "\033[0;32m" + strings.Repeat("█", fullBarsWidth) + strings.Repeat(" ", widthOfProgressBar-fullBarsWidth) + "\033[0m"
					//fullBarsWidth := int(math.Floor(charsWidth))
					//half_bar_width := int(math.Floor((charsWidth - float64(fullBarsWidth)) * float64(2)))
					/// original client uses nicer unicode chars which are not exists by default in windows
					// var bar_chars =
					// U+2588 	█ 	Full block
					// U+2589 	▉ 	Left seven eighths block
					// U+258A 	▊ 	Left three quarters block
					// U+258B 	▋ 	Left five eighths block
					// U+258C 	▌ 	Left half block
					// U+258D 	▍ 	Left three eighths block
					// U+258E 	▎ 	Left one quarter block
					// U+258F 	▏ 	Left one eighth block

					//progressBarStr = "\033[0;32m" + strings.Repeat("█", fullBarsWidth) + strings.Repeat("▌", half_bar_width) + strings.Repeat(" ", widthOfProgressBar-fullBarsWidth-half_bar_width) + "\033[0m"
				}
			}
			/// Underestimate percentage a bit to avoid displaying 100%.
			progressBarStr += fmt.Sprintf(" %2.0f%%", float64(progressRows)*float64(99)/float64(totalRows))
		}
	}

	w.Write([]byte(saveCursorPosition + indicators[increment%8] + str + progressBarStr))

	increment++
}
