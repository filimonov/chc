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
const SAVE_CURSOR_POSITION string = "\033" + "7"
const RESTORE_CURSOR_POSITION string = "\033" + "8"
const CLEAR_TO_END_OF_LINE string = "\033[K"

/// This codes are possibly not supported everywhere.
const DISABLE_LINE_WRAPPING string = "\033[?7l"
const ENABLE_LINE_WRAPPING string = "\033[?7h"

// const DISABLE_LINE_WRAPPING string = ""
// const ENABLE_LINE_WRAPPING string = ""

// var indicators = [...]string{"\033[1;30m→\033[0m", "\033[1;31m↘\033[0m", "\033[1;32m↓\033[0m", "\033[1;33m↙\033[0m", "\033[1;34m←\033[0m", "\033[1;35m↖\033[0m", "\033[1;36m↑\033[0m", "\033[1m↗\033[0m"}
var indicators = [...]string{"\033[1;30m→\033[0m", "\033[1;31m\\\033[0m", "\033[1;32m↓\033[0m", "\033[1;33m/\033[0m", "\033[1;34m←\033[0m", "\033[1;35m\\\033[0m", "\033[1;36m↑\033[0m", "\033[1m/\033[0m"}

var quantity_units = [...]string{"", " thousand", " million", " billion", " trillion", " quadrillion"}
var size_units_decimal = [...]string{" B", " KB", " MB", " GB", " TB", " PB", " EB", " ZB", " YB"}

// formatReadableQuantity
// formatReadableSizeWithDecimalSuffix
func formatReadableQuantity(value float64) string {
	i := 0
	for ; value >= 1000 && i <= 5; i++ {
		value /= 1000
	}
	return fmt.Sprintf("%.2f%s", value, quantity_units[i])
}

func formatReadableSizeWithDecimalSuffix(value float64) string {
	i := 0
	for ; value >= 1000 && i <= 8; i++ {
		value /= 1000
	}
	return fmt.Sprintf("%.2f%s", value, size_units_decimal[i])
}

var increment int = 0
var written_progress_chars int = 0
var show_progress_bar bool = false

func initProgress() {
	increment = 0
	written_progress_chars = 0
	show_progress_bar = false
}

func clearProgress(w io.Writer) {
	if written_progress_chars > 0 {
		w.Write([]byte(RESTORE_CURSOR_POSITION + CLEAR_TO_END_OF_LINE))
		written_progress_chars = 0
	}
}

func writeProgres(w io.Writer, progress_rows, progress_bytes, total_rows, elapsed_ns uint64) {
	clearProgress(w)
	// println(progress_rows, progress_bytes, total_rows, elapsed_ns)
	timing := ". "
	if elapsed_ns != 0 {
		timing = fmt.Sprintf(" (%s rows/s., %s/s.) ", formatReadableQuantity(float64(progress_rows)*float64(1000000000.0)/float64(elapsed_ns)), formatReadableSizeWithDecimalSuffix(float64(progress_bytes)*float64(1000000000.0)/float64(elapsed_ns)))
	}

	str := fmt.Sprintf(" Progress: %s rows, %s%s", formatReadableQuantity(float64(progress_rows)), formatReadableSizeWithDecimalSuffix(float64(progress_bytes)), timing)

	written_progress_chars = len(str) + 1

	progress_bar_str := ""

	/// If the approximate number of rows to process is known, we can display a progress bar and percentage.
	if total_rows > 0 {
		if progress_rows > total_rows {
			total_rows = progress_rows
		}

		width_of_progress_bar := readline.GetScreenWidth() - written_progress_chars - len(" 99%")

		/// To avoid flicker, display progress bar only if .5 seconds have passed since query execution start
		///  and the query is less than halfway done.
		if elapsed_ns > 500000000 {
			/// Trigger to start displaying progress bar. If query is mostly done, don't display it.
			if progress_rows*2 < total_rows {
				show_progress_bar = true
			}
			if show_progress_bar {
				if width_of_progress_bar > 0 {
					chars_width := float64(width_of_progress_bar) * float64(progress_rows) / float64(total_rows)
					full_bars_width := int(math.Floor(chars_width))
					progress_bar_str = "\033[0;32m" + strings.Repeat("█", full_bars_width) + strings.Repeat(" ", width_of_progress_bar-full_bars_width) + "\033[0m"
					//full_bars_width := int(math.Floor(chars_width))
					//half_bar_width := int(math.Floor((chars_width - float64(full_bars_width)) * float64(2)))
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

					//progress_bar_str = "\033[0;32m" + strings.Repeat("█", full_bars_width) + strings.Repeat("▌", half_bar_width) + strings.Repeat(" ", width_of_progress_bar-full_bars_width-half_bar_width) + "\033[0m"
				}
			}
			/// Underestimate percentage a bit to avoid displaying 100%.
			progress_bar_str += fmt.Sprintf(" %2.0f%%", float64(progress_rows)*float64(99)/float64(total_rows))
		}
	}

	w.Write([]byte(SAVE_CURSOR_POSITION + indicators[increment%8] + str + progress_bar_str))

	increment++
}
