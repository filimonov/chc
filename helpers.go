package main

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

func readTabSeparated(rd io.Reader) ([][]string, error) {
	res := [][]string{}
	bufferedReader := bufio.NewReader(rd)
	for {
		line, err := bufferedReader.ReadString('\n')
		line = strings.TrimRight(line, "\n")
		switch err {
		case nil:
			fields := strings.Split(line, "\t")
			for idx, v := range fields {
				// \b, \f, \r, \n, \t, \0, \', and \\
				v = strings.Replace(v, "\\b", "\b", -1)
				v = strings.Replace(v, "\\f", "\f", -1)
				v = strings.Replace(v, "\\r", "\r", -1)
				v = strings.Replace(v, "\\n", "\n", -1)
				v = strings.Replace(v, "\\t", "\t", -1)
				v = strings.Replace(v, "\\0", "\x00", -1)
				v = strings.Replace(v, "\\'", "'", -1)
				v = strings.Replace(v, "\\\\", "\\", -1)
				fields[idx] = v
			}
			res = append(res, fields)
		case io.EOF:
			return res, nil
		default:
			return res, err
		}
	}
}

type counerFunc func(string) uint64

func getRowsCounter(format string) counerFunc {
	var counterState int
	var count uint64

	switch format {
	case formatTabSeparated, "TSV", "CSV", "TSKV", "JSONEachRow", "TabSeparatedRaw", "TSVRaw":
		return func(line string) uint64 {
			count++
			return count
		}
	case "TabSeparatedWithNames", "TSVWithNames", "CSVWithNames":
		return func(line string) uint64 {
			if counterState > 0 {
				count++
			} else {
				counterState++
			}
			return count
		}
	case "TabSeparatedWithNamesAndTypes", "TSVWithNamesAndTypes", "PrettySpace", "PrettySpaceNoEscapes":
		return func(line string) uint64 {
			if counterState > 1 {
				count++
			} else {
				counterState++
			}
			return count
		}
	case "BlockTabSeparated":
		return func(line string) uint64 {
			if counterState == 0 {
				count = uint64(strings.Count(line, "\t")) + 1
				counterState = 1
			}
			return count
		}
	case "Pretty", "PrettyCompact", "PrettyCompactMonoBlock", "PrettyNoEscapes", "PrettyCompactNoEscapes":
		return func(line string) uint64 {
			if strings.HasPrefix(line, "│") {
				count++
			}
			return count
		}

	case formatVertical, "VerticalRaw":
		return func(line string) uint64 {
			if strings.HasPrefix(line, "───") {
				count++
			}
			return count
		}
	case "JSON", "JSONCompact":
		return func(line string) uint64 {
			lineTrimmed := strings.TrimSpace(line)
			switch counterState {
			case 0: // waiting for data start
				if lineTrimmed == "\"data\":" {
					counterState = 1
				}
			case 1: // waiting for </data> start
				if lineTrimmed == "]," {
					counterState = 2
				}
			case 2: // waiting for <rows>
				if strings.HasPrefix(lineTrimmed, "\"rows\":") {
					lineTrimmed = strings.TrimPrefix(lineTrimmed, "\"rows\": ")
					lineTrimmed = strings.TrimSuffix(lineTrimmed, ",")
					count, _ = strconv.ParseUint(lineTrimmed, 10, 64)
					counterState = 3
				}
			}
			return count
		}

	case "XML":
		return func(line string) uint64 {
			lineTrimmed := strings.TrimSpace(line)
			switch counterState {
			case 0: // waiting for <data> start
				if strings.HasPrefix(lineTrimmed, "<data>") {
					counterState = 1
				}
			case 1: // waiting for </data> start
				if strings.HasPrefix(lineTrimmed, "</data>") {
					counterState = 2
				}
			case 2: // waiting for <rows>
				if strings.HasPrefix(lineTrimmed, "<rows>") {
					lineTrimmed = strings.TrimPrefix(lineTrimmed, "<rows>")
					lineTrimmed = strings.TrimSuffix(lineTrimmed, "</rows>")
					count, _ = strconv.ParseUint(lineTrimmed, 10, 64)
					counterState = 3
				}
			}
			return count
		}
	default:
		// case "Null","Native","RowBinary","Values","CapnProto","ODBCDriver":
		return func(line string) uint64 {
			return 0
		}
	}
}
