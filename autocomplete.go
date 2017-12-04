package main

import (
	"log"
	"regexp"
	"strings"
)

var keywordsAutocomlete []string

func initAutocomlete() {
	keywords := []string{
		"ADD COLUMN",
		"AFTER",
		"ALL",
		"ALTER TABLE",
		"ANY",
		"ARRAY",
		"ARRAY JOIN",
		"AS",
		"ASC",
		"ASYNC",
		"ATTACH PART",
		"ATTACH PARTITION",
		"ATTACH",
		"BY",
		"CASE",
		"CHECK TABLE",
		"CLEAR COLUMN",
		"COLLATE",
		"COORDINATE",
		"COPY",
		"CREATE",
		"CROSS",
		"DATABASE",
		"DATABASES",
		"DEDUPLICATE",
		"DESC",
		"DESCRIBE",
		"DESCRIBE TABLE",
		"DETACH",
		"DETACH PARTITION",
		"DISTINCT",
		"DROP",
		"DROP COLUMN",
		"DROP PARTITION",
		"ELSE",
		"END",
		"ENGINE",
		"EXISTS",
		"EXISTS TABLE",
		"FETCH PARTITION",
		"FINAL",
		"FORMAT",
		"FREEZE PARTITION",
		"FROM",
		"FULL",
		"GROUP BY",
		"GLOBAL",
		"HAVING",
		"IF EXISTS",
		"IF NOT EXISTS",
		"IN PARTITION",
		"INNER",
		"INSERT INTO",
		"INTO",
		"JOIN",
		"KILL QUERY",
		"LEFT",
		"LEFT ARRAY JOIN",
		"LIKE",
		"LIMIT",
		"LOCAL",
		"MATERIALIZED",
		"MODIFY COLUMN",
		"MODIFY PRIMARY KEY",
		"NAME",
		"NOT",
		"ON",
		"OPTIMIZE TABLE",
		"ORDER",
		"ORDER BY",
		"OUTFILE",
		"PARTITION",
		"POPULATE",
		"PREWHERE",
		"RENAME TABLE",
		//		"RESHARD",
		"RIGHT",
		"SELECT",
		"SHOW",
		"SET",
		"SETTINGS",
		"SAMPLE",
		"SHOW CREATE TABLE",
		"SHOW PROCESSLIST",
		"SYNC",
		"TABLE",
		"TABLES",
		"TEMPORARY",
		"TEST",
		"THEN",
		"TO",
		"TOTALS",
		"UNION",
		"UNION ALL",
		"USE",
		"USING",
		"VALUES",
		"VIEW",
		"WHEN",
		"WHERE",
		"WITH",

		"AggregatingMergeTree",
		"Buffer",
		"CollapsingMergeTree",
		"Distributed",
		"File",
		"Join",
		"Kafka",
		"Log",
		"MaterializedView",
		"Memory",
		"Merge",
		"MergeTree",
		"Null",
		"ReplacingMergeTree",
		"ReplicatedAggregatingMergeTree",
		"ReplicatedCollapsingMergeTree",
		"ReplicatedMergeTree",
		"ReplicatedReplacingMergeTree",
		"ReplicatedSummingMergeTree",
		"Set",
		"SummingMergeTree",
		"TinyLog",
		"View",

		// https://github.com/yandex/ClickHouse/blob/master/dbms/src/DataStreams/FormatFactory.cpp
		"BlockTabSeparated",
		"CapnProto",
		"CSV",
		"CSVWithNames",
		"JSON",
		"JSONCompact",
		"JSONEachRow",
		"Native",
		"Null",
		"Null",
		"ODBCDriver",
		"Pretty",
		"PrettyCompact",
		"PrettyCompactMonoBlock",
		"PrettyCompactNoEscapes",
		"PrettyNoEscapes",
		"PrettySpace",
		"PrettySpaceNoEscapes",
		"RowBinary",
		formatTabSeparated,
		"TabSeparatedRaw",
		"TabSeparatedWithNames",
		"TabSeparatedWithNamesAndTypes",
		"TSKV",
		"TSV",
		"TSVRaw",
		"TSVWithNames",
		"TSVWithNamesAndTypes",
		"Values",
		formatVertical,
		"VerticalRaw",
		"XML",

		"Array",
		"Boolean",
		"Date",
		"DateTime",
		"Enum",
		"Expression",
		"FixedString",
		"Float32",
		"Float64",
		"Int8",
		"Int16",
		"Int32",
		"Int64",
		"Nullable",
		"Set",
		"String",
		"Tuple",
		"UInt8",
		"UInt16",
		"UInt32",
		"UInt64",
	}

	query := `
	 SELECT concat('dictGet', t, '(\'', name, '\',\'', n,'\',' ,replaceRegexpAll(key,'([A-Za-z0-9]+)','to\\1(id)'), ')') as n2
	 FROM system.dictionaries ARRAY JOIN attribute.names as n, attribute.types as t
	 UNION ALL
	 SELECT DISTINCT name FROM (
		SELECT name FROM system.functions
		UNION ALL
		SELECT concat(name,'If') FROM system.functions WHERE is_aggregate=1
		UNION ALL
		SELECT concat(name,'Array') FROM system.functions WHERE is_aggregate=1
		UNION ALL
		SELECT concat(name,'Merge') FROM system.functions WHERE is_aggregate=1
		UNION ALL
		SELECT concat(name,'State') FROM system.functions WHERE is_aggregate=1
		UNION ALL
		SELECT concat(name,'MergeState') FROM system.functions WHERE is_aggregate=1
		UNION ALL
		SELECT name FROM system.tables
		UNION ALL
		SELECT name FROM system.columns
		UNION ALL
		SELECT name FROM system.databases
		UNION ALL
		SELECT name FROM system.settings
	) ORDER BY name`

	data, err := serviceRequest(query)
	if err != nil {
		keywordsAutocomlete = keywords
		log.Println(err)
	}

	for _, element := range data {
		keywords = append(keywords, element[0])
	}

	keywordsAutocomlete = keywords
	//	spew.Dump(keywordsAutocomlete)
}

var lastWordRegexp = regexp.MustCompile("(^.*)\\b(\\w+)$")

func clickhouseComleter(line string) (c []string) {
	matches := lastWordRegexp.FindStringSubmatch(line)
	if len(matches) == 3 {
		lastWord := matches[2]
		prefix := matches[1]
		for _, n := range keywordsAutocomlete {
			// possible improvements:
			//  sort keywords by popularity
			//  make it context aware (?)
			//  use more effective search (like binary tree) than simple iterations through all the keywords
			if strings.HasPrefix(strings.ToLower(n), strings.ToLower(lastWord)) {
				c = append(c, prefix+n)
			}
		}
	}
	return
}
