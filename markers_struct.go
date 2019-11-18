package main

import (
	"unicode/utf8"
)

type Marker struct {
	Date       int64  `parquet:"name=date, type=INT64"`
	SourceHost string `parquet:"name=source_host, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Secure     bool   `parquet:"name=secure, type=BOOLEAN"`
	Source     string `parquet:"name=source, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Link       string `parquet:"name=link, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Fragment   string `parquet:"name=fragment, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Tag        string `parquet:"name=tag, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Extras     string `parquet:"name=extras, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DataOrigin string `parquet:"name=data_origin, type=UTF8, encoding=PLAIN_DICTIONARY"`
}

// Constructs a generic WebGenome Marker
func NewMarker(
	date int64,
	sourceHost string,
	secure bool,
	source string,
	link string,
	fragment string,
	tag string,
	extras string,
	DataOrigin string,
	) Marker {

	return Marker{
		Date:       date,
		SourceHost: sourceHost,
		Secure:     secure,
		Source:     toValidUTF8(source),
		Link:       toValidUTF8(link),
		Fragment:   toValidUTF8(fragment),
		Tag:        tag,
		Extras:     toValidUTF8(extras),
		DataOrigin: DataOrigin,
	}
}

// Constructs a special marker to track the HTTP code obtained when requesting a Web page
func NewWebpageMarker(
	date int64,
	sourceHost string,
	secure bool,
	source string,
	httpCode string,
	extras string,
	dataOrigin string,) Marker {

	return NewMarker(date, sourceHost, secure, source, "", "", httpCode, extras, dataOrigin)
}

func toValidUTF8(text string) string {
	if utf8.ValidString(text) {
		return text
	}
	s := []byte(text)
	b := make([]byte, 0, len(s))
	invalid := false // previous byte was from an invalid UTF-8 sequence
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			i++
			invalid = false
			b = append(b, byte(c))
			continue
		}
		_, wid := utf8.DecodeRune(s[i:])
		if wid == 1 {
			i++
			if !invalid {
				invalid = true
				//bb = append(b, replacement...)
			}
			continue
		}
		invalid = false
		b = append(b, s[i:i+wid]...)
		i += wid
	}
	return string(b)
}
