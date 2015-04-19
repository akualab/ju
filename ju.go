// Copyright (c) 2015 AKUALAB INC., All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package ju provides utilities for manipulating json objects.
package ju

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Done is returned as the error value when there are no more objects to process.
var Done = errors.New("no more json objects")

// ReadJSON unmarshals json data from an io.Reader.
// The param "o" must be a pointer to an object.
func ReadJSON(r io.Reader, o interface{}) error {
	dec := json.NewDecoder(r)
	err := dec.Decode(o)
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

// ReadJSONFile unmarshals json data from a file.
func ReadJSONFile(fn string, o interface{}) error {

	f, err := os.Open(fn)
	if err != nil {
		return err
	}
	e := ReadJSON(f, o)
	if e != nil {
		return e
	}
	e = f.Close()
	return e
}

// WriteJSON writes an object to an io.Writer.
func WriteJSON(w io.Writer, o interface{}) error {

	enc := json.NewEncoder(w)
	err := enc.Encode(o)
	if err != nil {
		return err
	}
	return nil
}

// WriteJSONFile writes to a file.
func WriteJSONFile(fn string, o interface{}) error {

	e := os.MkdirAll(filepath.Dir(fn), 0755)
	if e != nil {
		return e
	}
	f, err := os.Create(fn)
	if err != nil {
		return err
	}
	ee := WriteJSON(f, o)
	if ee != nil {
		return ee
	}
	e = f.Close()
	return e
}

// JSONStreamer will unmarshal a stream of JSON objects.
type JSONStreamer struct {
	fs  io.ReadCloser
	dec *json.Decoder
}

// NewJSONStreamer creates a new streamer to read json objects.
// See FileStreamer to specify the path.
func NewJSONStreamer(path string) (*JSONStreamer, error) {
	fs, err := FileStreamer(path, ".json")
	if err != nil {
		return nil, err
	}
	js := &JSONStreamer{
		fs:  fs,
		dec: json.NewDecoder(fs),
	}
	return js, nil
}

// Next returns the next JSON object.
// When there are no more results, Done is returned as the error.
func (js *JSONStreamer) Next(dst interface{}) error {
	e := js.dec.Decode(dst)
	if e == io.EOF {
		return Done
	}
	return e
}

// Close the JSON streamer. Will close the underlyign readers.
func (js *JSONStreamer) Close() error {
	return js.fs.Close()
}

// FileStreamer returns a reader that streams data from multiple files. The list of files can be specified in multiple ways:
// (1) path is a single file. The file may be gzipped in which case the name extension must be ".gz".
// (2) path is a directory. Reads from all the files in that directory such that (a) the filename must not start with a period,
// (b) the filename has extension ".gz", (c) the "ext" parameter is empty or the allowed extensions are listed, (d) path is not a symboic link.
// (3) path is a file with extension ".list" that contains a list of paths to files. Read from all the files in the list.
//
// The return value is of type io.ReadCloser. It is the caller's responsibility to call Close on the ReadCloser when done.
func FileStreamer(path string, ext ...string) (io.ReadCloser, error) {
	r, e := regexp.Compile("^[^.].*[.][[:alnum:]]+")
	if e != nil {
		return nil, e
	}
	allowed := map[string]bool{".gz": true}
	for _, v := range ext {
		if !strings.HasPrefix(v, ".") {
			v = "." + v
		}
		allowed[v] = true
	}
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	fext := filepath.Ext(path)
	switch {
	case fi.IsDir():
		return streamDir(path, allowed, r)
	case fext == ".list":
		return streamList(path)
	default:
		return streamFile(path)
	}
}

func matchExt(ext string, allowed map[string]bool) bool {
	if len(allowed) == 1 {
		return true
	}
	_, ok := allowed[ext]
	if ok {
		return true
	}
	return false
}

func streamDir(path string, allowed map[string]bool, r *regexp.Regexp) (io.ReadCloser, error) {
	files := []string{}
	filepath.Walk(path, func(fn string, info os.FileInfo, err error) error {
		if !r.MatchString(filepath.Base(fn)) {
			return nil
		}
		ext := filepath.Ext(fn)
		if !matchExt(ext, allowed) {
			return nil
		}
		files = append(files, fn)
		return nil
	})
	return &multi{files: files}, nil
}

type multi struct {
	files  []string
	idx    int
	reader io.ReadCloser
}

func (m *multi) Read(p []byte) (int, error) {
	if len(m.files) == 0 {
		return 0, io.EOF
	}
	if m.reader == nil {
		f, err := os.Open(m.files[m.idx])
		if err != nil {
			return 0, err
		}
		if filepath.Ext(m.files[m.idx]) == ".gz" {
			m.reader, err = NewGZIPReader(f)
			if err != nil {
				return 0, err
			}
		} else {
			m.reader = f
		}
		m.idx++
	}
	n, e := m.reader.Read(p)
	switch {

	case e == nil:
		// We are good.
		return n, nil

	case e == io.EOF && m.idx < len(m.files):
		// End of reader but we have more files.
		err := m.reader.Close()
		if err != nil {
			return n, err
		}
		m.reader = nil
		return n, nil // we are not done yet!

	case e == io.EOF:
		// End of last reader.
		err := m.reader.Close()
		if err != nil {
			return 0, err
		}
		m.reader = nil
		return n, io.EOF // we are done!

	default:
		// Some unknown error.
		err := m.reader.Close()
		if err != nil {
			return 0, err
		}
		return n, e
	}
}

// Close closes the underlying resources.
func (m *multi) Close() error {
	m.idx = 0
	m.files = nil
	if m.reader != nil {
		err := m.reader.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func streamFile(path string) (io.ReadCloser, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	if filepath.Ext(path) == ".gz" {
		r, err := NewGZIPReader(f)
		if err != nil {
			return nil, err
		}
		return r, nil
	}
	return f, nil
}

func streamList(path string) (io.ReadCloser, error) {

	f, e := os.Open(path)
	if e != nil {
		return nil, nil
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	files := []string{}
	for scanner.Scan() {
		line := scanner.Text()
		files = append(files, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return &multi{files: files}, nil
}

// GZIPReader is a wrapper to read compressed gzip files.
type GZIPReader struct {
	inReader   io.ReadCloser
	gzipReader *gzip.Reader
}

// NewGZIPReader creates a new GZIPReader that reads from r.
// The return value implements io.ReadCloser. It is the caller's responsibility to call Close when done.
func NewGZIPReader(r io.ReadCloser) (*GZIPReader, error) {
	gr := &GZIPReader{inReader: r}
	var err error
	gr.gzipReader, err = gzip.NewReader(gr.inReader)
	if err != nil {
		return nil, err
	}
	return gr, nil
}

// Read implements the io.Read interface.
func (g *GZIPReader) Read(p []byte) (int, error) {
	return g.gzipReader.Read(p)
}

// Close closes the gzip reader and the wrapped reader.
func (g *GZIPReader) Close() error {

	if g.inReader != nil {
		err := g.inReader.Close()
		if err != nil {
			return err
		}
	}
	err := g.gzipReader.Close()
	return err
}
