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
	"fmt"
	"io"
	"os"
	"path/filepath"
)

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

// JSONStreamer returns a reader that returns JSON objects read from a file or from multiple files. JSON objects are stored as follows:
//   {"example":11, "any":"json"}
//   {"example":12, "any":"json"}
//   ...
//   EOF
// Supported cases:
// (1) path is a file. Read stream of JSON objects from file. File may be gzipped. Extension must be ".json" or ".gz".
// (2) path is a directory. Read stream from all the files in that directory that have extension ".json" or ".gz".
// (3) path is a file with extension ".list" that contains a list of paths to json files. Read from all the files in the list.
//
// The return value is of type io.ReadCloser. It is the caller's responsibility to call Close on the ReadCloser when done.
func JSONStreamer(path string) (io.ReadCloser, error) {

	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	ext := filepath.Ext(path)
	switch {
	case fi.IsDir():
		return streamDir(path)
	case ext == ".json" || ext == ".gz":
		return streamFile(path)
	case ext == ".list":
		return streamList(path)
	default:
		return nil, fmt.Errorf("can't parse path [%s] - must be a dir or have extensions \".json\" or \".gz\" or \".list\"", path)
	}
}

func streamDir(path string) (io.ReadCloser, error) {

	files := []string{}
	filepath.Walk(path, func(fn string, info os.FileInfo, err error) error {

		ext := filepath.Ext(fn)
		if ext != ".json" && ext != ".gz" {
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
	if m.idx >= len(m.files) {
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
		ext := filepath.Ext(line)
		if ext != ".json" && ext != ".gz" {
			return nil, fmt.Errorf("in list [%s] found a line without a .json or .gz extension: %s", path, line)
		}
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
