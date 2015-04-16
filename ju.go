// Copyright (c) 2015 AKUALAB INC., All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package ju provides utilities for manipulating json objects.
package ju

import (
	"bufio"
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
//   1 - path is a file. Read stream of JSON objects from file.
//   2 - path is a directory. Read stream from all the files in that directory that have extension ".json".
//   3 - path is a file with extension ".list" that contains a list of paths to json files. Read from all the files in the list.
// The return value is of type io.ReadCloser, the caller is responsible for closing the JSONStreamer to release resources
// using the Close() method.
func JSONStreamer(path string) (io.ReadCloser, error) {

	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	switch {
	case fi.IsDir():
		return streamDir(path)
	case filepath.Ext(path) == ".json":
		return streamFile(path)
	case filepath.Ext(path) == ".list":
		return streamList(path)
	default:
		return nil, fmt.Errorf("can't parse path to stream json objects - must be a dir or have extensions \".json\" or \".list\"")
	}
}

func streamDir(path string) (io.ReadCloser, error) {

	files := []string{}
	filepath.Walk(path, func(fn string, info os.FileInfo, err error) error {

		if filepath.Ext(fn) != ".json" {
			return nil
		}
		files = append(files, fn)
		return nil
	})

	return &multi{files: files}, nil
}

type multi struct {
	files []string
	idx   int
	file  *os.File
}

func (m *multi) Read(p []byte) (int, error) {

	var err error
	if m.file == nil {
		m.file, err = os.Open(m.files[m.idx])
		if err != nil {
			return 0, err
		}
		m.idx++
	}
	n, e := m.file.Read(p)
	switch {

	case e == nil:
		// We are good.
		return n, nil

	case e == io.EOF && m.idx < len(m.files):
		// End of file but we have more files.
		err := m.file.Close()
		if err != nil {
			return n, err
		}
		m.file = nil
		return n, nil // we are not done yet!

	case e == io.EOF:
		// End of last file.
		err := m.file.Close()
		if err != nil {
			return 0, err
		}
		m.file = nil
		return n, io.EOF // we are done!

	default:
		// Some unknown error.
		err := m.file.Close()
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
	if m.file != nil {
		err := m.file.Close()
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
		if filepath.Ext(line) != ".json" {
			return nil, fmt.Errorf("in list [%s] found a line without a .json extension: %s", path, line)
		}
		files = append(files, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return &multi{files: files}, nil
}
