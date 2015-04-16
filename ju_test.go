// Copyright (c) 2015 AKUALAB INC., All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ju

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type tt struct {
	Name  string
	N     int
	Words []string
}

func (p tt) equal(o tt) bool {
	if p.Name != o.Name {
		return false
	}
	if p.N != o.N {
		return false
	}
	for k, v := range p.Words {
		if v != o.Words[k] {
			return false
		}
	}
	return true
}

func TestStreamFile(t *testing.T) {

	ref := []tt{}
	fn := filepath.Join(os.TempDir(), "stream.json")
	t.Log("writing to file: ", fn)
	f, err := os.Create(fn)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	words := []string{}
	for i := 0; i < 10; i++ {
		words = append(words, fmt.Sprintf("numero %d", i))
		x := tt{Name: "test", N: i, Words: words}
		ref = append(ref, x)
		WriteJSON(f, &x)
	}

	reader, err := JSONStreamer(fn)
	if err != nil {
		t.Fatal(err)
	}

	dec := json.NewDecoder(reader)
	for i := 0; ; i++ {
		var o tt
		e := dec.Decode(&o)
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		t.Log("read back:", o)
		if !ref[i].equal(o) {
			t.Fatalf("mismatch, expected %v, got %v", ref[i], o)
		}
	}
}

func TestStreamDir(t *testing.T) {

	ref := []tt{}
	base := filepath.Join(os.TempDir())
	dir := filepath.Join(base, "sd")
	e := os.MkdirAll(dir, 0777)
	if e != nil {
		t.Fatal(e)
	}

	listFN := filepath.Join(base, "jsonstreamer.list")
	t.Log("list file: ", listFN)
	listFile, err := os.Create(listFN)
	if err != nil {
		t.Fatal(err)
	}

	for k := 0; k < 10; k++ {
		fn := filepath.Join(dir, fmt.Sprintf("testfile-%d.json", k))
		listFile.WriteString(fn + "\n")
		t.Log("writing to file: ", fn)
		f, err := os.Create(fn)
		if err != nil {
			t.Fatal(err)
		}
		words := []string{}
		for i := 0; i < 10; i++ {
			words = append(words, fmt.Sprintf("numero %d", i))
			name := fmt.Sprintf("test file # %d, object # %d", k, i)
			x := tt{Name: name, N: i, Words: words}
			ref = append(ref, x)
			WriteJSON(f, &x)
		}
		f.Close()
	}
	listFile.Close()
	reader, err := JSONStreamer(dir)
	if err != nil {
		t.Fatal(err)
	}

	dec := json.NewDecoder(reader)
	for i := 0; ; i++ {
		var o tt
		e := dec.Decode(&o)
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		t.Log(i, "read back:", o)
		if !ref[i].equal(o) {
			t.Fatalf("mismatch, expected %v, got %v", ref[i], o)
		}
	}
	e = reader.Close()
	if e != nil {
		t.Fatal(e)
	}

	reader, err = JSONStreamer(listFN)
	if err != nil {
		t.Fatal(err)
	}
	dec = json.NewDecoder(reader)
	for i := 0; ; i++ {
		var o tt
		e := dec.Decode(&o)
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		t.Log(i, "read back:", o)
		if !ref[i].equal(o) {
			t.Fatalf("mismatch, expected %v, got %v", ref[i], o)
		}
	}
	e = reader.Close()
	if e != nil {
		t.Fatal(e)
	}

}
