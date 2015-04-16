// Copyright (c) 2015 AKUALAB INC., All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ju

import (
	"compress/gzip"
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
	zfn := filepath.Join(os.TempDir(), "stream.json.gz")
	t.Log("writing to file: ", fn)
	f, err := os.Create(fn)
	if err != nil {
		t.Fatal(err)
	}
	zf, zerr := os.Create(zfn)
	if zerr != nil {
		t.Fatal(zerr)
	}
	zr := gzip.NewWriter(zf)

	words := []string{}
	for i := 0; i < 10; i++ {
		words = append(words, fmt.Sprintf("numero %d", i))
		x := tt{Name: "test", N: i, Words: words}
		ref = append(ref, x)
		WriteJSON(f, &x)
		WriteJSON(zr, &x)
	}
	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = zr.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = zf.Close()
	if err != nil {
		t.Fatal(err)
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
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// gzip test
	reader, err = JSONStreamer(zfn)
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
		t.Log("read back:", o)
		if !ref[i].equal(o) {
			t.Fatalf("mismatch, expected %v, got %v", ref[i], o)
		}
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestStreamDir(t *testing.T) {

	ref := []tt{}
	base := filepath.Join(os.TempDir())
	dir := filepath.Join(base, "sd")
	zdir := filepath.Join(base, "sdz")
	e := os.MkdirAll(dir, 0777)
	if e != nil {
		t.Fatal(e)
	}
	e = os.MkdirAll(zdir, 0777)
	if e != nil {
		t.Fatal(e)
	}

	listFN := filepath.Join(base, "jsonstreamer.list")
	listFNz := filepath.Join(base, "jsonstreamerz.list")
	t.Log("list file: ", listFN)
	t.Log("list file: ", listFNz)
	listFile, err := os.Create(listFN)
	if err != nil {
		t.Fatal(err)
	}
	listFilez, errz := os.Create(listFNz)
	if errz != nil {
		t.Fatal(errz)
	}

	for k := 0; k < 10; k++ {
		fn := filepath.Join(dir, fmt.Sprintf("testfile-%d.json", k))
		fnz := filepath.Join(zdir, fmt.Sprintf("testfile-%d.json.gz", k))
		listFile.WriteString(fn + "\n")
		listFilez.WriteString(fnz + "\n")
		t.Log("writing to file: ", fn)
		t.Log("writing to file: ", fnz)
		f, err := os.Create(fn)
		if err != nil {
			t.Fatal(err)
		}
		fz, errz := os.Create(fnz)
		if errz != nil {
			t.Fatal(errz)
		}
		rz := gzip.NewWriter(fz)

		words := []string{}
		for i := 0; i < 10; i++ {
			words = append(words, fmt.Sprintf("numero %d", i))
			name := fmt.Sprintf("test file # %d, object # %d", k, i)
			x := tt{Name: name, N: i, Words: words}
			ref = append(ref, x)
			WriteJSON(f, &x)
			WriteJSON(rz, &x)
		}
		rz.Close()
		fz.Close()
		f.Close()
	}
	listFile.Close()
	listFilez.Close()

	// test dir
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

	// test list
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

	// test gzip dir
	reader, err = JSONStreamer(zdir)
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

	// test gzip list
	reader, err = JSONStreamer(listFNz)
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

func TestWrite(t *testing.T) {

	x := []float64{1.1, 2.2, 3.3}
	var y []float64

	fn := filepath.Join(os.TempDir(), "floats.json")
	WriteJSONFile(fn, x)
	t.Logf("Wrote to temp file: %s\n", fn)

	// Read back.
	e := ReadJSONFile(fn, &y)
	if e != nil {
		t.Fatal(e)
	}

	t.Logf("Original:%v", x)
	t.Logf("Read back from file:%v", y)

	for k, v := range x {
		if v != y[k] {
			t.Fatal("write/read mismatched")
		}
	}
}
