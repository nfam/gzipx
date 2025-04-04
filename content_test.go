package gzipx

import (
	"archive/zip"
	"bytes"
	_ "embed"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nfam/pool/iocopy"
)

func TestContent(t *testing.T) {
	const content_file = "content_gen.zip"

	zr, err := zip.OpenReader("testdata/" + content_file)
	if err != nil {
		return
	}
	defer zr.Close()

	var b bytes.Buffer
	w := NewWriter(&b)

	for _, f := range zr.File {
		r, _ := f.Open()
		if err = w.WriteBlock(r); err != nil {
			t.Error(err)
			return
		}
		r.Close()
	}
	if err = w.Close(); err != nil {
		t.Error(err)
		return
	}
	data := b.Bytes()
	_ = os.WriteFile("testdata/"+strings.TrimSuffix(content_file, ".zip")+".gz", data, os.ModePerm)

	// reader
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Error(err)
		return
	}

	var blockReader *BlockReader
	for i, f := range zr.File {
		var d1, d2 []byte
		{
			r, _ := f.Open()
			d1, _ = io.ReadAll(r)
			r.Close()
		}
		{
			blockReader, err = r.OpenBlock(i, blockReader)
			if err != nil {
				t.Error(err)
				return
			}
			d2, _ = io.ReadAll(blockReader)
		}
		if !bytes.Equal(d1, d2) {
			t.Error("not equal " + f.Name)
			return
		}
	}

	// time
	start := time.Now()
	for i := range zr.File {
		blockReader, err = r.OpenBlock(i, nil)
		if err != nil {
			t.Error(err)
			return
		}
		iocopy.Copy(io.Discard, blockReader)
	}
	d := time.Since(start) / time.Duration(len(zr.File))
	_ = os.WriteFile("testdata/"+strings.TrimSuffix(content_file, ".zip")+".read.txt", []byte(d.String()), os.ModePerm)
}
