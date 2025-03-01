package gzipx

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"encoding/json"
	"io"
	"os"
	"slices"
	"strconv"
	"testing"
)

var (
	//go:embed testdata/block.txt
	block_txt []byte

	//go:embed testdata/block_table.json
	block_table_json []byte
)

func TestReadWrite(t *testing.T) {
	var blockTable []blockEntry
	if err := json.Unmarshal(block_table_json, &blockTable); err != nil {
		t.Error(err)
		return
	}

	blocks := bytes.Split(block_txt, block_sep)

	// writer
	var b bytes.Buffer
	{
		w := NewWriterLimit(&b, blockTable[0].Length+blockTable[1].Length)
		for _, p := range blocks {
			if err := w.WriteBlock(bytes.NewReader(p)); err != nil {
				t.Error(err)
				return
			}
		}
		w.Close()
	}
	data := b.Bytes()
	_ = os.WriteFile("testdata/test_gen.gz", data, os.ModePerm)

	// generic ungzip
	{
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			t.Error(err)
			return
		}
		data, err := io.ReadAll(r)
		if err != nil {
			t.Error(err)
			return
		}
		if !slices.Equal(block_txt, data) {
			t.Error("genric gzip incorrect")
			_ = os.WriteFile("testdata/block_gen_txt", data, os.ModePerm)
			return
		}
	}

	// reader
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Error(err)
		return
	}

	// block_table
	if !slices.Equal(blockTable, r.blockTable) {
		t.Error("index incorrect")
		data, _ = json.MarshalIndent(r.blockTable, "", "")
		_ = os.WriteFile("testdata/block_table_gen.json", data, os.ModePerm)
		return
	}

	// block reader: jump
	var br *BlockReader
	for i := range blockTable {
		br, err = r.OpenBlock(i, br)
		if err != nil {
			t.Error(err)
			return
		}
		data, err = io.ReadAll(br)
		if err != nil {
			t.Error(err)
			return
		}
		if !bytes.Equal(data, blocks[i]) {
			t.Error("block " + strconv.Itoa(i) + "\nexpect " + string(blocks[i]) + "\nresult " + string(data))
		}
	}

	// block reader: separate
	for i := range blockTable {
		br, err = r.OpenBlock(i, nil)
		if err != nil {
			t.Error(err)
			return
		}
		data, err = io.ReadAll(br)
		br.Close()
		if err != nil {
			t.Error(err)
			return
		}
		if !bytes.Equal(data, blocks[i]) {
			t.Error("block " + strconv.Itoa(i) + "\nexpect " + string(blocks[i]) + "\nresult " + string(data))
		}
	}
}
