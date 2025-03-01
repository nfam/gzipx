package gzipx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"os"

	"github.com/nfam/pool/buffer"
	"github.com/nfam/pool/gzip"
)

var (
	errSeekable = errors.New("gzip: invalid seekable frame")
)

// A Reader serves content from a gzipx archive.
type Reader struct {
	rat        io.ReaderAt
	frameTable []int64
	blockTable []blockEntry
}

// A ReadCloser is a [Reader] that must be closed when no longer needed.
type ReadCloser struct {
	f *os.File
	Reader
}

// OpenReader will open the gzipx file specified by name and return a ReadCloser.
func OpenReader(name string) (*ReadCloser, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	r := new(ReadCloser)
	if err = r.init(f, fi.Size()); err != nil {
		f.Close()
		return nil, err
	}
	r.f = f
	r.rat = f
	return r, err
}

// NewReader returns a new [Reader] reading from r, which is assumed to
// have the given size in bytes.
func NewReader(r io.ReaderAt, size int64) (*Reader, error) {
	zr := new(Reader)
	zr.rat = r
	if err := zr.init(r, size); err != nil {
		return nil, err
	}
	zr.rat = r
	return zr, nil
}

func (r *Reader) init(rat io.ReaderAt, size int64) error {
	if size < empty_gzip_size_min {
		return errSeekable
	}

	b := buffer.Get()
	defer b.Close()

	// Read seek_table from extra of empty gzip.
	{
		var (
			pbuf [32]byte
			p    = pbuf[:]
		)

		var (
			off       = size
			sub_len   uint16
			extra_len uint16
		)
		for off > empty_gzip_size_min {
			off -= int64(empty_gzip_suffix_len + 2)
			if _, err := rat.ReadAt(p[:empty_gzip_suffix_len+2], off); err != nil {
				return err
			}
			if !bytes.Equal(p[2:2+empty_gzip_suffix_len], empty_gzip_suffix) {
				break
			}
			if _, err := binary.Decode(p[:2], binary.LittleEndian, &sub_len); err != nil {
				return errSeekable
			}
			off -= int64(sub_len)
			if off < 0 {
				return errSeekable
			}
			if _, err := b.ReadFrom(io.NewSectionReader(rat, off, int64(sub_len))); err != nil {
				return err
			}
			off -= int64(empty_gzip_prefix_len + 6)
			if off < 0 {
				return errSeekable
			}
			if _, err := rat.ReadAt(p[:empty_gzip_prefix_len+6], off); err != nil {
				return err
			}
			if !bytes.Equal(p[:empty_gzip_prefix_len], empty_gzip_prefix) {
				break
			}
			if _, err := binary.Decode(p[empty_gzip_prefix_len:empty_gzip_prefix_len+2], binary.LittleEndian, &extra_len); err != nil {
				return errSeekable
			}
			if extra_len != sub_len+6 {
				return errSeekable
			}
		}
	}

	// Parse seek_table.
	{
		// frame_table
		i64, err := binary.ReadVarint(b)
		if err != nil {
			return errSeekable
		}
		count := int(i64)
		r.frameTable = make([]int64, count)
		for i := 0; i < count; i++ {
			r.frameTable[i], err = binary.ReadVarint(b)
			if err != nil {
				return errSeekable
			}
		}

		// block_table
		i64, err = binary.ReadVarint(b)
		if err != nil {
			return errSeekable
		}
		count = int(i64)
		r.blockTable = make([]blockEntry, count)
		for i := 0; i < count; i++ {
			i64, err = binary.ReadVarint(b)
			if err != nil {
				return errSeekable
			}
			r.blockTable[i].Frame = int(i64)
			r.blockTable[i].Offset, err = binary.ReadVarint(b)
			if err != nil {
				return errSeekable
			}
			r.blockTable[i].Length, err = binary.ReadVarint(b)
			if err != nil {
				return errSeekable
			}
		}
	}

	return nil
}

// Close closes the gzipx file, rendering it unusable for I/O.
func (rc *ReadCloser) Close() error {
	return rc.f.Close()
}

// A BlockReader provides access to the Block's content.
type BlockReader struct {
	base   *Reader
	frame  *gzip.Reader
	index  int
	offset int64
	remain int64
}

// FrameCount returns the number of gzip frames.
func (r *Reader) FrameCount() int {
	return len(r.frameTable)
}

// BlockCount returns the number of block.
func (r *Reader) BlockCount() int {
	return len(r.blockTable)
}

// OpenFrame opens the frame reader at index (starts from 0) in the archive.
func (r *Reader) OpenFrame(index int) (*gzip.Reader, error) {
	if index < 0 || index >= len(r.frameTable) {
		return nil, fs.ErrNotExist
	}
	var off int64
	for i := 0; i < index; i++ {
		off += r.frameTable[i]
	}

	return gzip.NewReader(io.NewSectionReader(
		r.rat,
		off,
		r.frameTable[index]),
	)
}

// OpenBlock opens the block at index (starts from 0) in the archive.
func (r *Reader) OpenBlock(index int, br *BlockReader) (*BlockReader, error) {
	block, frameOffset, frameLength := r.find(index)
	if block == nil {
		return nil, fs.ErrNotExist
	}

	// Reuse reader if any.
	if br != nil && br.frame != nil {
		if b, _, _ := r.find(br.index); b != nil && b.Frame == block.Frame {
			// The expected block stays in the same frame, and
			// the reader has not read pass the block offset.
			if n := block.Offset - br.offset; n >= 0 {
				// Skip to the block offset.
				if n > 0 {
					if err := skip(br.frame, n); err != nil {
						br.frame.Close()
						br.frame = nil
						return nil, err
					}
				}
				br.index = index
				br.offset = block.Offset
				br.remain = block.Length
				return br, nil
			}
		}
		br.frame.Close()
		br.frame = nil
	}

	frameReader, err := gzip.NewReader(io.NewSectionReader(r.rat, frameOffset, frameLength))
	if err != nil {
		return nil, err
	}
	if block.Offset > 0 {
		if err := skip(frameReader, block.Offset); err != nil {
			frameReader.Close()
			return nil, err
		}
	}

	if br == nil {
		br = &BlockReader{
			r,
			frameReader,
			index,
			block.Offset,
			block.Length,
		}
	} else {
		*br = BlockReader{
			r,
			frameReader,
			index,
			block.Offset,
			block.Length,
		}
	}

	return br, nil
}

func (r *Reader) find(index int) (*blockEntry, int64, int64) {
	if index < 0 || index >= len(r.blockTable) {
		return nil, 0, 0
	}

	block := &r.blockTable[index]
	if block.Frame < 0 && block.Frame >= len(r.frameTable) {
		return nil, 0, 0
	}

	var off int64
	for i := 0; i < block.Frame; i++ {
		off += r.frameTable[i]
	}

	return block, off, r.frameTable[block.Frame]
}

// ----------------------------
// BlockReader
// ----------------------------

// Implements [io.Reader] interface.
func (r *BlockReader) Read(p []byte) (int, error) {
	if r.frame == nil || r.remain == 0 {
		return 0, io.EOF
	}
	if r.remain < int64(len(p)) {
		p = p[:r.remain]
	}
	n, err := r.frame.Read(p)
	r.offset += int64(n)
	r.remain -= int64(n)
	return n, err
}

// Implements [io.Closer] interface.
func (r *BlockReader) Close() error {
	if r.frame == nil {
		return nil
	}
	r.frame.Close()
	r.frame = nil
	return nil
}

func skip(r io.Reader, n int64) error {
	var (
		pbuf [256]byte
		p    = pbuf[:]
	)
	for n > 0 {
		if n < int64(len(p)) {
			p = p[:n]
		}
		if x, err := r.Read(p); err != nil {
			return err
		} else {
			n -= int64(x)
		}
	}
	return nil
}
