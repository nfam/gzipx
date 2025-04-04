package gzipx

import (
	"compress/gzip"
	"encoding/binary"
	"io"
	"io/fs"

	"github.com/nfam/pool/buffer"
	pgzip "github.com/nfam/pool/gzip"
	"github.com/nfam/pool/iocopy"
)

type Writer struct {
	base       io.Writer
	pool       *pgzip.Writer
	stat       frameStat
	frame      *gzip.Writer
	frameTable []int64
	blockTable []blockEntry
}

type frameStat struct {
	base      io.Writer
	limit     int64 // uncompressed size limit
	inputSize int64 // uncompressed size
	frameSize int64 // compressed size, including header, trailer
}

const default_frame_limit = int64(1 << 17) // 128 KiB

var block_sep = []byte{'\n', '\n'}

func NewWriter(w io.Writer) *Writer {
	return NewWriterLimit(w, default_frame_limit)
}

func NewWriterLimit(w io.Writer, limit int64) *Writer {
	return &Writer{
		base: w,
		pool: pgzip.NewWriter(io.Discard),
		stat: frameStat{base: w, limit: limit},
	}
}

func (w *Writer) WriteBlock(r io.Reader) error {
	if w.pool == nil {
		return fs.ErrClosed
	}
	if w.frame != nil {
		w.frame.Write(block_sep)
		w.stat.inputSize += int64(len(block_sep))
		if w.stat.inputSize >= w.stat.limit {
			if err := w.frame.Close(); err != nil {
				return err
			}
			w.frameTable = append(w.frameTable, w.stat.frameSize)
			w.frame = nil
		}
	}
	if w.frame == nil {
		w.stat.inputSize = 0
		w.stat.frameSize = 0
		w.frame = w.pool.Writer
		w.frame.Reset(&w.stat)
	}

	n, err := iocopy.Copy(w.frame, r)
	if err != nil {
		return err
	}
	w.blockTable = append(w.blockTable, blockEntry{
		len(w.frameTable),
		w.stat.inputSize,
		n,
	})
	w.stat.inputSize += n
	return nil
}

func (w *Writer) Close() error {
	if w.pool == nil {
		return fs.ErrClosed
	}
	if w.frame != nil {
		if err := w.frame.Close(); err != nil {
			return err
		}
		w.frameTable = append(w.frameTable, w.stat.frameSize)
		w.frame = nil
	}
	w.pool.Close()
	w.pool = nil

	b := buffer.Get()
	defer b.Close()
	s := seekStat{b: b}

	s.append(int64(len(w.frameTable)))
	for i := range w.frameTable {
		s.append(w.frameTable[i])
	}

	s.append(int64(len(w.blockTable)))
	for i := range w.blockTable {
		b := &w.blockTable[i]
		s.append(int64(b.Frame))
		s.append(b.Offset)
		s.append(b.Length)
	}
	return s.write(w.base)
}

func (s *frameStat) Write(p []byte) (int, error) {
	n, err := s.base.Write(p)
	s.frameSize += int64(n)
	return n, err
}

// ----------------------------
// seekStat
// ----------------------------

type seekStat struct {
	b    buffer.Buffer
	pbuf [10]byte
}

func (s *seekStat) append(n int64) {
	p := s.pbuf[:0]
	p = binary.AppendVarint(p, n)
	_, _ = s.b.Write(p)
}

func (s *seekStat) write(w io.Writer) error {
	var buf []buffer.Buffer
	defer func() {
		for _, b := range buf {
			b.Close()
		}
	}()

	data := s.b.Bytes()
	for len(data) > 0 {
		p := data[:min(len(data), empty_gzip_extra_max)]
		data = data[len(p):]
		size := uint16(len(p))

		b := buffer.Get()
		buf = append(buf, b)

		if _, err := b.Write(empty_gzip_prefix); err != nil {
			return err
		}
		if err := binary.Write(b, binary.LittleEndian, uint16(size+6)); err != nil {
			return err
		}
		if _, err := b.Write(empty_gzip_extra_sub); err != nil {
			return err
		}
		if err := binary.Write(b, binary.LittleEndian, size); err != nil {
			return err
		}
		if _, err := b.Write(p); err != nil {
			return err
		}
		if err := binary.Write(b, binary.LittleEndian, size); err != nil {
			return err
		}
		if _, err := b.Write(empty_gzip_suffix); err != nil {
			return err
		}
	}
	for i := len(buf) - 1; i >= 0; i-- {
		b := buf[i]
		if _, err := w.Write(b.Bytes()); err != nil {
			return err
		}
	}
	return nil
}
