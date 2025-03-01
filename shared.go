package gzipx

type blockEntry struct {
	Frame  int
	Offset int64
	Length int64
}

var (
	empty_gzip_prefix     = []byte{0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff}
	empty_gzip_prefix_len = len(empty_gzip_prefix)
	empty_gzip_suffix     = []byte{0x01, 0x00, 0x00, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	empty_gzip_suffix_len = len(empty_gzip_suffix)
	empty_gzip_extra_max  = 65529 // 65535 - 6
	empty_gzip_extra_sub  = []byte{0x00, 0x00}
	empty_gzip_size_min   = int64(empty_gzip_prefix_len + empty_gzip_suffix_len)
)
