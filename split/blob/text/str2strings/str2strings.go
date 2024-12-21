package str2strings

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"iter"
	"strings"

	. "github.com/takanoriyanagitani/go-avro-blob-split/util"
)

var (
	ErrInvalidBlob error = errors.New("invalid blob")
)

type BlobStr []byte
type SmallerBlobStr []byte

type StringToStrings func(BlobStr) IO[iter.Seq[SmallerBlobStr]]

func BlobToSmallerByScanner() StringToStrings {
	return func(original BlobStr) IO[iter.Seq[SmallerBlobStr]] {
		return func(_ context.Context) (iter.Seq[SmallerBlobStr], error) {
			return func(yield func(SmallerBlobStr) bool) {
				var rdr *bytes.Reader = bytes.NewReader(original)
				var s *bufio.Scanner = bufio.NewScanner(rdr)

				for s.Scan() {
					var line []byte = s.Bytes()
					if !yield(line) {
						return
					}
				}
			}, nil
		}
	}
}

func (s StringToStrings) AnyToSmallerBlobs(
	original any,
) IO[iter.Seq[SmallerBlobStr]] {
	var buf bytes.Buffer
	return func(ctx context.Context) (iter.Seq[SmallerBlobStr], error) {
		buf.Reset()
		switch t := original.(type) {
		case []byte:
			return s(t)(ctx)
		case string:
			_, _ = buf.WriteString(t) // error is always nil or panic
			return s(buf.Bytes())(ctx)
		default:
			return nil, ErrInvalidBlob
		}
	}
}

func SmallerToAnyNew(bytes2str bool) func([]byte) any {
	switch bytes2str {
	case false:
		return func(smaller []byte) any { return smaller }
	default:
		var buf strings.Builder
		return func(smaller []byte) any {
			buf.Reset()
			_, _ = buf.Write(smaller) // error is always nil or OOM
			return buf.String()
		}
	}
}

func (s StringToStrings) MapsToFlatMaps(
	original iter.Seq2[map[string]any, error],
	blobKey string,
	bytes2str bool,
) IO[iter.Seq2[map[string]any, error]] {
	var smaller2any func([]byte) any = SmallerToAnyNew(
		bytes2str,
	)
	return func(ctx context.Context) (iter.Seq2[map[string]any, error], error) {
		return func(yield func(map[string]any, error) bool) {
			buf := map[string]any{}
			for row, e := range original {
				clear(buf)

				if nil != e {
					yield(nil, e)
					return
				}

				for key, val := range row {
					if key != blobKey {
						buf[key] = val
					}
				}

				var blob any = row[blobKey]
				smallerBlobs, e := s.AnyToSmallerBlobs(blob)(ctx)
				if nil != e {
					yield(nil, e)
					return
				}

				for smaller := range smallerBlobs {
					buf[blobKey] = smaller2any(smaller)
					if !yield(buf, nil) {
						return
					}
				}
			}
		}, nil
	}
}

func (s StringToStrings) MapsToFlatMapsDefault(
	original iter.Seq2[map[string]any, error],
	blobKey string,
) IO[iter.Seq2[map[string]any, error]] {
	return s.MapsToFlatMaps(
		original,
		blobKey,
		false,
	)
}
