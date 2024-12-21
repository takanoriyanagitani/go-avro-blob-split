package enc

import (
	"context"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	ab "github.com/takanoriyanagitani/go-avro-blob-split"
	. "github.com/takanoriyanagitani/go-avro-blob-split/util"
)

func MapsToWriterHamba(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	w io.Writer,
	s ha.Schema,
	opts ...ho.EncoderFunc,
) error {
	enc, e := ho.NewEncoderWithSchema(
		s,
		w,
		opts...,
	)
	if nil != e {
		return e
	}
	defer enc.Close()

	for row, e := range m {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if nil != e {
			return e
		}

		e = enc.Encode(row)
		if nil != e {
			return e
		}

		e = enc.Flush()
		if nil != e {
			return e
		}
	}
	return enc.Flush()
}

func CodecConv(c ab.Codec) ho.CodecName {
	switch c {
	case ab.CodecNull:
		return ho.Null
	case ab.CodecDeflate:
		return ho.Deflate
	case ab.CodecSnappy:
		return ho.Snappy
	case ab.CodecZstd:
		return ho.ZStandard
	default:
		return ho.Null
	}
}

func ConfigToOpts(cfg ab.EncodeConfig) []ho.EncoderFunc {
	var blockLen int = cfg.BlockLength
	var codec ab.Codec = cfg.Codec
	var hc ho.CodecName = CodecConv(codec)
	return []ho.EncoderFunc{
		ho.WithBlockLength(blockLen),
		ho.WithCodec(hc),
	}
}

func MapsToWriter(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	w io.Writer,
	schema string,
	cfg ab.EncodeConfig,
) error {
	parsed, e := ha.Parse(schema)
	if nil != e {
		return e
	}
	var opts []ho.EncoderFunc = ConfigToOpts(cfg)
	return MapsToWriterHamba(
		ctx,
		m,
		w,
		parsed,
		opts...,
	)
}

func MapsToStdout(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	schema string,
	cfg ab.EncodeConfig,
) error {
	return MapsToWriter(
		ctx,
		m,
		os.Stdout,
		schema,
		cfg,
	)
}

func ConfigToSchemaToMapsToStdout(
	cfg ab.EncodeConfig,
) func(string) func(iter.Seq2[map[string]any, error]) IO[Void] {
	return func(schema string) func(iter.Seq2[map[string]any, error]) IO[Void] {
		return func(m iter.Seq2[map[string]any, error]) IO[Void] {
			return func(ctx context.Context) (Void, error) {
				return Empty, MapsToStdout(
					ctx,
					m,
					schema,
					cfg,
				)
			}
		}
	}
}

var SchemaToMapsToStdoutDefault func(
	schema string,
) func(
	iter.Seq2[map[string]any, error],
) IO[Void] = ConfigToSchemaToMapsToStdout(ab.EncodeConfigDefault)
