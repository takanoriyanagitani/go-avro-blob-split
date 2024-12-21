package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strconv"
	"strings"

	ab "github.com/takanoriyanagitani/go-avro-blob-split"
	. "github.com/takanoriyanagitani/go-avro-blob-split/util"

	ts "github.com/takanoriyanagitani/go-avro-blob-split/split/blob/text/str2strings"

	dh "github.com/takanoriyanagitani/go-avro-blob-split/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-blob-split/avro/enc/hamba"
)

var GetEnvVarByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var convertBytesToString IO[bool] = Bind(
	GetEnvVarByKey("ENV_CONVERT_BYTES_TO_STRING").Or(Of("false")),
	Lift(strconv.ParseBool),
)

var blobSizeMaxStr IO[string] = GetEnvVarByKey("ENV_BLOB_SIZE_MAX").
	Or(Of("1048576"))
var blobSizeMax IO[int] = Bind(
	blobSizeMaxStr,
	Lift(strconv.Atoi),
)

var decodeConfig IO[ab.DecodeConfig] = Bind(
	blobSizeMax,
	Lift(func(i int) (ab.DecodeConfig, error) {
		return ab.DecodeConfig{
			InputBlobSizeMax: i,
		}, nil
	}),
)

var blobKey IO[string] = GetEnvVarByKey("ENV_BLOB_KEY")

var originalMaps IO[iter.Seq2[map[string]any, error]] = Bind(
	decodeConfig,
	dh.ConfigToStdinToMaps,
)

var str2strings ts.StringToStrings = ts.BlobToSmallerByScanner()

type Config struct {
	BlobKey              string
	ConvertBytesToString bool
}

var config IO[Config] = Bind(
	blobKey,
	func(bk string) IO[Config] {
		return Bind(
			convertBytesToString,
			Lift(func(conv bool) (Config, error) {
				return Config{
					BlobKey:              bk,
					ConvertBytesToString: conv,
				}, nil
			}),
		)
	},
)

var schemaFilename IO[string] = GetEnvVarByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(
		func(filename string) (string, error) {
			f, e := os.Open(filename)
			if nil != e {
				return "", e
			}
			defer f.Close()

			limited := &io.LimitedReader{
				R: f,
				N: limit,
			}
			var buf strings.Builder
			_, e = io.Copy(&buf, limited)
			return buf.String(), e
		},
	)
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var flatMaps IO[iter.Seq2[map[string]any, error]] = Bind(
	originalMaps,
	func(
		original iter.Seq2[map[string]any, error],
	) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			config,
			func(cfg Config) IO[iter.Seq2[map[string]any, error]] {
				return str2strings.MapsToFlatMaps(
					original,
					cfg.BlobKey,
					cfg.ConvertBytesToString,
				)
			},
		)
	},
)

var maps2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(s string) IO[Void] {
		return Bind(
			flatMaps,
			eh.SchemaToMapsToStdoutDefault(s),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return maps2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
