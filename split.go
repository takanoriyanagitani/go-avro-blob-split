package split

const InputBlobSizeMaxDefault int = 1048576

type DecodeConfig struct {
	InputBlobSizeMax int
}

var DecodeConfigDefault DecodeConfig = DecodeConfig{
	InputBlobSizeMax: InputBlobSizeMaxDefault,
}

const BlockLengthDefault int = 100

type Codec string

const (
	CodecNull    Codec = "null"
	CodecDeflate Codec = "deflate"
	CodecSnappy  Codec = "snappy"
	CodecZstd    Codec = "zstandard"
	CodecBzip2   Codec = "bzip2"
	CodecXz      Codec = "xz"
)

type EncodeConfig struct {
	BlockLength int
	Codec
}

var EncodeConfigDefault EncodeConfig = EncodeConfig{
	BlockLength: BlockLengthDefault,
	Codec:       CodecNull,
}
