#!/bin/sh

export ENV_SCHEMA_FILENAME=sample.d/sample.avsc

jsons2avro(){
	cat sample.d/sample.jsonl |
		json2avrows |
		cat > ./sample.d/input.avro
}

#jsons2avro

export ENV_BLOB_KEY=data
export ENV_CONVERT_BYTES_TO_STRING=true

cat sample.d/input.avro |
	./avroblobstring2strings |
	rq -aJ |
	jaq -c
