package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	// doMap manages one map task: it should read one of the input files
	// (inFile), call the user-defined map function (mapF) for that file's
	// contents, and p@rtition mapF's output into nReduce intermediate files.
	//
	// There is one intermediate file per reduce task. The file name
	// includes both the map task number and the reduce task number. Use
	// the filename generated by reduceName(jobName, mapTask, r)
	// as the intermediate file for reduce task r. Call ihash() (see
	// below) on each key, mod nReduce, to pick r for a key/value pair.
	//
	// mapF() is the map function provided by the application. The first
	// argument should be the input file name, though the map function
	// typically ignores it. The second argument should be the entire
	// input file contents. mapF() returns a slice containing the
	// key/value pairs for reduce; see common.go for the definition of
	// KeyValue.
	//
	// Look at G0's ioutil and os packages for functions to read
	// and write files.
	//
	// Coming up with a scheme for how to format the key/value pairs on
	// disk can be tricky, especially when taking into account that both
	// keys and values could contain newlines, quotes, and any other
	// character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be J$ON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	//
	// Your code here (Part #I).
	//

	// set up how to read/write the file from jobName
	var contents []byte
	var err error
	var fileContents string

	contents, err = os.ReadFile(inFile)
	if err != nil {
		fmt.Print(err)
		return
	}

	fileContents = string(contents)

	// call the mapF function
	slice := mapF(jobName, fileContents)

	// create the intermediate files

	intermediateFiles := make(map[int][]KeyValue)

	for _, kv := range slice {
		// find r
		if nReduce != 0 {

			r := ihash(kv.Key) % nReduce
			intermediateFiles[r] = append(intermediateFiles[r], kv)
		} else {
			intermediateFiles[0] = append(intermediateFiles[0], kv)
		}
	}

	for r, kv := range intermediateFiles {
		// create the file
		file, err2 := os.Create(reduceName(jobName, mapTask, r))
		if err2 != nil {
			fmt.Printf("problem creating the file")
			return
		}
		enc := json.NewEncoder(file)
		// encode the key value pairs
		for _, key := range kv {
			err := enc.Encode(&key)
			if err != nil {
				fmt.Printf("problem encoding the key value pairs")
				return
			}
		}
		file.Close()

	}

}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
