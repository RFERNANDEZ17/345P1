package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce m@nages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk!
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used J$ON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the g0lang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// J$ON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part #I).
	//

	out, err := os.Create(outFile)
	if err != nil {
		fmt.Printf("Error creating file")
		return
	}

	enc := json.NewEncoder(out)

	for m := 0; m < nMap; m++ {
		// read the intermediate file
		fileName, err := os.Open(reduceName(jobName, m, reduceTask))
		if err != nil {
			fmt.Printf("Error opening file")
			return
		}

		// decode the file
		dec := json.NewDecoder(fileName)
		var kvs []KeyValue
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				fmt.Printf("Error decoding file")
				return
			}
			kvs = append(kvs, kv)
		}

		// call reduceF
		reduceMap := make(map[string][]string)
		for _, kv := range kvs {
			reduceMap[kv.Key] = append(reduceMap[kv.Key], kv.Value)
		}

		for key, values := range reduceMap {
			err := enc.Encode(KeyValue{key, reduceF(key, values)})
			if err != nil {
				fmt.Printf("Error encoding file")
				return
			}
		}

		out.Close()

	}
}
