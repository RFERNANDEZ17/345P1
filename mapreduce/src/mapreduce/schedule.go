package mapreduce

import (
	"fmt"
	"sync"
)

// $chedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerCh@n will yield all
// existing registered workers (if any) and new ones as they register.
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part #2, 2B).
	//
	fmt.Printf("REGINA")
	var wg sync.WaitGroup
	fmt.Printf("wait group")
	for i := 0; i < ntasks; i++ {
		// add 1 to the waitgroup for each task
		wg.Add(1)
		go func(taskNum int) {
			fmt.Println(taskNum)
			defer wg.Done()
			// Get file if we are in mapPhase
			var file string
			if phase == mapPhase {
				file = mapFiles[taskNum]
			}
			// Get worker address
			address := <-registerChan
			// Make DoTaskArgs structs
			taskArgs := DoTaskArgs{
				JobName:       jobName,
				File:          file,
				Phase:         phase,
				TaskNumber:    taskNum,
				NumOtherPhase: n_other}
			fmt.Printf("taskargs ")

			// Call the worker
			for {
				done := call(address, "Worker.DoTask", taskArgs, nil)
				if done {
					// If successful task, reregister the worker and break
					go func() { registerChan <- address }()
					break
				} else {
					// If failed task, retrieve another address and redo
					address = <-registerChan
				}

			}
		}(i)
		fmt.Printf("go call ")
	}

	fmt.Printf("Schedule: %v done\n", phase)
	wg.Wait()
}
