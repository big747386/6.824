package mapreduce

import (
	"fmt"
)

type Empty interface {}
type semaphore chan Empty
//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
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
	// Your code here (Part III, Part IV).
	//
	isDone := make(map[int]bool)
	sem := make(semaphore, ntasks)
	for i := 0; i < ntasks; i++ {
		isDone[i] = false
		e := new(Empty)
		sem <- e
	}
	i := 0
	for true {
		if len(sem) == 0{
			break
		}
		for i = 0; i < ntasks; i++  {
			if isDone[i] == false {
				break
			}
		}
		doTaskArgs := DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}
		go func(registerChan chan string, doTaskArgs DoTaskArgs) {
			address := <- registerChan
			ok := call(address, "Worker.DoTask", doTaskArgs, nil)
			if ok == false {
				isDone[doTaskArgs.TaskNumber] = false
				return
			}
			//应不应该把错误的worker重新放入队列
			go func() {
				registerChan <- address
			}()
			isDone[doTaskArgs.TaskNumber] = true
			<- sem
			return
		}(registerChan, doTaskArgs)
	}
	fmt.Printf("Schedule: %v done\n", phase)
}
