package main

import "runtime"

func Schedule(servers chan string, numTask int, f func(srv string, task int) bool) {
	work := make(chan int, numTask)
	done := make(chan bool)
	exit := make(chan bool)

	runTasks := func(srv string) {
		for task := range work {
			if f(srv, task) {
				done <- true
			} else {
				work <- task
			}
		}
	}
	go func() {
		for {
			select {
			case srv := <-servers:
				go runTasks(srv)
			case <-exit:
				return
			}
		}
	}()

	for task := range numTask {
		work <- task
	}

	for range numTask {
		<-done
	}
	close(work)
	exit <- true
}
