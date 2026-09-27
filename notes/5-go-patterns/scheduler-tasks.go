package main

import "runtime"

func Schedule(servers []string, numTask int, f func(srv string, task int)) {
	idle := make(chan string, len(servers))
	for _, srv := range servers {
		idle <- srv
	}

	for task := range numTask {
		srv := <-idle
		go func() {
			f(srv, task)
			idle <- srv
		}()
	}

	for range servers {
		<-idle
	}
	runtime.Gosched()

}
