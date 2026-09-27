package main

type Event string

type PubSub interface {
	Publish(e Event)
	Subscribe(c chan<- Event)
	Cancel(c chan<- Event)
}

type subReq struct {
	c  chan<- Event
	ok chan bool
}

type Server struct {
	publish   chan Event
	subscribe chan subReq
	cancel    chan subReq
}

func MakeServer() *Server {
	s := Server{}
	s.publish = make(chan Event)
	s.subscribe = make(chan subReq)
	s.cancel = make(chan subReq)
	go s.loop()
	return &s
}

// Crashes if queue is empty with 'index out of range'
func queueNaive(in <-chan Event, out chan<- Event) {
	var q []Event
	for {
		select {
		case e := <-in:
			out <- e
		case out <- q[0]:
			q = q[1:]
		}
	}
}

func queueSendOut(in <-chan Event, out chan<- Event) {
	var q []Event
	for {
		var sendOut chan<- Event
		var next Event
		if len(q) > 0 {
			sendOut = out
			next = q[0]
		}
		select {
		case e := <-in:
			out <- e
		case sendOut <- next:
			q = q[1:]
		}
	}
}

func queue(in <-chan Event, out chan<- Event) {
	var q []Event
	for in != nil || len(q) > 0 {
		var sendOut chan<- Event
		var next Event
		if len(q) > 0 {
			sendOut = out
			next = q[0]
		}
		select {
		case e, ok := <-in:
			if !ok {
				in = nil
				break
			}
			out <- e
		case sendOut <- next:
			q = q[1:]
		}
	}
	close(out)
}

func (s *Server) loop() {
	sub := map[chan<- Event]chan<- Event{}
	for {
		select {
		case e := <-s.publish:
			for c := range sub {
				c <- e
			}
		case r := <-s.subscribe:
			if sub[r.c] != nil {
				r.ok <- false
				break
			}
			q := make(chan Event)
			go queue(q, r.c)
			sub[r.c] = q
			r.ok <- true
		case r := <-s.cancel:
			if sub[r.c] == nil {
				r.ok <- false
				break
			}
			close(sub[r.c])
			delete(sub, r.c)
			r.ok <- true
		}
	}
}

func (s *Server) Publish(e Event) {
	s.publish <- e
}

func (s *Server) Subscribe(c chan<- Event) {
	r := subReq{c: c, ok: make(chan bool)}
	s.subscribe <- r
	if !<-r.ok {
		panic("already subscribed")
	}
}

func (s *Server) Cancel(c chan<- Event) {
	r := subReq{c: c, ok: make(chan bool)}
	s.cancel <- r
	if !<-r.ok {
		panic("not subscribed")
	}
}
