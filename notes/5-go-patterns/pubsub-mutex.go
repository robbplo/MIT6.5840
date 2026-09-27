package main

import "sync"

type Event string

type PubSub interface {
	Publish(e Event)
	Subscribe(c chan<- Event)
	Cancel(c chan<- Event)
}

type Server struct {
	mu  sync.Mutex
	sub map[chan<- Event]bool
}

func MakeServer() *Server {
	s := Server{}
	s.sub = make(map[chan<- Event]bool)
	s.sub = map[chan<- Event]bool{}
	return &s
}

func (s *Server) Publish(e Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.sub {
		c <- e
	}
}

func (s *Server) Subscribe(c chan<- Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sub[c] {
		panic("already subscribed")
	}
	s.sub[c] = true
}

func (s *Server) Cancel(c chan<- Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.sub[c] {
		panic("not subscribed")
	}
	close(c)
	delete(s.sub, c)
}
