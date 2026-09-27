package main

import (
	"sync"
	"time"
)

type Args any
type Reply any

type ReplicatedClient interface {
	// Init initializes the client to use the given servers.
	// To make a particular request later,
	// the client can use callOne(srv, args), where srv
	// is one of the servers from the list.
	Init(servers []string, callOne func(string, Args) Reply)
	// Call makes a request on any available server.
	// Multiple goroutines may call Call concurrently.
	Call(args Args) Reply
}

type Client struct {
	servers []string
	callOne func(string, Args) Reply
	mu      sync.Mutex
	prefer  int
}

func (c *Client) Init(servers []string, callOne func(string, Args) Reply) {
	c.servers = servers
	c.callOne = callOne
}

func (c *Client) Call(args Args) Reply {
	type result struct {
		serverID int
		reply    Reply
	}

	done := make(chan result, len(c.servers))

	c.mu.Lock()
	prefer := c.prefer
	c.mu.Unlock()

	const timeout = 1 * time.Second
	t := time.NewTimer(timeout)
	defer t.Stop()

	var r result
	for offset := range c.servers {
		id := (prefer + offset) % len(c.servers)
		go func() {
			done <- result{id, c.callOne(c.servers[id], args)}
		}()

		select {
		case r = <-done:
			goto Done
		case <-t.C:
			// timeout
			t.Reset(timeout)
		}
	}

	r = <-done
Done:
	c.mu.Lock()
	c.prefer = r.serverID
	c.mu.Unlock()
	return r.reply
}
