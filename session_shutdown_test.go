package yamux

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blockingCloseConn models a transport whose Close blocks while another
// goroutine is still writing to it. golang.org/x/net/websocket behaves this
// way: Close writes a close frame, so it has to take the same mutex that a
// pending write holds. frp uses that transport for both its client and its
// server side of a tunnel.
//
// The block ends when the transport is released explicitly or when a write
// deadline expires, which mirrors a transport that gives up on a pending
// write once its deadline is reached.
type blockingCloseConn struct {
	net.Conn
	closeStarted chan struct{}
	release      chan struct{}
	closeOnce    sync.Once
	deadline     atomic.Int64
}

func newBlockingCloseConn(conn net.Conn) *blockingCloseConn {
	return &blockingCloseConn{
		Conn:         conn,
		closeStarted: make(chan struct{}),
		release:      make(chan struct{}),
	}
}

func (c *blockingCloseConn) SetWriteDeadline(t time.Time) error {
	if t.IsZero() {
		c.deadline.Store(0)
		return nil
	}
	c.deadline.Store(t.UnixNano())
	return nil
}

func (c *blockingCloseConn) Close() error {
	c.closeOnce.Do(func() { close(c.closeStarted) })
	for {
		select {
		case <-c.release:
			return c.Conn.Close()
		default:
		}
		if d := c.deadline.Load(); d != 0 && time.Now().UnixNano() >= d {
			return c.Conn.Close()
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestSessionCloseUnblocksStreamsWhenTransportCloseBlocks verifies that a
// session shutdown releases the goroutines that wait on its streams even when
// the transport close itself blocks.
//
// Before the fix, Session.Close awaited the transport close before it closed
// the streams, so a blocked transport close left every stream reader blocked
// forever. The wait loop then fell through to START on the closed shutdown
// channel, which turned the blocked readers into a 100% CPU spin.
func TestSessionCloseUnblocksStreamsWhenTransportCloseBlocks(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()

	// Keep draining the peer so the session can be established.
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := serverConn.Read(buf); err != nil {
				return
			}
		}
	}()

	transport := newBlockingCloseConn(clientConn)
	conf := testConf()
	conf.EnableKeepAlive = false
	session, err := Client(transport, conf)
	if err != nil {
		t.Fatalf("failed to create client session: %v", err)
	}

	stream, err := session.OpenStream()
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 16)
		_, err := stream.Read(buf)
		readDone <- err
	}()

	closeDone := make(chan struct{})
	go func() {
		_ = session.Close()
		close(closeDone)
	}()

	select {
	case <-transport.closeStarted:
	case <-time.After(5 * time.Second):
		close(transport.release)
		t.Fatal("session close did not reach the transport")
	}

	select {
	case err := <-readDone:
		if err == nil {
			t.Error("expected the blocked read to fail")
		}
		if !errors.Is(err, ErrSessionShutdown) && !errors.Is(err, io.EOF) {
			t.Errorf("unexpected read error: %v", err)
		}
	case <-time.After(5 * time.Second):
		close(transport.release)
		t.Fatal("read did not return while the transport close was blocked")
	}

	close(transport.release)
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("session close did not finish after the transport was released")
	}
}

// TestSessionCloseFinishesWhenTransportCloseBlocks verifies that the session
// shutdown itself does not depend on the transport close returning.
//
// Before the fix, Session.Close waited for the transport close and therefore
// never returned while a transport close was blocked, so the owner of the
// session could not proceed with a reconnect.
func TestSessionCloseFinishesWhenTransportCloseBlocks(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()

	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := serverConn.Read(buf); err != nil {
				return
			}
		}
	}()

	transport := newBlockingCloseConn(clientConn)
	conf := testConf()
	conf.EnableKeepAlive = false
	session, err := Client(transport, conf)
	if err != nil {
		t.Fatalf("failed to create client session: %v", err)
	}

	closeDone := make(chan struct{})
	go func() {
		_ = session.Close()
		close(closeDone)
	}()

	select {
	case <-transport.closeStarted:
	case <-time.After(5 * time.Second):
		close(transport.release)
		t.Fatal("session close did not reach the transport")
	}

	// The transport close is still blocked, but the session close must not
	// depend on it. Give the shutdown a bounded amount of time to finish.
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		close(transport.release)
		t.Fatal("session close did not finish while the transport close was blocked")
	}

	close(transport.release)
}

// TestStreamReadReturnsAfterSessionShutdown verifies that a stream read does
// not spin once the session has shut down.
//
// The wait loop used to fall through to START when the shutdown channel was
// closed, so the read kept looping at 100% CPU instead of returning.
func TestStreamReadReturnsAfterSessionShutdown(t *testing.T) {
	session := newSendTestSession(nil, time.Second)
	stream := newStream(session, 1, streamEstablished)
	close(session.shutdownCh)

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 16)
		_, err := stream.Read(buf)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("read error = %v, want %v", err, io.EOF)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read did not return after the session shut down")
	}
}

// TestStreamWriteReturnsAfterSessionShutdown verifies that a stream write
// does not spin once the session has shut down.
func TestStreamWriteReturnsAfterSessionShutdown(t *testing.T) {
	session := newSendTestSession(nil, time.Second)
	stream := newStream(session, 1, streamEstablished)
	close(session.shutdownCh)

	if _, err := stream.Write([]byte("payload")); !errors.Is(err, ErrSessionShutdown) {
		t.Fatalf("write error = %v, want %v", err, ErrSessionShutdown)
	}
}
