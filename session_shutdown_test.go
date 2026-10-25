package yamux

import (
	"errors"
	"io"
	"testing"
	"time"
)

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
