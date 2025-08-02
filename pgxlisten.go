// Package pgxlisten provides higher level PostgreSQL LISTEN / NOTIFY tooling built on pgx.
package pgxlisten

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Listener connects to a PostgreSQL server, listens for notifications, and dispatches them to handlers based on
// channel.
type Listener struct {
	// Connect establishes or otherwise gets a connection for the exclusive use of the Listener. Listener takes
	// responsibility for closing any connection it receives. Connect is required.
	Connect func(ctx context.Context) (*pgx.Conn, error)

	// LogError is called by Listen when a non-fatal error occurs. Most errors are non-fatal. For example, a database
	// connection failure is considered non-fatal as it may be due to a temporary outage and the connection should be
	// attempted again later. LogError is optional.
	LogError func(context.Context, error)

	// ReconnectDelay configures the amount of time to wait before reconnecting in case the connection to the database
	// is lost. If set to 0, the default of 1 minute is used. A negative value disables the timeout entirely.
	ReconnectDelay time.Duration

	handlers   map[string]Handler
	mux        sync.RWMutex
	hasStarted bool
	conn       *pgx.Conn
}

// Handle sets the handler for notifications sent to channel.
func (l *Listener) Handle(ctx context.Context, channel string, handler Handler) error {
	l.mux.Lock()
	defer l.mux.Unlock()
	if l.handlers == nil {
		l.handlers = make(map[string]Handler)
	}

	_, ok := l.handlers[channel]
	l.handlers[channel] = handler
	if l.hasStarted && !ok {
		err := l.listenandbacklog(ctx, channel, handler)
		return err
	}
	return nil
}

// Unhandle removes the handler for notifications sent to channel.
func (l *Listener) Unhandle(ctx context.Context, channel string) (bool, error) {
	l.mux.Lock()
	defer l.mux.Unlock()
	if l.handlers == nil {
		return false, nil
	}
	_, ok := l.handlers[channel]
	if !ok {
		return false, nil
	}
	delete(l.handlers, channel)
	if l.hasStarted {
		_, err := l.conn.Exec(ctx, "unlisten "+pgx.Identifier{channel}.Sanitize())
		if err != nil {
			return true, err
		}
	}
	return true, nil
}

// Listen listens for and handles notifications. It will only return when ctx is cancelled or a fatal error occurs.
// Because Listen is intended to continue running even when there is a network or database outage most errors are not
// considered fatal. For example, if connecting to the database fails it will wait a while and try to reconnect.
func (l *Listener) Listen(ctx context.Context) error {
	if l.Connect == nil {
		return errors.New("Listen: Connect is nil")
	}

	if l.handlers == nil {
		return errors.New("Listen: No handlers")
	}

	reconnectDelay := time.Minute
	if l.ReconnectDelay != 0 {
		reconnectDelay = l.ReconnectDelay
	}

	for {
		err := l.listen(ctx)
		if err != nil {
			l.logError(ctx, err)
		}

		if reconnectDelay < 0 {
			if err := ctx.Err(); err != nil {
				return err
			}

			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(reconnectDelay):
			// If listenAndSendOneConn returned and ctx has not been cancelled that means there was a fatal database error.
			// Wait a while to avoid busy-looping while the database is unreachable.
		}
	}
}

func (l *Listener) listenandbacklog(ctx context.Context, channel string, handler Handler) error {
	_, err := l.conn.Exec(ctx, "listen "+pgx.Identifier{channel}.Sanitize())
	if err != nil {
		return fmt.Errorf("listen %q: %w", channel, err)
	}

	if backlogHandler, ok := handler.(BacklogHandler); ok {
		err := backlogHandler.HandleBacklog(ctx, channel, l.conn)
		if err != nil {
			l.logError(ctx, fmt.Errorf("handle backlog %q: %w", channel, err))
		}
	}
	return nil
}

func (l *Listener) listen(ctx context.Context) error {
	conn, err := l.Connect(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	l.hasStarted = true
	l.conn = conn

	for channel, handler := range l.handlers {
		err = l.listenandbacklog(ctx, channel, handler)
		if err != nil {
			return err
		}
	}

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("waiting for notification: %w", err)
		}

		func() {
			l.mux.RLock()
			defer l.mux.RUnlock()
			if handler, ok := l.handlers[notification.Channel]; ok {
				err := handler.HandleNotification(ctx, notification, conn)
				if err != nil {
					l.logError(ctx, fmt.Errorf("handle %s notification: %w", notification.Channel, err))
				}
			} else {
				l.logError(ctx, fmt.Errorf("missing handler: %s", notification.Channel))
			}
		}()
	}

}

func (l *Listener) logError(ctx context.Context, err error) {
	if l.LogError != nil {
		l.LogError(ctx, err)
	}
}

// Handler is the interface by which notifications are handled.
type Handler interface {
	// HandleNotification is synchronously called by Listener to handle a notification. If processing the notification can
	// take any significant amount of time this method should process it asynchronously (e.g. via goroutine with a
	// different database connection). If an error is returned it will be logged with the Listener.LogError function.
	HandleNotification(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error
}

// HandlerFunc is an adapter to allow use of a function as a Handler.
type HandlerFunc func(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error

// HandleNotification calls f(ctx, notificaton, conn).
func (f HandlerFunc) HandleNotification(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
	return f(ctx, notification, conn)
}

// BacklogHandler is an optional interface that can be implemented by a Handler to process unhandled events that
// occurred before the Listener started. For example, a simple pattern is to insert jobs into a table and to send a
// notification of the new work. When jobs are enqueued but the Listener is not running then HandleBacklog can read from
// that table and handle all jobs.
//
// To ensure that no notifications are lost the Listener starts listening before handling any backlog. This means it is
// possible for HandleBacklog to handle a notification and for HandleNotification still to be called. A Handler must be
// prepared for this situation when it is also a BacklogHandler.
type BacklogHandler interface {
	// HandleBacklog is synchronously called by Listener at the beginning of Listen at process any previously queued
	// messages or jobs. If processing can take any significant amount of time this method should process it
	// asynchronously (e.g. via goroutine with a different database connection). If an error is returned it will be logged
	// with the Listener.LogError function.
	HandleBacklog(ctx context.Context, channel string, conn *pgx.Conn) error
}
