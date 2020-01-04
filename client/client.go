/*
Copyright 2019 Stefan Miller

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"go-resp3/client/internal/monitor"
)

//go:generate rediser

//go:generate converter

// Environment variables.
const (
	EnvHost = "REDIS_HOST"
	EnvPort = "REDIS_PORT"
)

// Default redis host and port.
const (
	LocalHost        = "127.0.0.1"
	DefaultRedisPort = "6379"
)

const (
	tcpNetwork = "tcp"
)

func hostPort(address string) string {
	if address != "" { // use provided address
		return address
	}
	host, ok := os.LookupEnv(EnvHost)
	if !ok {
		host = LocalHost
	}
	port, ok := os.LookupEnv(EnvPort)
	if !ok {
		port = DefaultRedisPort
	}
	return net.JoinHostPort(host, port)
}

// MsgCallback is the function type for Redis pubsub message callback functions.
type MsgCallback func(pattern, channel, msg string)

// MonitorCallback is the function type for the Redis monitor callback function.
type MonitorCallback func(time time.Time, db int64, addr string, cmd []string)

// TraceCallback is the function type for the tracing callback function.
type TraceCallback func(dir bool, b []byte)

// SendInterceptor is the function type for the send interceptor function.
type SendInterceptor func(name string, values []interface{})

type sendFct func(name string, result result)
type encodeFct func(values ...interface{}) error

// Conn represents the redis network connection.
type Conn interface {
	Commands
	Pipeline() Pipeline
	Close() error
}

// Dial connects to the redis server address.
func Dial(address string) (Conn, error) {
	return new(Dialer).Dial(address)
}

// DialContext connects to the redis server address using the provided context.
func DialContext(ctx context.Context, address string) (Conn, error) {
	return new(Dialer).DialContext(ctx, address)
}

//Dialer contains options for connecting to a redis server.
type Dialer struct {
	net.Dialer
	// Connection logging.
	Logger *log.Logger
	// Channel size for asynchronous result reader and handler.
	ChannelSize int
	// Duration to wait for async results before timeout.
	AsyncTimeout time.Duration
	// Redis authentication.
	User, Password string
	// Redis client name.
	ClientName string
	// Client cache.
	Cache *Cache
	// Monitor callback.
	MonitorCallback MonitorCallback
	// Callback function tracing Redis commands and results on RESP3 protocol level.
	// Direction dir is true for sent bytes b (commands), false for received bytes b (results).
	TraceCallback TraceCallback
	// Command interceptor (debugging).
	SendInterceptor SendInterceptor
}

func (d *Dialer) channelSize() int {
	if d.ChannelSize < 1 {
		return defaultChannelSize
	}
	if d.ChannelSize < minimumChannelSize {
		return minimumChannelSize
	}
	return d.ChannelSize
}

// Dial connects to the redis server address.
func (d *Dialer) Dial(address string) (Conn, error) {
	return d.DialContext(context.Background(), address)
}

// DialContext connects to the redis server address using the provided context.
func (d *Dialer) DialContext(ctx context.Context, address string) (Conn, error) {
	c, err := d.Dialer.DialContext(ctx, tcpNetwork, hostPort(address))
	if err != nil {
		return nil, err
	}
	return newConn(c, d)
}

// command
type command struct {
	mu     *sync.Mutex
	encode encodeFct
	send   sendFct
	values []interface{}
}

func newCommand(mu *sync.Mutex, encode encodeFct, send sendFct, sendInterceptor SendInterceptor) *command {
	if sendInterceptor == nil {
		return &command{
			mu:     mu,
			encode: encode,
			send:   send,
			values: make([]interface{}, 0),
		}
	}
	c := &command{
		mu:     mu,
		values: make([]interface{}, 0),
	}
	c.encode = func(values ...interface{}) error {
		c.values = append(c.values, values...)
		return encode(values...)
	}
	c.send = func(name string, result result) {
		sendInterceptor(name, c.values)
		c.values = c.values[:0]
		send(name, result)
	}
	return c
}

// check interface implementations.
var _ Commands = (*command)(nil)
var _ Conn = (*conn)(nil)

type conn struct {
	mu sync.Mutex

	netConn net.Conn
	logger  *log.Logger

	*command

	cancel context.CancelFunc

	wg sync.WaitGroup // wait for all goroutines to complete

	dec Decoder
	enc Encoder

	cmdChan chan RedisValue
	resChan chan result

	err error

	hello Result

	asyncTimeout    time.Duration
	cache           *Cache
	monitorCallback MonitorCallback

	sendInterceptor SendInterceptor
}

const (
	defaultChannelSize = 1000
	minimumChannelSize = 100
)

func newConn(netConn net.Conn, d *Dialer) (*conn, error) {
	c := &conn{
		netConn:         netConn,
		logger:          d.Logger,
		cmdChan:         make(chan RedisValue, d.channelSize()),
		resChan:         make(chan result, d.channelSize()),
		asyncTimeout:    d.AsyncTimeout,
		cache:           d.Cache,
		monitorCallback: d.MonitorCallback,
		sendInterceptor: d.SendInterceptor,
	}

	if c.logger != nil {
		c.logger.Printf("remote address %s - local address %s", c.netConn.RemoteAddr().String(), c.netConn.LocalAddr().String())
	}

	if d.TraceCallback != nil {
		c.enc, c.dec = tracer(d.TraceCallback, netConn)
	} else {
		c.enc = NewEncoder(netConn)
		c.dec = NewDecoder(netConn)
	}

	c.command = newCommand(&c.mu, c.enc.Encode, c.send, c.sendInterceptor)

	var ctx context.Context
	ctx, c.cancel = context.WithCancel(context.Background())

	c.wg.Add(2) //cmdHandler, reader
	go c.cmdHandler(ctx, c.cmdChan, c.resChan)
	go c.reader(ctx, c.cmdChan)

	var userPassword *UserPassword
	if d.User != "" || d.Password != "" {
		userPassword = &UserPassword{User: d.User, Password: d.Password}
	}
	var clientName *string
	if d.ClientName != "" {
		clientName = &d.ClientName
	}

	c.hello = c.Hello(redisVersion, userPassword, clientName)
	if err := c.hello.Err(); err != nil {
		c.cancel() //TODO how to shutdown gracefully
		return nil, err
	}
	return c, nil
}

func (c *conn) send(name string, result result) {
	if c.err != nil {
		result.setErr(c.err)
		c.mu.Unlock()
		return
	}

	err := c.enc.Flush()
	if err != nil {
		c.err = err
		result.setErr(err)
		c.mu.Unlock()
		return
	}

	result.setTimeout(c.asyncTimeout)
	result.setFlushed(true) // no pipeline

	c.resChan <- result
	c.mu.Unlock()
}

func (c *conn) flushPipeline(b *bytes.Buffer, results flushResults) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.err != nil {
		results.setErr(c.err)
		return c.err
	}
	if _, err := b.WriteTo(c.netConn); err != nil {
		c.err = err
		results.setErr(err)
		return err
	}
	for _, result := range results {
		result.flush()
		c.resChan <- result
	}
	return nil
}

func (c *conn) Close() error {
	c.cancel()
	if err := c.Quit().Err(); err != nil {
		return err
	}
	if c.logger != nil {
		c.logger.Println("...wait for goroutines")
	}
	c.wg.Wait() // wait for all goroutines to complete
	if c.logger != nil {
		c.logger.Println("...close connection")
	}
	return c.netConn.Close()
}

func (c *conn) Pipeline() Pipeline {
	return newPipeline(c)
}

func (c *conn) cmdHandler(ctx context.Context, readChan <-chan RedisValue, resChan chan result) {
	channels := map[string]MsgCallback{}

	defer c.wg.Done()

	for {

		val, ok := <-readChan
		if !ok { // channel closed
			goto close
		}

		switch val.Kind {
		case RkInvalid:
			panic("cmdHandler: invalid value type")
		case RkError:
			result := <-resChan
			result.setErr(val.Value.(*RedisError))
		case RkPush:

			switch notification := val.Value.(type) {

			case subscribeNotification:
				result := <-resChan
				result.ack()

				subscribe, ok := result.(*asyncSubscribeResult)
				if !ok {
					panic("subscribe: result mismatch")
				}

				for i, ch := range subscribe.channel { // expect a push message for all subscribed channels

					if i != 0 {
						val, ok := <-readChan
						if !ok { // channel closed
							goto close
						}
						notification = val.Value.(subscribeNotification)
					}
					if notification.channel != ch {
						panic("subscribe: command message channel mismatch")
					}
					channels[notification.channel] = subscribe.cb
				}

			case unsubscribeNotification:
				result := <-resChan
				result.ack()

				unsubscribe, ok := result.(*asyncUnsubscribeResult)
				if !ok {
					panic("subscribe: result mismatch")
				}

				if len(unsubscribe.channel) != 0 { // unsubscribe list of channels

					for i, ch := range unsubscribe.channel { // expect a push message for all unsubscribed channels

						if i != 0 {
							val, ok := <-readChan
							if !ok { // channel closed
								goto close
							}
							notification = val.Value.(unsubscribeNotification)
						}
						if notification.channel != ch {
							panic("subscribe: command message channel mismatch")
						}
						delete(channels, notification.channel)
					}

				} else { // unsubscribe from all channels
					for {
						delete(channels, notification.channel)
						if notification.count == 0 {
							break
						}
						val, ok := <-readChan
						if !ok { // channel closed
							goto close
						}
						notification = val.Value.(unsubscribeNotification)
					}
				}

			case publishNotification:
				if cb, ok := channels[notification.channel]; ok && cb != nil {
					cb(notification.pattern, notification.channel, notification.msg)
				}

			case invalidateNotification:
				if c.cache != nil {
					c.cache.invalidate(uint32(notification))
				}

			case monitor.Notification:
				if c.monitorCallback != nil {
					c.monitorCallback(notification.Time, notification.DB, notification.Addr, notification.Cmd)
				}

			case genericNotification:

			default:
				panic("push message: invalid messsage type")
			}

		default:
			result := <-resChan
			result.setValue(val)
		}
	}

close:

	// drain resChan
	if c.logger != nil {
		c.logger.Println("...draining commands in command handler")
	}
	close(resChan)
	for result := range resChan {
		result.setErr(c.err)
	}
	if c.logger != nil {
		c.logger.Println("...stopping command handler")
	}
}

func (c *conn) reader(ctx context.Context, cmdChan chan<- RedisValue) {
	defer c.wg.Done()

	for {
		val, err := c.dec.Decode()
		switch err {

		case nil: // ok

		case io.EOF:
			c.err = err
			if c.logger != nil {
				c.logger.Println("received EOF - connection closed")
			}
			goto close

		default:
			c.err = err
			if c.logger != nil {
				c.logger.Printf("error %s", err)
			}
			goto close
		}

		cmdChan <- val
	}

close:
	// stop handler
	if c.logger != nil {
		c.logger.Println("...stopping reader")
	}
	// stop handler
	close(cmdChan)
}
