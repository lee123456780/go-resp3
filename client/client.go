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
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stfnmllr/go-resp3/client/internal/monitor"
)

//go:generate converter

// ClientVersion is the version number of the go-resp3 client.
const ClientVersion = "0.9.0"

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

// InvalidateCallback is the function type for the Redis cache invalidate callback function.
type InvalidateCallback func(keys []string)

// MonitorCallback is the function type for the Redis monitor callback function.
type MonitorCallback func(time time.Time, db int64, addr string, cmds []string)

// TraceCallback is the function type for the tracing callback function.
type TraceCallback func(dir bool, b []byte)

// SendInterceptor is the function type for the send interceptor function.
type SendInterceptor func(name string, values []interface{})

type sendFct func(name string, r *result)

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
	// TLS
	TLSConfig *tls.Config
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
	// Client cache invalidation callback.
	InvalidateCallback InvalidateCallback
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
	if d.TLSConfig != nil {
		c = tls.Client(c, d.TLSConfig)
	}
	return newConn(c, d)
}

// check interface implementations.
var _ Conn = (*conn)(nil)

const (
	defResultListItems = 1000
)

type conn struct {
	mu sync.Mutex

	netConn net.Conn
	logger  *log.Logger

	*command

	dec Decoder
	enc Encoder

	readChan chan interface{}
	resChan  chan *resultList
	sendChan chan *result

	hello Result

	asyncTimeout       time.Duration
	invalidateCallback InvalidateCallback
	monitorCallback    MonitorCallback

	sendInterceptor SendInterceptor

	nextResult func() *result

	shutdown   <-chan bool
	inShutdown uint32
}

const (
	defaultChannelSize = 10000
	minimumChannelSize = 100
)

func newConn(netConn net.Conn, d *Dialer) (*conn, error) {
	c := &conn{
		netConn:            netConn,
		logger:             d.Logger,
		readChan:           make(chan interface{}, d.channelSize()),
		resChan:            make(chan *resultList, defResultListItems),
		sendChan:           make(chan *result, d.channelSize()),
		asyncTimeout:       d.AsyncTimeout,
		invalidateCallback: d.InvalidateCallback,
		monitorCallback:    d.MonitorCallback,
		sendInterceptor:    d.SendInterceptor,
	}

	if c.logger != nil {
		c.logger.Printf("remote address %s - local address %s", c.netConn.RemoteAddr().String(), c.netConn.LocalAddr().String())
	}

	c.command = newCommand(c.send, c.sendInterceptor)

	if d.TraceCallback != nil {
		c.enc, c.dec = tracer(d.TraceCallback, netConn)
	} else {
		c.enc = NewEncoder(netConn)
		c.dec = NewDecoder(netConn)
	}

	c.nextResult = c.resultIterator()

	c.shutdown = c.watch()

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
		//c.cancel() //TODO how to shutdown gracefully
		return nil, err
	}
	return c, nil
}

func (c *conn) resultIterator() func() *result {
	var (
		list      *resultList
		size, pos int
	)

	return func() *result {
		if pos >= size {
			if list != nil {
				freeResultlist.put(list)
			}
			pos = 0
			var ok bool
			for {
				list, ok = <-c.resChan
				if !ok {
					return nil
				}
				// skip empty list
				if size = len(list.items); size != 0 {
					break
				}
			}
		}
		i := pos
		pos++
		return list.items[i]
	}
}

// ErrInShutdown is returned by a command executed during client is in shutdown status.
var ErrInShutdown = errors.New("connection is shutdown")

func (c *conn) send(name string, r *result) {
	if atomic.LoadUint32(&c.inShutdown) != 0 {
		r.setErr(ErrInShutdown)
		return
	}
	r.flush() // no pipeline
	c.sendChan <- r
}

func (c *conn) Close() error {
	if err := c.Quit().Err(); err != nil {
		return err
	}
	<-c.shutdown // wait for watcher to shutdown
	return c.netConn.Close()
}

func (c *conn) Pipeline() Pipeline {
	return newPipeline(c)
}

func (c *conn) watch() <-chan bool {

	shutdown := make(chan bool)

	go func() {

		var wgReader sync.WaitGroup  // wait for reader
		var wgHandler sync.WaitGroup // wait for handler
		var wgSender sync.WaitGroup  // wait for reader

		var event = make(chan error)

		wgHandler.Add(1)
		go c.cmdHandler(&wgHandler, c.readChan)
		wgReader.Add(1)
		go c.reader(&wgReader, c.readChan, event)
		wgSender.Add(1)
		go c.sender(&wgSender, c.sendChan, c.resChan)

		err := <-event
		wgReader.Wait() // wait for reader

		atomic.StoreUint32(&c.inShutdown, 1)

		close(c.readChan) // stop handler
		wgHandler.Wait()  // wait for handler

		close(c.sendChan) // stop sender
		wgSender.Wait()   // wait for sender

		// drain resChan
		close(c.resChan)
		for {
			r := c.nextResult()
			if r == nil {
				break
			}
			r.ack(nil, err)
		}

		close(shutdown)

	}()

	return shutdown

}

func (c *conn) cmdHandler(wg *sync.WaitGroup, readChan <-chan interface{}) {
	channelMap := map[string]MsgCallback{}

	defer wg.Done()

	for {

		val, ok := <-readChan
		if !ok { // channel closed
			return
		}

		switch val := val.(type) {

		case RedisValue:
			result := c.nextResult()
			result.ack(val, nil)

		case error:
			result := c.nextResult()
			result.ack(nil, val)

		case *subscribeNotification:
			if ok := c.handleSubscribeNotification(val, channelMap, readChan); !ok {
				return
			}

		case *unsubscribeNotification:
			if ok := c.handleUnsubscribeNotification(val, channelMap, readChan); !ok {
				return
			}

		case *publishNotification:
			if cb, ok := channelMap[val.channel]; ok && cb != nil {
				cb(val.pattern, val.channel, val.msg)
			}

		case *invalidateNotification:
			if c.invalidateCallback != nil {
				c.invalidateCallback(val.keys)
			}

		case *monitor.Notification:
			if c.monitorCallback != nil {
				c.monitorCallback(val.Time, val.Db, val.Addr, val.Cmds)
			}

		case *genericNotification:

		default:
			panic("invalid messsage type")
		}
	}
}

func (c *conn) handleSubscribeNotification(n *subscribeNotification, channelMap map[string]MsgCallback, readChan <-chan interface{}) bool {
	result := c.nextResult()

	size := len(result.request.cmd)
	channels := result.request.cmd[1:size]

	for i, ch := range channels { // expect a push message for all subscribed channels

		ch := ch.(string)

		if i != 0 {
			val, ok := <-readChan
			if !ok { // channel closed
				return false
			}
			n = val.(*subscribeNotification)
		}

		if n.channel != ch {
			panic("subscribe: command message channel mismatch")
		}
		channelMap[ch] = result.request.cb
	}

	result.ack(nil, nil)
	return true
}

func (c *conn) handleUnsubscribeNotification(n *unsubscribeNotification, channelMap map[string]MsgCallback, readChan <-chan interface{}) bool {
	result := c.nextResult()

	size := len(result.request.cmd)
	channels := result.request.cmd[1:size]

	if size > 1 { // unsubscribe list of channels

		for i, ch := range channels { // expect a push message for all unsubscribed channels

			ch := ch.(string)

			if i != 0 {
				val, ok := <-readChan
				if !ok { // channel closed
					return false
				}
				n = val.(*unsubscribeNotification)
			}
			if n.channel != ch {
				panic("unsubscribe: command message channel mismatch")
			}
			delete(channelMap, ch)
		}

		result.ack(nil, nil)
		return true
	}

	// unsubscribe from all channels
	for {
		delete(channelMap, n.channel)
		if n.count == 0 {
			break
		}
		val, ok := <-readChan
		if !ok { // channel closed
			return false
		}
		n = val.(*unsubscribeNotification)
	}

	result.ack(nil, nil)
	return true
}

func (c *conn) reader(wg *sync.WaitGroup, readChan chan<- interface{}, event chan<- error) {
	defer wg.Done()

	for {
		val, err := c.dec.Decode()
		if err != nil {
			event <- err
			return
		}
		readChan <- val
	}
}

func (c *conn) flush(pipeline bool, list *resultList) error {
	c.mu.Lock()
	for _, r := range list.items {
		if pipeline {
			r.flush()
		}
		r.setTimeout(c.asyncTimeout)
		c.enc.Encode(r.cmd())
	}
	if err := c.enc.Flush(); err != nil {
		c.mu.Unlock()
		panic(err) // TODO
	}
	c.resChan <- list
	c.mu.Unlock()
	return nil
}

func (c *conn) sender(wg *sync.WaitGroup, sendChan <-chan *result, resChan chan<- *resultList) {
	defer wg.Done()

	list := freeResultlist.get()

	for {
		select {
		case r, ok := <-sendChan:
			if !ok {
				goto close
			}
			list.items = append(list.items, r)

			burst := true
			for burst {
				select {
				case r, ok := <-sendChan:
					if !ok {
						goto close
					}
					list.items = append(list.items, r)
				default:
					burst = false
				}
			}
			c.flush(false, list)
			list = freeResultlist.get()
		}
	}
close:
	c.flush(false, list)
}
