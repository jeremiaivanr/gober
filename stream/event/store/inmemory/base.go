package inmemory

import (
	"context"
	"sync"
	"time"

	"github.com/iidesho/gober/metrics"
	"github.com/iidesho/gober/stream/event/store"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	writeCount     *prometheus.CounterVec
	writeTimeTotal *prometheus.CounterVec
	readCount      *prometheus.CounterVec
	readTimeTotal  *prometheus.CounterVec
)

type inMemEvent struct {
	Created  time.Time
	Event    store.Event
	Position store.StreamPosition
}

// stream Need to add a way to not store multiple events with the same id in the same stream.
type stream struct {
	dbLock *sync.RWMutex
	// notify is a channel that is closed to wake blocked readers whenever new
	// data is appended. Closing a channel is visible to synctest (unlike
	// sync.Cond.Wait which synctest classifies as permanently blocked), so
	// replacing sync.Cond with this pattern prevents spurious synctest
	// deadlock panics when tests run in a synctest bubble.
	notify   chan struct{}
	notifyMu sync.Mutex
	db       []inMemEvent
	position store.StreamPosition
}

type Stream struct {
	ctx       context.Context
	writeChan chan<- store.WriteEvent
	name      string
	data      stream
}

func Init(name string, ctx context.Context) (es *Stream, err error) {
	writeChan := make(chan store.WriteEvent, 100)
	es = &Stream{
		data: stream{
			db:     make([]inMemEvent, 0),
			dbLock: &sync.RWMutex{},
			notify: make(chan struct{}),
		},
		name:      name,
		writeChan: writeChan,
		ctx:       ctx,
	}
	if metrics.Registry != nil && writeCount == nil {
		writeCount = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inmemeory_event_write_count",
			Help: "in-memory event write count",
		}, []string{"stream"})
		err = metrics.Registry.Register(writeCount)
		if err != nil {
			return nil, err
		}
		writeTimeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inmemeory_event_write_time_total",
			Help: "in-memory event write time total",
		}, []string{"stream"})
		err = metrics.Registry.Register(writeTimeTotal)
		if err != nil {
			return nil, err
		}
		readCount = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inmemeory_event_read_count",
			Help: "in-memory event read count",
		}, []string{"stream"})
		err = metrics.Registry.Register(readCount)
		if err != nil {
			return nil, err
		}
		readTimeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "inmemeory_event_read_time_total",
			Help: "in-memory event read time total",
		}, []string{"stream"})
		err = metrics.Registry.Register(readTimeTotal)
		if err != nil {
			return nil, err
		}
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				// Wake any blocked readers so they can observe ctx.Done().
				es.data.broadcast()
				return
			case e := <-writeChan:
				func() {
					if writeCount != nil {
						start := time.Now()
						defer func() {
							writeCount.WithLabelValues(es.name).Inc()
							writeTimeTotal.WithLabelValues(es.name).
								Add(float64(time.Since(start).Microseconds()))
						}()
					}
					es.data.dbLock.Lock()
					defer es.data.dbLock.Unlock()
					defer func() {
						if e.Status != nil {
							close(e.Status)
						}
					}()
					se := inMemEvent{
						Event:    e.Event,
						Position: store.StreamPosition(len(es.data.db) + 1),
						Created:  time.Now(),
					}

					es.data.db = append(es.data.db, se)
					es.data.position = se.Position
					if e.Status != nil {
						e.Status <- store.WriteStatus{
							Time:     se.Created,
							Position: se.Position,
						}
					}

					es.data.broadcast()
				}()
			}
		}
	}()
	return
}

// broadcast closes the current notify channel (waking all waiters) and
// replaces it with a fresh one for the next wait cycle.
func (s *stream) broadcast() {
	s.notifyMu.Lock()
	old := s.notify
	s.notify = make(chan struct{})
	s.notifyMu.Unlock()
	close(old)
}

// notifyChan returns the current notification channel. Readers should capture
// it before checking whether there is new data, then block on it if there is
// none. This mirrors the sync.Cond pattern but uses a channel so synctest can
// track the goroutine as blocked-on-channel rather than permanently blocked.
func (s *stream) notifyChan() chan struct{} {
	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()
	return s.notify
}

func (es *Stream) Write() chan<- store.WriteEvent {
	return es.writeChan
}

func (es *Stream) Stream(
	from store.StreamPosition,
	ctx context.Context,
) (out <-chan store.ReadEvent, err error) {
	eventChan := make(chan store.ReadEvent, 5)
	out = eventChan
	go func() {
		defer close(eventChan)
		var start time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case <-es.ctx.Done():
				return
			default:
			}
			position := uint64(from)
			if from == store.STREAM_END {
				position = uint64(len(es.data.db))
			}
			for {
				select {
				case <-ctx.Done():
					return
				case <-es.ctx.Done():
					return
				default:
					if writeCount != nil {
						start = time.Now()
					}
					es.data.dbLock.RLock()
					for ; position < uint64(len(es.data.db)); position++ {
						se := es.data.db[position]
						es.data.dbLock.RUnlock()
						readEvent := store.ReadEvent{
							Event:    se.Event,
							Position: se.Position,
							Created:  se.Created,
						}
						select {
						case <-ctx.Done():
							return
						case <-es.ctx.Done():
							return
						case eventChan <- readEvent:
						}
						es.data.dbLock.RLock()
					}
					dbLen := uint64(len(es.data.db))
					es.data.dbLock.RUnlock()
					if position >= dbLen {
						// Capture the notify channel before the check so we
						// don't miss a broadcast that arrives between the
						// length check and the channel receive below.
						notify := es.data.notifyChan()
						es.data.dbLock.RLock()
						stillEmpty := position >= uint64(len(es.data.db))
						es.data.dbLock.RUnlock()
						if stillEmpty {
							select {
							case <-ctx.Done():
								return
							case <-es.ctx.Done():
								return
							case <-notify:
							}
						}
					}
				}
				if readCount != nil {
					readCount.WithLabelValues(es.name).Inc()
					readTimeTotal.WithLabelValues(es.name).
						Add(float64(time.Since(start).Microseconds()))
				}
			}
		}
	}()
	return
}

func (es *Stream) Name() string {
	return es.name
}

func (es *Stream) End() (pos store.StreamPosition, err error) {
	pos = es.data.position
	return
}
