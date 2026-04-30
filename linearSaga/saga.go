package saga

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"

	// "sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/iidesho/gober/bcts"
	consensus "github.com/iidesho/gober/consensus/contenious"
	"github.com/iidesho/gober/crypto"
	"github.com/iidesho/gober/discovery/local"

	// "github.com/iidesho/gober/itr"
	"github.com/iidesho/gober/metrics"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/traces"
	"go.opentelemetry.io/otel/trace"

	// "github.com/iidesho/gober/stream/consumer"
	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store"
	gsync "github.com/iidesho/gober/sync"
	"github.com/iidesho/gober/webserver"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	MESSAGE_BUFFER_SIZE = 1024
	ERROR_BUFFER_SIZE   = 16
)

var (
	executionCount     *prometheus.CounterVec
	executionTimeTotal *prometheus.CounterVec
	reductionCount     *prometheus.CounterVec
	reductionTimeTotal *prometheus.CounterVec
)

type Saga[BT bcts.Writer, T bcts.ReadWriter[BT]] interface {
	ExecuteFirst(BT, context.Context) (uuid.UUID, error)
	// Status(uuid.UUID) (State, error)
	ReadErrors(
		id uuid.UUID,
		ctx context.Context,
	) (<-chan error, func() State, error)
	Close()
}

type executor[BT bcts.Writer, T bcts.ReadWriter[BT]] struct {
	ctx context.Context
	// es       consumer.Consumer[sagaValue[BT, T], *sagaValue[BT, T]]
	es       stream.FilteredStream[sagaValue[BT, T], *sagaValue[BT, T]]
	statuses gsync.Map[uuid.UUID, status]
	// startPosition gsync.Map[uuid.UUID, store.StreamPosition]
	knownExecutions gsync.Map[uuid.UUID, struct{}]
	provider        stream.CryptoKeyProvider
	failed          chan<- uuid.UUID
	close           context.CancelFunc
	sagaName        string
	version         string
	story           Story[BT, T]
	// tasks    []status
	// taskLock sync.Mutex
}

func Init[BT bcts.Writer, T bcts.ReadWriter[BT]](
	pers stream.Stream,
	serv webserver.Server,
	dataTypeVersion string,
	story Story[BT, T],
	p stream.CryptoKeyProvider,
	workers int,
	ctx context.Context,
) (*executor[BT, T], error) {
	if len(story.Actions) <= 1 {
		err := ErrNotEnoughActions
		return nil, err
	}
	ctxTask, cancel := context.WithCancel(ctx)
	token := gsync.NewObj[string]()
	dataTypeName := story.Name + "_saga"
	token.Set(dataTypeName) // This should be configurable and secure
	// es, err := consumer.New[sagaValue[BT, T]](pers, p, ctxTask)
	es, err := stream.Init[sagaValue[BT, T]](pers, ctxTask)
	if err != nil {
		cancel()
		return nil, err
	}
	events, err := es.Stream(
		event.AllTypes(),
		store.STREAM_START,
		stream.ReadDataType(dataTypeName),
		ctx,
	)
	if err != nil {
		cancel()
		return nil, err
	}
	out := &executor[BT, T]{
		sagaName: dataTypeName,
		version:  dataTypeVersion,
		// taskLock: sync.Mutex{},
		story:           story,
		statuses:        gsync.NewMap[uuid.UUID, status](),
		knownExecutions: gsync.NewMap[uuid.UUID, struct{}](),
		// startPosition: gsync.NewMap[uuid.UUID, store.StreamPosition](),
		// errors:   gsync.NewMap[uuid.UUID, chan error](),
		es:    es,
		close: cancel,
		ctx:   ctxTask,
	}
	discovery := local.New()
	for actI, action := range story.Actions {
		cons, aborted, approved, err := consensus.New(
			serv,
			token,
			discovery,
			fmt.Sprintf("saga_%s_%s", story.Name, action.ID),
			ctxTask,
		)
		if err != nil {
			return nil, err
		}
		story.Actions[actI].cons = cons
		story.Actions[actI].aborted = aborted
		story.Actions[actI].approved = approved
		go func() {
			for {
				select {
				case <-out.ctx.Done():
					return
				case <-approved:
				case <-aborted:
				}
				runtime.Gosched()
			}
		}()
	}
	if metrics.Registry != nil && executionCount == nil {
		executionCount = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "saga_execution_count",
			Help: "Contains saga execution count",
		}, []string{"story", "part", "id", "trace_id"})
		err := metrics.Registry.Register(executionCount)
		if err != nil {
			return nil, err
		}
		executionTimeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "saga_execution_time_total",
			Help: "Contains saga execution time total",
		}, []string{"story", "part", "id", "trace_id"})
		err = metrics.Registry.Register(executionTimeTotal)
		if err != nil {
			return nil, err
		}
		reductionCount = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "saga_reduction_count",
			Help: "Contains saga reduction count",
		}, []string{"story", "part", "id", "trace_id"})
		err = metrics.Registry.Register(reductionCount)
		if err != nil {
			return nil, err
		}
		reductionTimeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "saga_reduction_time_total",
			Help: "Contains saga reduction time total",
		}, []string{"story", "part", "id", "trace_id"})
		err = metrics.Registry.Register(reductionTimeTotal)
		if err != nil {
			return nil, err
		}
	}
	// Should probably move this out to an external function created by the user instead. For now adding a customizable worker pool size
	// exec := make(chan event.ReadEventWAcc[sagaValue[BT, T], *sagaValue[BT, T]])
	exec := make(chan event.ReadEvent[sagaValue[BT, T], *sagaValue[BT, T]], workers)
	for range workers {
		// go func(events <-chan event.ReadEventWAcc[sagaValue[BT, T], *sagaValue[BT, T]]) {
		go func(events <-chan event.ReadEvent[sagaValue[BT, T], *sagaValue[BT, T]]) {
			// for e := range events {
			for {
				select {
				case <-out.ctx.Done():
					return
				case e := <-events:
					func() {
						startTime := time.Now()
						actionI := findStep(story.Actions, e.Data.status.stepDone)
						if actionI >= len(story.Actions) {
							log.Fatal(
								"this should never happen...",
								"saga",
								story.Name,
								"actionLen",
								len(story.Actions),
								"gotI",
								actionI,
							)
							return
						}
						var curActionI int
						if e.Data.status.state == StateRetryable ||
							e.Data.status.state == StateFailed { // story.Actions[actionI].Id {
							curActionI = actionI
						} else {
							curActionI = actionI + 1
						}
						ctx := e.Data.ctx
						var childSpan trace.Span
						if traces.Traces != nil {
							ctx, childSpan = traces.Traces.Start(
								e.Data.ctx,
								fmt.Sprintf(
									"saga[%s] execute step %s id %s",
									story.Name,
									story.Actions[curActionI].ID,
									e.Data.status.id.String(),
								),
							)
							defer childSpan.End()
						}
						log := log.WithContext(ctx)
						defer func() {
							r := recover()
							if r != nil {
								log.WithError(
									fmt.Errorf("recoverd: %v, stack: %s", r, string(debug.Stack())),
								).Error("panic while executing")
								id, err := uuid.NewV7()
								log.WithError(err).Fatal("could not generage UUID")
								log.WithError(out.writeEvent(sagaValue[BT, T]{
									v: e.Data.v,
									status: status{
										stepDone:  e.Data.status.stepDone,
										retryFrom: "",
										duration:  time.Since(startTime),
										state:     StatePaniced,
										id:        e.Data.status.id,
										consID:    consensus.ConsID(id),
										err:       fmt.Errorf("recoveced panic: %v", r),
									},
									ctx: e.Data.ctx,
								})).Error("writing panic event", "id", e.Data.status.id.String())
								return
							}
						}()
						log.Debug("selected task", "id", e.Data.status.id, "event", e)
						if curActionI >= 0 && // Falesafing the -1 case
							(e.Data.status.state == StateRetryable ||
								e.Data.status.state == StateFailed) { // story.Actions[actionI].Id {
							state := StateFailed
							start := time.Now()
							reduceErr := story.Actions[curActionI].Handler.Reduce(&e.Data.v, ctx)
							if reductionCount != nil {
								var traceID string
								if childSpan != nil && childSpan.SpanContext().IsValid() {
									traceID = childSpan.SpanContext().TraceID().String()
								} else {
									traceID = "nil"
								}
								reductionCount.WithLabelValues(story.Name, story.Actions[curActionI].ID, e.Data.status.id.String(), traceID).
									Inc()
								reductionTimeTotal.WithLabelValues(story.Name, story.Actions[curActionI].ID, e.Data.status.id.String(), traceID).
									Add(float64(time.Since(start).Microseconds()))
							}

							story.Actions[curActionI].cons.Completed(e.Data.status.consID)
							retryFrom := e.Data.status.retryFrom
							consID := e.Data.status.consID
							if reduceErr == nil &&
								e.Data.status.state == StateRetryable {
								state = StateRetryable
								if curActionI == 0 ||
									story.Actions[curActionI].ID == e.Data.status.retryFrom {
									retryFrom = ""
									state = StatePending
									id, err := uuid.NewV7()
									log.WithError(err).Fatal("could not generage UUID")
									consID = consensus.ConsID(id)
								}
							}

							prevStepID := ""
							if curActionI != 0 {
								prevStepID = story.Actions[curActionI-1].ID
							}
							log.WithError(out.writeEvent(sagaValue[BT, T]{
								v: e.Data.v,
								status: status{
									stepDone:  prevStepID,
									retryFrom: retryFrom,
									duration:  time.Since(startTime),
									state:     state,
									id:        e.Data.status.id,
									consID:    consID,
									err:       reduceErr,
								},
								ctx: e.Data.ctx,
							})).Error("writing failed event", "id", e.Data.status.id.String())
							return
						}
						// Ignoring this state for now
						log.WithError(out.writeEvent(sagaValue[BT, T]{
							v: e.Data.v,
							status: status{
								stepDone:  e.Data.status.stepDone,
								retryFrom: e.Data.status.stepDone,
								state:     StateWorking,
								id:        e.Data.status.id,
								consID:    e.Data.status.consID,
							},
							ctx: e.Data.ctx,
						})).Error("writing panic event", "id", e.Data.status.id.String())
						state := StateSuccess
						start := time.Now()
						execErr := story.Actions[curActionI].Handler.Execute(&e.Data.v, ctx)
						if executionCount != nil {
							var traceID string
							if childSpan != nil && childSpan.SpanContext().IsValid() {
								traceID = childSpan.SpanContext().TraceID().String()
							} else {
								traceID = "nil"
							}
							executionCount.WithLabelValues(story.Name, story.Actions[curActionI].ID, e.Data.status.id.String(), traceID).
								Inc()
							executionTimeTotal.WithLabelValues(story.Name, story.Actions[curActionI].ID, e.Data.status.id.String(), traceID).
								Add(float64(time.Since(start).Microseconds()))
						}
						if execErr != nil {
							state := StateFailed
							retryFrom := ""
							stepDone := e.Data.status.stepDone
							var retryError retryableError
							if errors.As(execErr, &retryError) {
								log.WithError(retryError.err).
									Notice("there was an retryable error while executing task")
								retryFrom = retryError.from
								state = StateRetryable
								stepDone = story.Actions[curActionI].ID
								select {
								case <-out.ctx.Done():
									return
								case <-time.NewTimer(time.Second * 10).C:
									log.Warning("slept before retry", "duration", time.Second*10)
								}
							} else {
								log.WithError(execErr).
									Warning("there was an error while executing task. not finishing")
							}
							id, err := uuid.NewV7()
							log.WithError(err).Fatal("could not generage UUID")
							log.WithError(out.writeEvent(sagaValue[BT, T]{
								v: e.Data.v,
								status: status{
									stepDone:  stepDone,
									retryFrom: retryFrom,
									duration:  time.Since(startTime),
									state:     state,
									id:        e.Data.status.id,
									consID:    consensus.ConsID(id),
									err:       execErr,
								},
								ctx: e.Data.ctx,
							})).Error("writing failed event", "id", e.Data.status.id.String())
							return
						}
						if state != StateSuccess {
							log.Fatal(
								"Invalid saga state, should be success",
								"state",
								state,
								"value",
								e.Data,
							)
						}
						log.WithError(out.writeEvent(sagaValue[BT, T]{
							v: e.Data.v,
							status: status{
								stepDone: story.Actions[curActionI].ID,
								duration: time.Since(startTime),
								state:    state,
								id:       e.Data.status.id,
								consID:   e.Data.status.consID, // consensus.ConsID(rid),
							},
							ctx: e.Data.ctx,
						})).Error("writing panic event", "id", e.Data.status.id.String())
						log.Trace("executed task", "event", e)
					}()
				}
			}
		}(exec)
	}

	go out.handler(events, exec)

	return out, nil
}

func (t *executor[BT, T]) handler(
	// events <-chan event.ReadEventWAcc[sagaValue[BT, T], *sagaValue[BT, T]],
	// execChan chan event.ReadEventWAcc[sagaValue[BT, T], *sagaValue[BT, T]],
	events <-chan event.ReadEvent[sagaValue[BT, T], *sagaValue[BT, T]],
	execChan chan event.ReadEvent[sagaValue[BT, T], *sagaValue[BT, T]],
) {
	defer func() {
		r := recover()
		if r != nil {
			log.Fatal("exiting reader!!!", "r", r)
		}
	}()

	// for e := range events {
	for {
		select {
		case <-t.ctx.Done():
			log.Trace("service context timed out")
			return
		case e, ok := <-events:
			if !ok {
				return
			}

			log := log.WithContext(e.Data.ctx)
			log.Trace(
				"read event",
				"event",
				e,
				"data",
				e.Data,
				"id",
				e.Data.status.id,
				"state",
				e.Data.status.state,
				"step",
				findStep(t.story.Actions, e.Data.status.stepDone)+1,
			)
			// e.Acc()
			var actionI int
			switch e.Data.status.state {
			case StatePending:
				t.knownExecutions.GetOrInit(e.Data.status.id, func() struct{} { return struct{}{} })
				fallthrough
			case StateSuccess:
				// t.taskLock.Lock()
				// i := itr.NewIterator(t.tasks).
				// 	Enumerate().
				// 	Contains(func(v status) bool { return v.id == e.Data.status.id })
				// if i >= 0 {
				// 	t.tasks[i] = e.Data.status
				// } else {
				// 	t.tasks = append(t.tasks, e.Data.status)
				// }
				// t.taskLock.Unlock()
				actionI = findStep(t.story.Actions, e.Data.status.stepDone) + 1
				log.Trace("success / pending found", "loc", "executor")
				if actionI >= len(t.story.Actions) {
					log.Trace("saga is completed", "loc", "executor")
					continue
				}
			case StatePaniced:
				fallthrough
			case StateFailed:
				// t.taskLock.Lock()
				// i := itr.NewIterator(t.tasks).
				// 	Enumerate().
				// 	Contains(func(v status) bool { return v.id == e.Data.status.id })
				// if i >= 0 {
				// 	t.tasks[i] = e.Data.status
				// } else {
				// 	t.tasks = append(t.tasks, e.Data.status)
				// }
				// t.taskLock.Unlock()
				if e.Data.status.stepDone == "" {
					log.Trace(
						"rollback completed as there is no more completed steps",
						"loc",
						"executor",
					)
					continue
				}
				actionI = findStep(t.story.Actions, e.Data.status.stepDone)
				if actionI >= len(t.story.Actions) {
					log.Fatal(
						"this should never happen...",
						"saga",
						t.sagaName,
						"actionLen",
						len(t.story.Actions),
						"gotI",
						actionI,
					)
					continue
				}
				log.Trace("failed / paniced found")
			case StateRetryable:
				// t.taskLock.Lock()
				// i := itr.NewIterator(t.tasks).
				// 	Enumerate().
				// 	Contains(func(v status) bool { return v.id == e.Data.status.id })
				// if i >= 0 {
				// 	t.tasks[i] = e.Data.status
				// } else {
				// 	t.tasks = append(t.tasks, e.Data.status)
				// }
				// t.taskLock.Unlock()
				if e.Data.status.stepDone == "" {
					actionI = 0
					e.Data.status.state = StatePending
					break
				}
				actionI = findStep(t.story.Actions, e.Data.status.stepDone)
				if actionI >= len(t.story.Actions) {
					log.Fatal(
						"this should never happen...",
						"saga",
						t.sagaName,
						"actionLen",
						len(t.story.Actions),
						"gotI",
						actionI,
					)
					continue
				}
				log.Trace("retryable found")
			case StateWorking:
				// t.taskLock.Lock()
				// i := itr.NewIterator(t.tasks).
				// 	Enumerate().
				// 	Contains(func(v status) bool { return v.id == e.Data.status.id })
				// if i >= 0 {
				// 	t.tasks[i] = e.Data.status
				// } else {
				// 	t.tasks = append(t.tasks, e.Data.status)
				// }
				// t.taskLock.Unlock()
				log.Trace("working found")
				continue
			default:
				log.Fatal(
					"Invalid state found",
					"state",
					e.Data.status.state,
					"status",
					e.Data.status,
				)
				continue
			}
			// This is just temporary, it will change when Barry is done...
			// log.Info("requesting consensus")
			startCons := time.Now()
			t.story.Actions[actionI].cons.Request(e.Data.status.consID)
			log.Debug("consensus timing",
				"saga_id", e.Data.status.id,
				"action", t.story.Actions[actionI].ID,
				"duration", time.Since(startCons),
				"action_index", actionI,
			)
			// log.Debug(
			// 	"won event",
			// 	"name",
			// 	t.es.Name(),
			// 	"id",
			// 	e.Data.status.id,
			// )
			// log.Info("received consensus consensus")
			// func(e event.ReadEventWAcc[sagaValue[BT, T], *sagaValue[BT, T]]) {
			func(e event.ReadEvent[sagaValue[BT, T], *sagaValue[BT, T]]) {
				log.Trace("time to do work, writing to exec chan")
				select {
				case execChan <- e:
					log.Trace(
						"wrote to exec chan",
						"name",
						t.es.Name(),
						"id",
						e.Data.status.id,
					)
				case <-t.ctx.Done():
					log.Trace("service context timed out")
					return
					// case <-time.After(time.Second * 10):
					// 	log.Fatal("timed out writing to exec chan")
					// case <-e.CTX.Done():
					// 	log.Trace("event context timed out")
					// 	return
				}
			}(e)
			// case <-time.After(time.Second * 20):
			// 	log.Fatal(
			// 		"timed out reading new events",
			// 		"completedSagas",
			// 		completedSagas,
			// 		"started",
			// 		startedSagas,
			// 		"events",
			// 		eventCount,
			// 	)
		}
	}
}

func (t *executor[BT, T]) event(
	eventType event.Type,
	data *sagaValue[BT, T],
) (e event.Event[sagaValue[BT, T], *sagaValue[BT, T]]) {
	e = event.Event[sagaValue[BT, T], *sagaValue[BT, T]]{
		Type: eventType,
		Data: data,
		Metadata: event.Metadata{
			Version:  t.version,
			DataType: t.sagaName,
			Key:      crypto.SimpleHash(data.status.id.String()),
			Extra: map[string]string{
				"id": data.status.id.String(),
			},
		},
		Shard: data.status.id.String(),
	}
	return
}

func (t *executor[BT, T]) writeEvent(tt sagaValue[BT, T]) error {
	// start := time.Now()
	// defer func() {
	// 	log.Notice("wrote event", "took", time.Since(start))
	// }()
	we := event.NewWriteEvent(t.event(event.Created, &tt))
	select {
	case t.es.Write() <- we:
	// case <-time.After(time.Second * 6):
	// 	err := fmt.Errorf("failed to write event in time, %s=%s", "id", tt.status.id)
	// 	log.WithError(err).Fatal("writing event")
	// 	return err
	case <-t.ctx.Done():
		log.Info("Executor context cancelled during event write")
		select {
		case t.es.Write() <- we:
		case <-time.After(time.Second * 6):
			log.Info("timed out writing after ctx done")
			// return nil
		}
		return nil
	}
	select {
	case ws := <-we.Done():
		return ws.Error
	// case <-time.After(time.Second * 6):
	// 	err := fmt.Errorf("failed to read write event status in time, %s=%s", "id", tt.status.id)
	// 	log.WithError(err).Fatal("writing event")
	// 	return err
	case <-t.ctx.Done():
		log.Info("Executor context cancelled during event write")
		select {
		case ws := <-we.Done():
			return ws.Error
		case <-time.After(time.Second * 6):
			log.Info("timed out writing after ctx done")
			return nil
		}
	}
}

func (t *executor[BT, T]) ExecuteFirst(
	dt BT,
	ctx context.Context,
) (uuid.UUID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, err
	}
	if traces.Traces != nil {
		_, childSpan := traces.Traces.Start(
			ctx,
			fmt.Sprintf("saga[%s] execute first %s", t.story.Name, id.String()),
		)
		defer childSpan.End()
	}
	// errChan := make(chan error, MESSAGE_BUFFER_SIZE)
	err = t.writeEvent(sagaValue[BT, T]{
		v: dt,
		status: status{
			state:  StatePending,
			id:     id,
			consID: consensus.ConsID(id),
		},
		ctx: ctx,
	})
	if err != nil {
		return uuid.Nil, err
	}
	t.knownExecutions.Set(id, struct{}{})
	return id, nil
}

func (t *executor[BT, T]) ReadErrors(
	id uuid.UUID,
	ctx context.Context,
) (<-chan error, func() State, error) {
	_, ok := t.knownExecutions.Get(id)
	if !ok {
		return nil, nil, ErrExecutionNotFound
	}
	curState := StateInvalid
	idstr := id.String()
	ctx, cancel := context.WithCancel(ctx)
	// s, err := t.es.StreamShard(
	// 	idstr,
	// 	event.AllTypes(),
	// 	store.STREAM_START,
	// 	func(md event.Metadata) bool {
	// 		return md.Extra["id"] != idstr
	// 	},
	// 	ctx,
	// )
	s, err := t.es.Stream(event.AllTypes(), store.STREAM_START,
		func(md event.Metadata) bool {
			return stream.ReadDataType(t.sagaName)(md) || md.Extra["id"] != idstr
		}, ctx)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	out := make(chan error, ERROR_BUFFER_SIZE)
	go func() {
		defer close(out)
		defer cancel()
		for {
			select {
			case <-t.ctx.Done():
				log.Trace("ReadErrors: context done")
				return
			case <-ctx.Done():
				log.Trace("ReadErrors: caller context done")
				return
			case e, ok := <-s:
				if !ok {
					log.Trace("ReadErrors: stream closed")
					return
				}
				curState = e.Data.status.state
				log.WithContext(e.Data.ctx).Trace(
					"read event in error",
					"want",
					idstr,
					"got",
					e.Metadata.Extra["id"],
					"stepDone",
					e.Data.status.stepDone,
					"state",
					e.Data.status.state,
					"failed",
					e.Data.status.state == StateFailed,
					"err",
					e.Data.status.err,
				)
				if e.Data.status.err != nil {
					select {
					case <-t.ctx.Done():
						log.Trace("ReadErrors: context done")
						return
					case <-ctx.Done():
						log.Trace("ReadErrors: caller context done")
						return
					case out <- e.Data.status.err:
					}
				}
				switch e.Data.status.state {
				case StateFailed:
					fallthrough
				case StatePaniced:
					log.WithContext(e.Data.ctx).Trace("rolling back")
					if e.Data.status.stepDone == "" {
						log.WithContext(e.Data.ctx).
							Trace("rollback completed as there is no more completed steps")
						return
					}
				case StatePending:
					fallthrough
				case StateSuccess:
					actionI := findStep(t.story.Actions, e.Data.status.stepDone) + 1
					log.WithContext(e.Data.ctx).Trace(
						"success / pending found",
						"actionI",
						actionI,
						"step done",
						e.Data.status.stepDone,
						"acrions",
						len(t.story.Actions),
					)
					if actionI >= len(t.story.Actions) {
						log.WithContext(e.Data.ctx).Trace("saga is completed")
						return
					}
				}
			}
		}
	}()
	return out, func() State { return curState }, nil
}

func (t *executor[BT, T]) Close() {
	t.close()
}

func findStep[BT any, T bcts.ReadWriter[BT]](actions []Action[BT, T], id string) int {
	if id == "" {
		return -1
	}
	for i, a := range actions {
		if a.ID == id {
			return i
		}
	}
	return -1
}

func IsRetryableError(err error) bool {
	return strings.HasPrefix(err.Error(), "RetryableError")
}

func RetryableError(from string, err error) retryableError {
	return retryableError{
		err:  err,
		from: from,
	}
}

type retryableError struct {
	err  error
	from string
}

func (r retryableError) Error() string {
	return fmt.Sprintf("RetryableError[%s]: %v", r.from, r.err)
}

var (
	// ErrRetryable                    = errors.New("error occured but can be retried")
	ErrNotEnoughActions             = errors.New("a story need more than one arc")
	ErrExecutionNotFound            = errors.New("saga id is invalid")
	ErrPreconditionsNosagaValueet   = errors.New("preconfitions not met for action")
	ErrBaseArcNeedsExactlyOneAction = errors.New("base arc can only and needs one action")
)
