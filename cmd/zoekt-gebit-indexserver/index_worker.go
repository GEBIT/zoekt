package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
)

type indexWorker struct {
	// the actual worker, where we delegate to
	w core.Worker
	// reference to queue, used for re-queuing
	q *queue.Queue
	// mutex on the index guaranteeing exclusive access
	muIndexDir indexMutex
	// counter metric for initial index run
	initialMetric queue.Metric
	// rescheduled requests
	rescheduledReqs sync.Map
	// callback function after initial index is finished
	initialFinished func()
}

// Unmarshals an indexRequest from a TaskMessage
func UnmarshalReq(task core.TaskMessage) (*indexRequest, error) {
	var r indexRequest
	if err := json.Unmarshal(task.Payload(), &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// Runs the given task, ensuring exclusive access to the index and
// accounting of metrics for initial tasks . If a task can not be run because
// it is already running, it will be rescheduled.
func (i *indexWorker) Run(ctx context.Context, task core.TaskMessage) error {
	r, err := UnmarshalReq(task)
	if err != nil {
		return err
	}

	ran := i.muIndexDir.With(r.ProjectPathWithNamespace, func() {
		err = i.w.Run(ctx, task)
	})
	if r.IsInitial {
		i.initialMetric.IncCompletedTask()
		if i.initialMetric.CompletedTasks() == i.initialMetric.SubmittedTasks() {
			i.initialFinished()
		}
	}
	if !ran {
		log.Printf("index job for repository already running, putting into reschedule: %s", r.ProjectPathWithNamespace)
		i.rescheduledReqs.Store(r.ProjectPathWithNamespace, r)
	}
	if err != nil {
		log.Printf("indexing %s failed: %s", r.ProjectPathWithNamespace, err)
		if r.IsInitial {
			i.initialMetric.IncFailureTask()
		}
	} else {
		if r.IsInitial {
			i.initialMetric.IncSuccessTask()
		}
	}

	return err
}

// Shutdown stops the worker and performs any necessary cleanup.
// It returns an error if the shutdown process fails.
func (i *indexWorker) Shutdown() error {
	return i.w.Shutdown()
}

// Queue adds a task to the worker's queue.
// It returns an error if the task cannot be added to the queue.
func (i *indexWorker) Queue(task core.TaskMessage) error {
	r, err := UnmarshalReq(task)
	if err != nil {
		return err
	}

	if r.IsInitial {
		i.initialMetric.IncSubmittedTask()
	}

	return i.w.Queue(task)
}

// Request retrieves a task from the worker's queue.
// It returns the queued message and an error if the retrieval fails.
func (i *indexWorker) Request() (core.TaskMessage, error) {
	return i.w.Request()
}

// Starts a new ticker with the given watchInterval. This checks
// periodically if there are index requests that should be rescheduled
// If so, it re-queues all index requests and deletes them from rescheduledReqs.
// This is supposed to run as a go routine.
func (i *indexWorker) reschedule(watchInterval time.Duration) {
	t := time.NewTicker(watchInterval)

	for {
		// drain the pool of rescheduled requests and re-queue them
		i.rescheduledReqs.Range(func(key, value any) bool {
			req := value.(*indexRequest)
			err := i.q.Queue(req)
			if err != nil {
				log.Printf("error rescheduling req: %v", req)
			}
			// this is allowed as per docs
			i.rescheduledReqs.Delete(key)
			return true
		})
		<-t.C
	}
}

// Creates a new indexWorker.
func NewIndexWorker(numWorkers int64, fn func()) *indexWorker {
	r := queue.NewRing(queue.WithFn(indexRepoTask))
	w := indexWorker{
		w:               r,
		muIndexDir:      indexMutex{},
		initialMetric:   queue.NewMetric(),
		initialFinished: fn,
	}
	w.q = queue.NewPool(numWorkers, queue.WithWorker(&w))
	w.rescheduledReqs = sync.Map{}
	go w.reschedule(10 * time.Second)
	return &w
}
