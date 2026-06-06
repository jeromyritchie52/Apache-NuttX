package main

import (
	"errors"
	"fmt"
	"sync"
)

// Config mimics the Kconfig options for NuttX task/stack allocation.
type Config struct {
	ConfigSchedTcbPool     bool // CONFIG_SCHED_TCBPOOL
	ConfigSchedStackPool   bool // CONFIG_SCHED_STACKPOOL
	ConfigSchedTcbPoolSize int  // CONFIG_SCHED_TCBPOOL_SIZE
	ConfigStackPoolSize    int  // CONFIG_STACK_POOL_SIZE
	ConfigStackSize        int  // CONFIG_STACK_SIZE
}

// TCB represents a Task Control Block (struct tcb_s).
type TCB struct {
	TaskID   int
	Name     string
	Priority int
	State    int
}

// TCBMemPool manages a fixed-size pool of TCBs to prevent fragmentation.
type TCBMemPool struct {
	pool chan *TCB
}

// NewTCBMemPool initializes the TCB memory pool.
func NewTCBMemPool(size int) *TCBMemPool {
	p := &TCBMemPool{
		pool: make(chan *TCB, size),
	}
	for i := 0; i < size; i++ {
		p.pool <- &TCB{}
	}
	return p
}

// Alloc allocates a TCB from the pool.
func (p *TCBMemPool) Alloc() (*TCB, error) {
	select {
	case tcb := <-p.pool:
		return tcb, nil
	default:
		return nil, errors.New("TCB pool exhausted")
	}
}

// Free returns a TCB to the pool.
func (p *TCBMemPool) Free(tcb *TCB) {
	// Reset TCB state before returning to pool
	*tcb = TCB{}
	select {
	case p.pool <- tcb:
	default:
		// Pool is full (should not happen if alloc/free are balanced)
	}
}

// StackPool manages a dedicated memory region for task stacks.
type StackPool struct {
	pool      chan []byte
	stackSize int
}

// NewStackPool initializes the stack pool.
func NewStackPool(poolSize, stackSize int) *StackPool {
	p := &StackPool{
		pool:      make(chan []byte, poolSize),
		stackSize: stackSize,
	}
	for i := 0; i < poolSize; i++ {
		p.pool <- make([]byte, stackSize)
	}
	return p
}

// Alloc allocates a stack from the pool.
func (p *StackPool) Alloc() ([]byte, error) {
	select {
	case stack := <-p.pool:
		return stack, nil
	default:
		return nil, errors.New("stack pool exhausted")
	}
}

// Free returns a stack to the pool.
func (p *StackPool) Free(stack []byte) {
	select {
	case p.pool <- stack:
	default:
		// Pool is full
	}
}

// TaskAllocator coordinates TCB and stack allocations.
type TaskAllocator struct {
	config    Config
	tcbPool   *TCBMemPool
	stackPool *StackPool
	mu        sync.Mutex
}

// NewTaskAllocator creates a new TaskAllocator based on the configuration.
func NewTaskAllocator(cfg Config) *TaskAllocator {
	var tcbPool *TCBMemPool
	var stackPool *StackPool

	if cfg.ConfigSchedTcbPool {
		tcbPool = NewTCBMemPool(cfg.ConfigSchedTcbPoolSize)
	}
	if cfg.ConfigSchedStackPool {
		stackPool = NewStackPool(cfg.ConfigStackPoolSize, cfg.ConfigStackSize)
	}

	return &TaskAllocator{
		config:    cfg,
		tcbPool:   tcbPool,
		stackPool: stackPool,
	}
}

// AllocateTask allocates a TCB and stack for a new task.
// It gracefully falls back to the general heap if pools are disabled or exhausted.
func (ta *TaskAllocator) AllocateTask(name string, priority int) (*TCB, []byte, error) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	var tcb *TCB
	var err error

	// 1. Allocate TCB
	if ta.config.ConfigSchedTcbPool && ta.tcbPool != nil {
		tcb, err = ta.tcbPool.Alloc()
		if err != nil {
			// Fallback to general heap allocation if pool is exhausted
			tcb = &TCB{}
		}
	} else {
		// Default flat heap allocation
		tcb = &TCB{}
	}

	tcb.Name = name
	tcb.Priority = priority

	// 2. Allocate Stack
	var stack []byte
	if ta.config.ConfigSchedStackPool && ta.stackPool != nil {
		stack, err = ta.stackPool.Alloc()
		if err != nil {
			// Fallback to general heap allocation if pool is exhausted
			stack = make([]byte, ta.config.ConfigStackSize)
		}
	} else {
		// Default flat heap allocation
		stack = make([]byte, ta.config.ConfigStackSize)
	}

	return tcb, stack, nil
}

// FreeTask releases the TCB and stack back to their respective pools or heap.
func (ta *TaskAllocator) FreeTask(tcb *TCB, stack []byte) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	// 1. Free Stack
	if ta.config.ConfigSchedStackPool && ta.stackPool != nil {
		// Only return to pool if it matches the pool's stack size
		if len(stack) == ta.config.ConfigStackSize {
			ta.stackPool.Free(stack)
		}
	}

	// 2. Free TCB
	if ta.config.ConfigSchedTcbPool && ta.tcbPool != nil {
		ta.tcbPool.Free(tcb)
	}
}

func main() {
	// Example configuration enabling both TCB and Stack pools
	cfg := Config{
		ConfigSchedTcbPool:     true,
		ConfigSchedTcbPoolSize: 5,
		ConfigSchedStackPool:   true,
		ConfigStackPoolSize:    5,
		ConfigStackSize:        4096,
	}

	allocator := NewTaskAllocator(cfg)

	fmt.Println("Simulating task creation and destruction with dedicated pools...")

	// Allocate a task
	tcb, stack, err := allocator.AllocateTask("WorkerTask", 100)
	if err != nil {
		fmt.Printf("Failed to allocate task: %v\n", err)
		return
	}
	fmt.Printf("Successfully allocated task '%s' with stack size %d bytes\n", tcb.Name, len(stack))

	// Free the task
	allocator.FreeTask(tcb, stack)
	fmt.Println("Successfully freed task and returned resources to pools.")
}
