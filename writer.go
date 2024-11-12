package ringslice

import (
	"errors"
	"io"
	"sync"
)

// Writer is the main data container.
type Writer[T any] struct {
	data  []T
	size  int64
	wPos  int64 // write pos
	cycle int64

	closed bool
	mutex  sync.RWMutex
	cond   *sync.Cond
	wg     sync.WaitGroup
}

func New[T any](size int64) (*Writer[T], error) {
	if size <= 0 {
		return nil, errors.New("Size must be positive")
	}

	w := &Writer[T]{
		data: make([]T, size),
		size: size,
	}
	w.cond = sync.NewCond(w.mutex.RLocker())

	return w, nil
}

// Reader returns a new reader positioned at the buffer's oldest available
// position. The reader can be moved to the most recent position by calling
// its Reset() method.
//
// If there isn't enough data to read, the reader's read method will return
// error io.EOF. If you need Read() to not return until new data is available,
// use BlockingReader()
func (w *Writer[T]) Reader() *Reader[T] {
	if w.closed {
		return nil
	}

	cycle := w.cycle
	pos := w.wPos
	// rewind
	if cycle > 0 {
		cycle = cycle - 1
	} else {
		pos = 0
	}

	w.wg.Add(1)

	return &Reader[T]{
		w:      w,
		block:  false,
		cycle:  cycle,
		rPos:   pos,
		closed: new(uint64),
	}
}

// BlockingReader returns a new reader positioned at the buffer's oldest
// available position which reads will block if no new data is available.
func (w *Writer[T]) BlockingReader() *Reader[T] {
	if w.closed {
		return nil
	}

	cycle := w.cycle
	pos := w.wPos
	// rewind
	if cycle > 0 {
		cycle = cycle - 1
	} else {
		pos = 0
	}

	w.wg.Add(1)

	return &Reader[T]{
		w:      w,
		block:  true,
		cycle:  cycle,
		rPos:   pos,
		closed: new(uint64),
	}
}

// BlockingCurrentReader returns a new reader positionned at the buffer's
// edge.
func (w *Writer[T]) BlockingCurrentReader() *Reader[T] {
	if w.closed {
		return nil
	}

	cycle := w.cycle
	pos := w.wPos

	w.wg.Add(1)

	return &Reader[T]{
		w:      w,
		block:  true,
		cycle:  cycle,
		rPos:   pos,
		closed: new(uint64),
	}
}

// Append values to the slice
func (w *Writer[T]) Append(values ...T) (int, error) {
	return w.Write(values)
}

func (w *Writer[T]) Write(values []T) (int, error) {
	n := int64(len(values))

	// lock buffer while writing
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return 0, io.ErrClosedPipe
	}

	if n > w.size {
		// volume of written data is larger than our buffer (NOTE: will invalidate ALL existing readers)
		cnt := n / w.size
		w.cycle += cnt - 1
		w.wPos += n % w.size
		// only use relevant part of buf
		values = values[n-w.size:]
	}

	// copy
	remain := w.size - w.wPos
	copy(w.data[w.wPos:], values)
	if int64(len(values)) > remain {
		copy(w.data, values[remain:])
		w.cycle += 1
	} else if int64(len(values)) == remain {
		w.cycle += 1
	}

	// update cursor position
	w.wPos = ((w.wPos + int64(len(values))) % w.size)

	// wake readers
	w.cond.Broadcast()
	return int(n), nil
}

func (w *Writer[T]) Size() int64 {
	return w.size
}

func (w *Writer[T]) TotalWritten() int64 {
	return w.cycle*w.size + w.wPos
}

// Close will cause all readers to return EOF once they have read the whole
// buffer and will wait until all readers have called Close(). If you do not
// need EOF synchronization you can ignore the whole close system as it is not
// used to free any resources, but if you use ringbuf as an output buffer, for
// example, it will enable waiting for writes to have completed prior to
// ending the program.
//
// Note that if any reader failed to call close prior to end and being freed
// this may cause a deadlock. Use CloseNow() to avoid this.
func (w *Writer[T]) Close() error {
	w.mutex.Lock()
	if w.closed {
		w.mutex.Unlock()
		// calling close multiple times isn't an error
		return nil
	}
	w.closed = true

	// wake all readers (they will really start moving after the unlock)
	w.cond.Broadcast()

	w.mutex.Unlock()

	// wait for everyone to complete
	w.wg.Wait()
	return nil
}
