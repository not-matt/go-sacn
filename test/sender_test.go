package stress

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sacn "gitlab.com/patopest/go-sacn"
	"gitlab.com/patopest/go-sacn/packet"
)

func TestSenderStress(t *testing.T) {
	sender, err := sacn.NewSender("127.0.0.1", &sacn.SenderOptions{})
	if err != nil {
		t.Fatalf("NewSender error: %v", err)
	}
	defer sender.Close()

	ch, err := sender.StartUniverse(1)
	if err != nil {
		t.Fatalf("StartUniverse error: %v", err)
	}
	if err := sender.AddDestination(1, "127.0.0.1"); err != nil {
		t.Fatalf("AddDestination error: %v", err)
	}

	// 100 writers -> 1000 writers hit same throughput. This indicates that the bottleneck is in the sender internals / network stack, and the system is stable under high concurrency.
	const writers = 1000
	const metricsInterval = 500 * time.Millisecond
	const testDuration = 10 * time.Second

	var sent atomic.Uint64
	var sendErr atomic.Uint64
	var blocked atomic.Uint64
	var attempted atomic.Uint64

	errCh := make(chan error, 1024)
	stop := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}

				attempted.Add(1)
				p := packet.NewDataPacket()
				p.SetData([]byte{byte(seed), byte(time.Now().UnixNano())})
				if seed%2 == 0 {
					if err := sender.Send(1, p); err != nil {
						sendErr.Add(1)
						select {
						case errCh <- err:
						case <-stop:
							return
						}
						continue
					}
					sent.Add(1)
					continue
				}

				select {
				case ch <- p:
					sent.Add(1)
				case <-time.After(1 * time.Millisecond):
					blocked.Add(1)
					select {
					case errCh <- errors.New("send channel blocked"):
					case <-stop:
						return
					}
				case <-stop:
					return
				}
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = sender.SetMulticast(1, time.Now().UnixNano()%2 == 0)
			_ = sender.SetDestinations(1, []string{"127.0.0.1"})
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(metricsInterval)
		defer ticker.Stop()
		var last uint64
		var lastAttempted uint64
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				current := sent.Load()
				currentAttempted := attempted.Load()
				delta := current - last
				deltaAttempted := currentAttempted - lastAttempted
				last = current
				lastAttempted = currentAttempted

				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				throughput := (delta * uint64(time.Second)) / uint64(metricsInterval)
				attemptRate := (deltaAttempted * uint64(time.Second)) / uint64(metricsInterval)
				successRate := uint64(0)
				if deltaAttempted > 0 {
					successRate = (delta * 100) / deltaAttempted
				}
				t.Logf("throughput=%d/s attempts=%d/s success=%d%% total=%d sendErr=%d blocked=%d goroutines=%d heap=%dKB",
					throughput,
					attemptRate,
					successRate,
					current,
					sendErr.Load(),
					blocked.Load(),
					runtime.NumGoroutine(),
					mem.HeapAlloc/1024,
				)
			}
		}
	}()

	time.Sleep(testDuration)
	close(stop)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		// Blocked sends are expected under high load and not a test failure
		if err != nil && err.Error() != "send channel blocked" {
			t.Fatalf("stress error: %v", err)
		}
	}

	if err := sender.StopUniverse(1); err != nil {
		t.Fatalf("StopUniverse error: %v", err)
	}
	if err := sender.Send(1, packet.NewDataPacket()); err == nil {
		t.Fatalf("expected error after StopUniverse")
	}
}
