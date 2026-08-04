// Copyright 2025 The Tessera authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tessera

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/mod/sumdb/note"
)

func TestMemoize(t *testing.T) {
	// Set up an AddFn which will increment a counter every time it's called, and return that in the Index.
	i := uint64(0)
	deleg := func() (Index, error) {
		i++
		return Index{
			Index: i,
		}, nil
	}
	add := func(_ context.Context, _ *Entry) IndexFuture {
		return deleg
	}

	// Create a single future (for a single Entry), and convince ourselves that the counter is being incremented
	// each time the future is being invoked.
	f1 := add(nil, nil)
	a, _ := f1()
	b, _ := f1()
	if a.Index == b.Index {
		t.Fatalf("a(=%d) == b(=%d)", a.Index, b.Index)
	}

	// Now create an AddFn which memoizes the result of the delegate, like we do in NewAppender, and assert that
	// repeated calls to the future work as expected; only incrementing the counter once.
	add = func(_ context.Context, _ *Entry) IndexFuture {
		return memoizeFuture(deleg)
	}
	f2 := add(nil, nil)
	c, _ := f2()
	d, _ := f2()

	if c.Index != d.Index {
		t.Fatalf("c(=%d) != d(=%d)", c.Index, d.Index)
	}
}

const testSignerKey = "PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg"

func TestAppendOptionsValid(t *testing.T) {
	for _, test := range []struct {
		name            string
		opts            *AppendOptions
		wantErrContains string
	}{
		{
			name: "Valid",
			opts: NewAppendOptions().WithCheckpointSigner(mustCreateSigner(t, testSignerKey)),
		}, {
			name: "Valid: CheckpointRepublishInterval == CheckpointInterval",
			opts: NewAppendOptions().
				WithCheckpointSigner(mustCreateSigner(t, testSignerKey)).
				WithCheckpointInterval(10 * time.Second).
				WithCheckpointRepublishInterval(10 * time.Second),
		}, {
			name: "Error: CheckpointRepublishInterval < CheckpointInterval",
			opts: NewAppendOptions().
				WithCheckpointSigner(mustCreateSigner(t, testSignerKey)).
				WithCheckpointInterval(10 * time.Second).
				WithCheckpointRepublishInterval(9 * time.Second),
			wantErrContains: "WithCheckpointRepublishInterval",
		}, {
			name:            "Error: No CheckpointSigner",
			opts:            NewAppendOptions(),
			wantErrContains: "WithCheckpointSigner",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.opts.valid()
			switch gotErr, wantErr := err != nil, test.wantErrContains != ""; {
			case gotErr && !wantErr:
				t.Fatalf("Got unexpected error %q, want no error", err)
			case !gotErr && wantErr:
				t.Fatalf("Got no error, expected error")
			case gotErr:
				if !strings.Contains(err.Error(), test.wantErrContains) {
					t.Fatalf("Got err %q, want error containing %q", err.Error(), test.wantErrContains)
				}
			}
		})
	}
}

func TestMaxEntrySize(t *testing.T) {
	d := func(_ context.Context, e *Entry) IndexFuture {
		return func() (Index, error) {
			return Index{}, nil
		}
	}

	const limit = 128
	add := entrySizeLimitDecorator(d, limit)

	for _, test := range []struct {
		name    string
		size    uint
		wantErr bool
	}{
		{
			name: "< limit",
			size: limit - 1,
		}, {
			name: "== limit",
			size: limit,
		}, {
			name:    "> limit",
			size:    limit + 1,
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := add(t.Context(), NewEntry(make([]byte, test.size)))()
			if gotErr := err != nil; gotErr != test.wantErr {
				t.Fatalf("Got err %q, want err? %T", err, test.wantErr)
			}
		})
	}
}

func TestAppendOptionsStatsInterval(t *testing.T) {
	for _, test := range []struct {
		name string
		opts *AppendOptions
		want time.Duration
	}{
		{
			name: "Default",
			opts: NewAppendOptions(),
			want: DefaultStatsInterval,
		}, {
			name: "Explicit",
			opts: NewAppendOptions().WithStatsInterval(30 * time.Second),
			want: 30 * time.Second,
		}, {
			name: "Zero disables",
			opts: NewAppendOptions().WithStatsInterval(0),
			want: 0,
		}, {
			name: "Negative disables",
			opts: NewAppendOptions().WithStatsInterval(-time.Second),
			want: -time.Second,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.opts.statsInterval; got != test.want {
				t.Fatalf("Got statsInterval %v, want %v", got, test.want)
			}
		})
	}
}

// countingLogReader records the number of polling calls updateStats makes, and signals the
// first IntegratedSize call on polled.
type countingLogReader struct {
	integratedSize atomic.Int64
	nextIndex      atomic.Int64
	polled         chan struct{}
}

func (r *countingLogReader) IntegratedSize(_ context.Context) (uint64, error) {
	r.integratedSize.Add(1)
	select {
	case r.polled <- struct{}{}:
	default:
	}
	return 0, nil
}

func (r *countingLogReader) NextIndex(_ context.Context) (uint64, error) {
	r.nextIndex.Add(1)
	return 0, nil
}

func (r *countingLogReader) ReadCheckpoint(_ context.Context) ([]byte, error) {
	return nil, os.ErrNotExist
}

func (r *countingLogReader) ReadTile(_ context.Context, _, _ uint64, _ uint8) ([]byte, error) {
	return nil, os.ErrNotExist
}

func (r *countingLogReader) ReadEntryBundle(_ context.Context, _ uint64, _ uint8) ([]byte, error) {
	return nil, os.ErrNotExist
}

func TestUpdateStatsPolls(t *testing.T) {
	r := &countingLogReader{polled: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan struct{})
	go func() {
		(&integrationStats{}).updateStats(ctx, r, time.Millisecond)
		close(done)
	}()

	select {
	case <-r.polled:
	case <-time.After(30 * time.Second):
		t.Fatal("Timed out waiting for updateStats to poll the log reader")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("updateStats did not return after its context was cancelled")
	}

	if got := r.integratedSize.Load(); got == 0 {
		t.Error("Got 0 IntegratedSize calls, want at least one")
	}
	if got := r.nextIndex.Load(); got == 0 {
		t.Error("Got 0 NextIndex calls, want at least one")
	}
}

// TestRegressionUpdateStatsHonoursInterval pins the poll period to the configured interval.
// A hardcoded period polls repeatedly inside the window this leaves silent.
func TestRegressionUpdateStatsHonoursInterval(t *testing.T) {
	r := &countingLogReader{polled: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go (&integrationStats{}).updateStats(ctx, r, time.Hour)

	select {
	case <-r.polled:
		t.Fatal("Got a poll before the configured interval elapsed, want none")
	case <-time.After(500 * time.Millisecond):
	}

	if got := r.integratedSize.Load(); got != 0 {
		t.Errorf("Got %d IntegratedSize calls before the interval elapsed, want 0", got)
	}
}

func TestUpdateStatsNilLogReader(t *testing.T) {
	done := make(chan struct{})
	go func() {
		(&integrationStats{}).updateStats(t.Context(), nil, time.Millisecond)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("updateStats did not return for a nil LogReader")
	}
}

func mustCreateSigner(t *testing.T, k string) note.Signer {
	t.Helper()
	s, err := note.NewSigner(k)
	if err != nil {
		t.Fatalf("Failed to create signer: %v", err)
	}
	return s
}
