/*
Copyright 2025 The Kubernetes Authors.

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

package runningrequestsdetector

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	backendmetrics "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/backend/metrics"
)

// --- Detector Tests ---

func TestDetector_Saturation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		running        float64
		readyPods      float64
		maxConcurrency int64
		wantSaturation float64
	}{
		{
			name:           "empty pool, fail-closed",
			running:        0,
			readyPods:      0,
			maxConcurrency: 100,
			wantSaturation: 1.0,
		},
		{
			name:           "no load",
			running:        0,
			readyPods:      2,
			maxConcurrency: 100,
			wantSaturation: 0.0,
		},
		{
			name:           "half load",
			running:        100,
			readyPods:      2,
			maxConcurrency: 100,
			wantSaturation: 0.5,
		},
		{
			name:           "full load",
			running:        200,
			readyPods:      2,
			maxConcurrency: 100,
			wantSaturation: 1.0,
		},
		{
			name:           "overloaded",
			running:        300,
			readyPods:      2,
			maxConcurrency: 100,
			wantSaturation: 1.5,
		},
		{
			name:           "single pod partial",
			running:        30,
			readyPods:      1,
			maxConcurrency: 50,
			wantSaturation: 0.6,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			runningGauge, readyPodsGauge, registry := newTestGauges()
			runningGauge.WithLabelValues("model-a").Set(tc.running)
			readyPodsGauge.WithLabelValues("pool-1").Set(tc.readyPods)

			detector := NewDetector(
				&Config{MaxConcurrencyPerPod: tc.maxConcurrency},
				registry, logr.Discard(),
			)

			got := detector.Saturation(context.Background(), []backendmetrics.PodMetrics{})
			require.InDelta(t, tc.wantSaturation, got, 1e-6, "Saturation mismatch")
		})
	}
}

func TestDetector_Saturation_NilGatherer(t *testing.T) {
	t.Parallel()
	detector := NewDetector(&Config{MaxConcurrencyPerPod: 100}, nil, logr.Discard())
	got := detector.Saturation(context.Background(), nil)
	require.InDelta(t, 1.0, got, 1e-6, "nil gatherer should fail-closed")
}

func TestDetector_Saturation_MultipleModels(t *testing.T) {
	t.Parallel()

	runningGauge, readyPodsGauge, registry := newTestGauges()
	runningGauge.WithLabelValues("model-a").Set(50)
	runningGauge.WithLabelValues("model-b").Set(30)
	readyPodsGauge.WithLabelValues("pool-1").Set(2)

	detector := NewDetector(&Config{MaxConcurrencyPerPod: 100}, registry, logr.Discard())
	got := detector.Saturation(context.Background(), nil)
	// total running = 80, total capacity = 2 * 100 = 200
	require.InDelta(t, 0.4, got, 1e-6)
}

// --- CachedDetector Tests ---

func TestCachedDetector_CacheHit(t *testing.T) {
	t.Parallel()

	runningGauge, readyPodsGauge, registry := newTestGauges()
	runningGauge.WithLabelValues("model-a").Set(100)
	readyPodsGauge.WithLabelValues("pool-1").Set(2)

	detector := NewDetector(&Config{MaxConcurrencyPerPod: 100}, registry, logr.Discard())
	cached := NewCachedDetector(detector, 500*time.Millisecond, logr.Discard())

	// First call: computes and caches
	got1 := cached.Saturation(context.Background(), nil)
	require.InDelta(t, 0.5, got1, 1e-6)

	// Change underlying metrics
	runningGauge.WithLabelValues("model-a").Set(200)

	// Second call within TTL: should return cached value (0.5), not new value (1.0)
	got2 := cached.Saturation(context.Background(), nil)
	require.InDelta(t, 0.5, got2, 1e-6, "Expected cached value within TTL")
}

func TestCachedDetector_CacheExpiry(t *testing.T) {
	t.Parallel()

	runningGauge, readyPodsGauge, registry := newTestGauges()
	runningGauge.WithLabelValues("model-a").Set(100)
	readyPodsGauge.WithLabelValues("pool-1").Set(2)

	detector := NewDetector(&Config{MaxConcurrencyPerPod: 100}, registry, logr.Discard())
	cached := NewCachedDetector(detector, 10*time.Millisecond, logr.Discard())

	// First call
	got1 := cached.Saturation(context.Background(), nil)
	require.InDelta(t, 0.5, got1, 1e-6)

	// Change underlying metrics
	runningGauge.WithLabelValues("model-a").Set(200)

	// Wait for cache to expire
	time.Sleep(20 * time.Millisecond)

	// Should return new value
	got2 := cached.Saturation(context.Background(), nil)
	require.InDelta(t, 1.0, got2, 1e-6, "Expected fresh value after TTL expiry")
}

func TestCachedDetector_Concurrency(t *testing.T) {
	t.Parallel()

	runningGauge, readyPodsGauge, registry := newTestGauges()
	runningGauge.WithLabelValues("model-a").Set(100)
	readyPodsGauge.WithLabelValues("pool-1").Set(2)

	detector := NewDetector(&Config{MaxConcurrencyPerPod: 100}, registry, logr.Discard())
	cached := NewCachedDetector(detector, 50*time.Millisecond, logr.Discard())

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := cached.Saturation(context.Background(), nil)
			assert.InDelta(t, 0.5, got, 1e-6)
		}()
	}
	wg.Wait()
}

// --- Helpers ---

func newTestGauges() (running, readyPods *prometheus.GaugeVec, registry *prometheus.Registry) {
	registry = prometheus.NewRegistry()
	running = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: runningRequestsMetricName},
		[]string{"model_name"},
	)
	readyPods = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: readyPodsMetricName},
		[]string{"name"},
	)
	registry.MustRegister(running, readyPods)
	return
}
