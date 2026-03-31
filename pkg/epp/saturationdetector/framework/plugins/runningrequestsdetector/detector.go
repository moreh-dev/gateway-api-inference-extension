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

// Package runningrequestsdetector implements a saturation detector that reads EPP-exported
// Prometheus Gauge metrics (inference_objective_running_requests and inference_pool_ready_pods)
// via a prometheus.Gatherer to determine pool saturation.
//
// # Saturation Logic
//
//	Saturation = TotalRunningRequests / (ReadyPods * MaxConcurrencyPerPod)
//
// A value >= 1.0 means the pool is fully saturated.
//
// # Caching
//
// CachedDetector is a decorator that caches the computed saturation value for a configurable TTL.
// Calling Gather() on the full Prometheus registry is expensive because it collects all registered
// metrics. Within the TTL window, concurrent callers receive the cached value without triggering
// a new Gather().
package runningrequestsdetector

import (
	"context"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"

	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/common/util/logging"
	backendmetrics "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/backend/metrics"
)

const (
	runningRequestsMetricName = "inference_objective_running_requests"
	readyPodsMetricName       = "inference_pool_ready_pods"

	loggerName = "RunningRequestsSaturationDetector"
)

// --- Detector ---

// Detector determines system saturation by reading EPP-exported Gauge metrics via a Gatherer.
type Detector struct {
	config   *Config
	gatherer prometheus.Gatherer
}

// NewDetector creates a new RunningRequests-based SaturationDetector.
func NewDetector(config *Config, gatherer prometheus.Gatherer, logger logr.Logger) *Detector {
	logger.WithName(loggerName).V(logutil.DEFAULT).Info("Creating new RunningRequestsSaturationDetector",
		"maxConcurrencyPerPod", config.MaxConcurrencyPerPod)

	return &Detector{config: config, gatherer: gatherer}
}

// Saturation calculates saturation from EPP-tracked running requests.
//
//	Saturation = TotalRunningRequests / (ReadyPods * MaxConcurrencyPerPod)
func (d *Detector) Saturation(_ context.Context, _ []backendmetrics.PodMetrics) float64 {
	if d.gatherer == nil {
		return 1.0
	}
	families, err := d.gatherer.Gather()
	if err != nil {
		return 1.0
	}

	var totalRunning float64
	var readyPods float64

	for _, mf := range families {
		switch mf.GetName() {
		case runningRequestsMetricName:
			for _, m := range mf.GetMetric() {
				if g := m.GetGauge(); g != nil {
					totalRunning += g.GetValue()
				}
			}
		case readyPodsMetricName:
			for _, m := range mf.GetMetric() {
				if g := m.GetGauge(); g != nil {
					readyPods += g.GetValue()
				}
			}
		}
	}

	totalCapacity := readyPods * float64(d.config.MaxConcurrencyPerPod)
	if totalCapacity == 0 {
		return 1.0
	}
	return totalRunning / totalCapacity
}

// --- CachedDetector ---

// cachedSaturation holds a computed saturation value with its expiry time.
type cachedSaturation struct {
	value  float64
	expiry time.Time
}

// CachedDetector is a decorator that caches results from a delegate Detector to avoid expensive
// full-registry Gather() calls on every request.
type CachedDetector struct {
	delegate *Detector
	ttl      time.Duration

	mu    sync.RWMutex
	cache cachedSaturation
}

// NewCachedDetector creates a new CachedDetector wrapping the given delegate.
func NewCachedDetector(delegate *Detector, ttl time.Duration, logger logr.Logger) *CachedDetector {
	logger.WithName(loggerName).V(logutil.DEFAULT).Info("Creating new CachedDetector",
		"cacheTTL", ttl)

	return &CachedDetector{delegate: delegate, ttl: ttl}
}

// Saturation returns the cached saturation value if still valid, otherwise recomputes it.
// Uses a read-lock fast path and a write-lock slow path with double-check to prevent thundering herd.
func (cd *CachedDetector) Saturation(ctx context.Context, pods []backendmetrics.PodMetrics) float64 {
	// Fast path: read lock
	cd.mu.RLock()
	if time.Now().Before(cd.cache.expiry) {
		val := cd.cache.value
		cd.mu.RUnlock()
		return val
	}
	cd.mu.RUnlock()

	// Slow path: write lock + double-check
	cd.mu.Lock()
	defer cd.mu.Unlock()

	if time.Now().Before(cd.cache.expiry) {
		return cd.cache.value
	}

	saturation := cd.delegate.Saturation(ctx, pods)
	cd.cache = cachedSaturation{
		value:  saturation,
		expiry: time.Now().Add(cd.ttl),
	}
	return saturation
}
