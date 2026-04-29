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

// Package runningrequests implements a saturation detector that reads EPP-exported
// Prometheus Gauge metrics (inference_objective_running_requests and
// inference_pool_ready_pods) via a prometheus.Gatherer to determine pool saturation.
//
// # Saturation Logic
//
//	Saturation = TotalRunningRequests / (ReadyPods * MaxConcurrencyPerPod)
//
// A value >= 1.0 means the pool is fully saturated.
//
// # Caching
//
// The detector caches the computed saturation value for a configurable TTL
// (see Config.CacheTTL). Calling Gather() on the full Prometheus registry is
// expensive because it collects all registered metrics. Within the TTL window,
// concurrent callers receive the cached value without triggering a new Gather().
package runningrequests

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/log"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/common/observability/logging"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/flowcontrol"
	fwkplugin "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
)

const (
	// RunningRequestsDetectorType is the unique identifier for this plugin.
	RunningRequestsDetectorType = "running-requests-detector"

	runningRequestsMetricName = "inference_objective_running_requests"
	readyPodsMetricName       = "inference_pool_ready_pods"
)

// RunningRequestsDetectorFactory instantiates the detector plugin using the provided JSON parameters.
// It reads gauges from the controller-runtime metrics registry.
func RunningRequestsDetectorFactory(
	name string,
	params json.RawMessage,
	handle fwkplugin.Handle,
) (fwkplugin.Plugin, error) {
	var apiCfg apiConfig
	if len(params) > 0 {
		if err := json.Unmarshal(params, &apiCfg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal running-requests detector config: %w", err)
		}
	}
	cfg, err := buildConfig(&apiCfg)
	if err != nil {
		return nil, err
	}
	return NewDetector(name, *cfg, ctrlmetrics.Registry, log.FromContext(handle.Context())), nil
}

var (
	_ flowcontrol.SaturationDetector = &Detector{}
	_ fwkplugin.Plugin               = &Detector{}
)

// Detector determines pool saturation by reading EPP-exported Gauge metrics via a Gatherer.
// The computed value is cached for Config.CacheTTL to avoid re-gathering on every request.
type Detector struct {
	config    Config
	gatherer  prometheus.Gatherer
	typedName fwkplugin.TypedName

	mu    sync.RWMutex
	cache cachedSaturation
}

// cachedSaturation holds a computed saturation value with its expiry time.
type cachedSaturation struct {
	value  float64
	expiry time.Time
}

// NewDetector creates a new instance of the RunningRequests saturation detector.
func NewDetector(name string, cfg Config, gatherer prometheus.Gatherer, logger logr.Logger) *Detector {
	typedName := fwkplugin.TypedName{
		Type: RunningRequestsDetectorType,
		Name: name,
	}

	pluginLogger := logger.WithName(typedName.String())
	pluginLogger.V(logutil.DEFAULT).Info("Creating new RunningRequestsDetector",
		"maxConcurrencyPerPod", cfg.MaxConcurrencyPerPod,
		"cacheTTL", cfg.CacheTTL.String())

	return &Detector{
		config:    cfg,
		gatherer:  gatherer,
		typedName: typedName,
	}
}

// TypedName returns the type and name tuple of this plugin instance.
func (d *Detector) TypedName() fwkplugin.TypedName {
	return d.typedName
}

// Saturation returns the cached saturation value if still valid, otherwise recomputes it from
// the Prometheus registry. Uses a read-lock fast path and a write-lock slow path with
// double-check to prevent thundering herd.
func (d *Detector) Saturation(_ context.Context, _ []datalayer.Endpoint) float64 {
	// Fast path: read lock.
	d.mu.RLock()
	if time.Now().Before(d.cache.expiry) {
		val := d.cache.value
		d.mu.RUnlock()
		return val
	}
	d.mu.RUnlock()

	// Slow path: write lock + double-check.
	d.mu.Lock()
	defer d.mu.Unlock()

	if time.Now().Before(d.cache.expiry) {
		return d.cache.value
	}

	saturation := d.compute()
	d.cache = cachedSaturation{
		value:  saturation,
		expiry: time.Now().Add(d.config.CacheTTL),
	}
	return saturation
}

// compute calculates saturation from EPP-tracked running requests.
//
//	Saturation = TotalRunningRequests / (ReadyPods * MaxConcurrencyPerPod)
func (d *Detector) compute() float64 {
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
