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

import "time"

// Config holds the configuration for the RunningRequests-based SaturationDetector.
type Config struct {
	// MaxConcurrencyPerPod is the ideal request capacity per pod.
	// Saturation = totalRunningRequests / (readyPods * MaxConcurrencyPerPod)
	MaxConcurrencyPerPod int64

	// CacheTTL is how long a computed saturation value is reused before re-gathering metrics.
	// This avoids calling Gather() on the full Prometheus registry for every request.
	CacheTTL time.Duration
}

const (
	// DefaultMaxConcurrencyPerPod is a safe baseline for many LLM serving engines.
	DefaultMaxConcurrencyPerPod int64 = 100

	// DefaultCacheTTL is the default duration for caching the computed saturation value.
	DefaultCacheTTL = 100 * time.Millisecond
)
