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

package runningrequests

import (
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// Default configuration values.
const (
	// DefaultMaxConcurrencyPerPod is a safe baseline for many LLM serving engines.
	DefaultMaxConcurrencyPerPod int64 = 100

	// DefaultCacheTTL is the default duration for caching the computed saturation value.
	DefaultCacheTTL time.Duration = 100 * time.Millisecond
)

// apiConfig represents the external configuration schema for the running-requests detector.
// It is designed to be deserialized from JSON via the plugin's raw parameters.
type apiConfig struct {
	// MaxConcurrencyPerPod is the ideal per-pod request capacity.
	//
	//   Saturation = totalRunningRequests / (readyPods * MaxConcurrencyPerPod)
	//
	// Defaults to 100 if unset.
	MaxConcurrencyPerPod *int64 `json:"maxConcurrencyPerPod,omitempty"`

	// CacheTTL bounds how long a computed saturation value is reused before re-gathering
	// metrics. This avoids calling Gather() on the full Prometheus registry on every request.
	//
	// Defaults to 100ms if unset.
	CacheTTL *metav1.Duration `json:"cacheTTL,omitempty"`
}

// Config is the validated configuration used by the running-requests detector.
type Config struct {
	MaxConcurrencyPerPod int64
	CacheTTL             time.Duration
}

func buildConfig(apiCfg *apiConfig) (*Config, error) {
	cfg := &Config{
		MaxConcurrencyPerPod: DefaultMaxConcurrencyPerPod,
		CacheTTL:             DefaultCacheTTL,
	}

	if apiCfg == nil {
		return cfg, nil
	}

	if v := ptr.Deref(apiCfg.MaxConcurrencyPerPod, DefaultMaxConcurrencyPerPod); v > 0 {
		cfg.MaxConcurrencyPerPod = v
	} else if apiCfg.MaxConcurrencyPerPod != nil {
		return nil, errors.New("maxConcurrencyPerPod must be > 0")
	}

	if apiCfg.CacheTTL != nil {
		if apiCfg.CacheTTL.Duration < 0 {
			return nil, fmt.Errorf("cacheTTL must be >= 0, got %s", apiCfg.CacheTTL.Duration)
		}
		cfg.CacheTTL = apiCfg.CacheTTL.Duration
	}

	return cfg, nil
}
