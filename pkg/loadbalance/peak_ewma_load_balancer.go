/*
 * Copyright 2024 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package loadbalance

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/gopkg/lang/fastrand"
	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/klog"
	"golang.org/x/sync/singleflight"
)

type ewmaInstance struct {
	discovery.Instance
	mu         sync.Mutex
	alpha      float64
	activeReqs int64
	lastTick   time.Time
	latency    float64
	errs       float64
	total      float64
}

func (e *ewmaInstance) Update(_ context.Context, duration time.Duration, value interface{}) {
	err := 0
	if value != nil {
		err = 1
	}

	atomic.AddInt64(&e.activeReqs, -1)
	now := time.Now()
	d := float64(now.Sub(e.lastTick)) / float64(time.Second)

	e.mu.Lock()
	defer e.mu.Unlock()
	e.lastTick = now
	e.latency = ewma(e.alpha, e.latency, float64(duration.Milliseconds()), d)
	e.errs = ewma(e.alpha, e.errs, float64(err), d)
	e.total = ewma(e.alpha, e.total, float64(1), d)
}

const defaultEWMALatency = 1000

func (e *ewmaInstance) score() float64 {
	activeReqs := float64(atomic.LoadInt64(&e.activeReqs))

	e.mu.Lock()
	latency := e.latency
	errs := e.errs
	total := e.total
	e.mu.Unlock()

	if latency == 0 {
		latency = defaultEWMALatency
	}
	successRate := 1 - errs/(total+1)

	return activeReqs * latency / successRate
}

func ewma(alpha, v, i float64, d float64) float64 {
	return i*alpha + math.Pow(1-alpha, d)*v
}

type PeakEWMAPicker struct {
	instances []*ewmaInstance
}

func (e *PeakEWMAPicker) Next(_ context.Context, _ interface{}) discovery.Instance {
	if len(e.instances) == 0 {
		return nil
	}
	a := e.instances[fastrand.Int()%len(e.instances)]
	b := e.instances[fastrand.Int()%len(e.instances)]
	if a.score() < b.score() {
		return a
	}
	return b
}

func newPeakEWMAPicker(instances []discovery.Instance) Picker {
	newInstances := make([]*ewmaInstance, len(instances))
	for i, ins := range instances {
		newInstances[i] = &ewmaInstance{Instance: ins, alpha: weightedAlpha(float64(ins.Weight()))}
	}
	return &PeakEWMAPicker{instances: newInstances}
}

func weightedAlpha(w float64) float64 {
	return 1 - math.Exp(-5.0/60.0/w) // for a W minute moving average.
}

type peakEWMABalancer struct {
	pickerCache sync.Map
	sfg         singleflight.Group
}

func NewPeakEWMABalancer() Loadbalancer {
	return &peakEWMABalancer{}
}

func (b *peakEWMABalancer) GetPicker(e discovery.Result) Picker {
	if !e.Cacheable {
		picker := b.createPicker(e)
		return picker
	}

	picker, ok := b.pickerCache.Load(e.CacheKey)
	if !ok {
		picker, _, _ = b.sfg.Do(e.CacheKey, func() (interface{}, error) {
			p := b.createPicker(e)
			b.pickerCache.Store(e.CacheKey, p)
			return p, nil
		})
	}
	return picker.(Picker)
}

func (b *peakEWMABalancer) createPicker(e discovery.Result) Picker {
	instances := make([]discovery.Instance, 0, len(e.Instances))
	for _, ins := range instances {
		w := ins.Weight()
		if w <= 0 {
			klog.Warnf("KITEX: invalid weight, weight=%d instance=%s", w, ins.Address())
			continue
		}
		instances = append(instances, ins)
	}
	return newPeakEWMAPicker(instances)
}

func (b *peakEWMABalancer) Rebalance(change discovery.Change) {
	if !change.Result.Cacheable {
		return
	}
	b.pickerCache.Store(change.Result.CacheKey, b.createPicker(change.Result))
}

func (b *peakEWMABalancer) Delete(change discovery.Change) {
	if !change.Result.Cacheable {
		return
	}
	b.pickerCache.Delete(change.Result.CacheKey)
}

func (b *peakEWMABalancer) Name() string {
	return "peak_ewma"
}
