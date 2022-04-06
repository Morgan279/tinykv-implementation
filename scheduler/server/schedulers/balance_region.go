// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	allStores := cluster.GetStores()
	var stores []*core.StoreInfo

	for _, store := range allStores {
		if !store.IsUp() || store.DownTime() > cluster.GetMaxStoreDownTime() {
			continue
		}
		stores = append(stores, store)
	}

	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})

	for _, source := range stores {
		for i := 0; i < balanceRegionRetryLimit; i++ {
			region := pickRegionByOrder(cluster, source.GetID())
			if region != nil && len(region.GetPeers()) == cluster.GetMaxReplicas() {
				oldPeer := region.GetStorePeer(source.GetID())
				if op := s.generateMovePeerOp(cluster, region, oldPeer); op != nil {
					return op
				}
			}
		}
	}
	return nil
}

func pickRegionByOrder(cluster opt.Cluster, sourceID uint64) *core.RegionInfo {
	var region *core.RegionInfo
	cluster.GetPendingRegionsWithLock(sourceID, func(regions core.RegionsContainer) {
		region = regions.RandomRegion(nil, nil)
	})
	if region == nil {
		cluster.GetFollowersWithLock(sourceID, func(regions core.RegionsContainer) {
			region = regions.RandomRegion(nil, nil)
		})
	}
	if region == nil {
		cluster.GetLeadersWithLock(sourceID, func(regions core.RegionsContainer) {
			region = regions.RandomRegion(nil, nil)
		})
	}
	return region
}

func (s *balanceRegionScheduler) generateMovePeerOp(cluster opt.Cluster, region *core.RegionInfo, oldPeer *metapb.Peer) *operator.Operator {
	srcStoreID := oldPeer.GetStoreId()
	source := cluster.GetStore(srcStoreID)
	distStore := pickDistStore(cluster, region)
	if distStore == nil {
		return nil
	}

	if int64(source.GetRegionSize()-distStore.GetRegionSize()) > 2*region.GetApproximateSize() {
		newPeer, err := cluster.AllocPeer(distStore.GetID())
		if err != nil {
			return nil
		}
		desc := fmt.Sprintf("move region from store %d to store %d", srcStoreID, distStore.GetID())
		op, err := operator.CreateMovePeerOperator(desc, cluster, region, operator.OpBalance, oldPeer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
		if err != nil {
			return nil
		}
		return op
	}

	return nil
}

func pickDistStore(cluster opt.Cluster, region *core.RegionInfo) *core.StoreInfo {
	var dist *core.StoreInfo
	for _, store := range cluster.GetStores() {
		if _, ok := region.GetStoreIds()[store.GetID()]; !ok && store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			dist = maybeBetterDist(dist, store)
		}

	}
	return dist
}

func maybeBetterDist(best *core.StoreInfo, candidate *core.StoreInfo) *core.StoreInfo {
	if best == nil || candidate.GetRegionSize() < best.GetRegionSize() {
		return candidate
	}
	return best
}
