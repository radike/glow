package scheduler

import (
	"github.com/chrislusf/glow/driver/plan"
	"github.com/chrislusf/glow/driver/scheduler/market"
	"github.com/chrislusf/glow/resource"
)

func (s *Scheduler) Score(r market.Requirement, bid float64, obj market.Object) float64 {
	al := obj.(resource.Allocation)
	tg, loc := r.(*plan.TaskGroup), al.Location
	firstTask := tg.Tasks[0]
	cost := float64(1)
	for _, input := range firstTask.Inputs {
		dataLocation, found := s.shardLocator.GetShardLocation(s.Option.ExecutableFileHash + "-" + input.Name())
		if !found {
			// log.Printf("Strange1: %s not allocated yet.", input.Name())
			continue
		}
		if tg.RequiredResource != "" && !containsStr(al.ProvidedResources, tg.RequiredResource) {
			cost += 10000
		}
		cost += dataLocation.Distance(loc)
	}
	return float64(bid) / cost
}

func containsStr(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}
