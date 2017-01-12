package main

import (
	"math"

	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
)

type resourcesPool struct {
	cpu float64
	mem float64
}

type offerResources struct {
	cpu float64
	mem float64
}

func newResourcePool(offers []*mesos.Offer) *resourcesPool {
	cpu := 0.0
	mem := 0.0

	for _, offer := range offers {
		res := getOfferResources(offer)
		cpu += res.cpu
		mem += res.mem
	}

	return &resourcesPool{cpu: cpu, mem: mem}
}

func (r *resourcesPool) CanSubtract(cpu float64, mem float64) bool {
	return r.cpu >= cpu && r.mem >= mem
}

func (r *resourcesPool) Subtract(cpu float64, mem float64) bool {
	if !r.CanSubtract(cpu, mem) {
		return false
	}

	r.cpu -= cpu
	r.mem -= mem
	return true
}

func getOfferResources(offer *mesos.Offer) *offerResources {
	resources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool { return res.GetName() == resourceNameCPU })
	cpu := 0.0
	for _, res := range resources {
		cpu += res.GetScalar().GetValue()
	}

	resources = util.FilterResources(offer.Resources, func(res *mesos.Resource) bool { return res.GetName() == resourceNameMEM })
	mem := 0.0
	for _, res := range resources {
		mem += res.GetScalar().GetValue()
	}

	// account for executor resources if there's not an executor already running on the slave
	if len(offer.ExecutorIds) == 0 {
		cpu = math.Max(0, cpu-executorOverheadCPU)
		mem = math.Max(0, mem-executorOverheadMEM)
	}

	return &offerResources{cpu: cpu, mem: mem}
}
