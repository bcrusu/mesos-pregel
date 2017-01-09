package main

import (
	"math"

	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
)

type resourcesPool struct {
	cpus float64
	mems float64
}

type offerResources struct {
	cpus float64
	mems float64
}

func newResourcePool(offers []*mesos.Offer) *resourcesPool {
	cpus := 0.0
	mems := 0.0

	for _, offer := range offers {
		res := getOfferResources(offer)
		cpus += res.cpus
		mems += res.mems
	}

	return &resourcesPool{cpus: cpus, mems: mems}
}

func (r *resourcesPool) CanSubtract(cpus float64, mems float64) bool {
	return r.cpus >= cpus && r.mems >= mems
}

func (r *resourcesPool) Subtract(cpus float64, mems float64) bool {
	if !r.CanSubtract(cpus, mems) {
		return false
	}

	r.cpus -= cpus
	r.mems -= mems
	return true
}

func getOfferResources(offer *mesos.Offer) *offerResources {
	resources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool { return res.GetName() == resourceNameCPU })
	cpus := 0.0
	for _, res := range resources {
		cpus += res.GetScalar().GetValue()
	}

	resources = util.FilterResources(offer.Resources, func(res *mesos.Resource) bool { return res.GetName() == resourceNameMEM })
	mems := 0.0
	for _, res := range resources {
		mems += res.GetScalar().GetValue()
	}

	// account for executor resources if there's not an executor already running on the slave
	if len(offer.ExecutorIds) == 0 {
		cpus = math.Max(0, cpus-executorOverheadCPU)
		mems = math.Max(0, mems-executorOverheadMEM)
	}

	return &offerResources{cpus: cpus, mems: mems}
}
