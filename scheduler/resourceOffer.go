package main

import (
	"math"

	"github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
)

type resourceOffer struct {
	host string
	cpus float64
	mems float64
}

type offerResources struct {
	cpus float64
	mems float64
}

func newResourceOffer(offers []*mesos.Offer) *resourceOffer {
	if len(offers) == 0 {
		return nil
	}

	host := *offers[0].Hostname
	cpus := 0.0
	mems := 0.0

	for _, offer := range offers {
		if *offer.Hostname != host {
			glog.Warningf("ignoring offer %s - wrong host %s", offer.Id.Value, *offer.Hostname)
			continue
		}

		res := getOfferResources(offer)
		cpus += res.cpus
		mems += res.mems
	}

	return &resourceOffer{host: host, cpus: cpus, mems: mems}
}

func (r *resourceOffer) Host() string {
	return r.host
}

func (r *resourceOffer) Subtract(cpus float64, mems float64) bool {
	ok := r.cpus >= cpus && r.mems >= mems
	if !ok {
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
