package task

import "github.com/bcrusu/mesos-pregel/protos"

type hostSet struct {
	byID   map[int]*hostInfo // map[HOST_ID]hostname
	byName map[string]int    // map[hostname]HOST_ID
}

type hostInfo struct {
	hostname  string
	failCount int
}

func (h *hostSet) Add(hostname string) int {
	h.ensureMaps()

	id, ok := h.byName[hostname]
	if ok {
		return id
	}

	id = h.nextID()
	h.byID[id] = &hostInfo{hostname: hostname}
	h.byName[hostname] = id
	return id
}

func (h *hostSet) Get(ID int) (*hostInfo, bool) {
	info, ok := h.byID[ID]
	return info, ok
}

func (h *hostSet) GetID(hostname string) (int, bool) {
	id, ok := h.byName[hostname]
	return id, ok
}

func (h *hostSet) GetHostname(ID int) (string, bool) {
	info, ok := h.byID[ID]
	if !ok {
		return "", false
	}

	return info.hostname, true
}

func (h *hostSet) IsEmpty() bool {
	return h.byID == nil || len(h.byID) == 0
}

func (h *hostSet) nextID() int {
	return len(h.byID) + 1
}

func (h *hostSet) ensureMaps() {
	if h.byID != nil {
		return
	}

	h.byID = make(map[int]*hostInfo)
	h.byName = make(map[string]int)
}

func (h *hostSet) toProto() []*protos.JobCheckpoint_Host {
	result := make([]*protos.JobCheckpoint_Host, len(h.byID))

	i := 0
	for hostID, info := range h.byID {
		result[i] = &protos.JobCheckpoint_Host{
			Id:        int32(hostID),
			Hostname:  info.hostname,
			FailCount: int32(info.failCount),
		}

		i++
	}

	return result
}

func (h *hostSet) fromProto(hosts []*protos.JobCheckpoint_Host) *hostSet {
	h.byID = make(map[int]*hostInfo)
	h.byName = make(map[string]int)

	for _, host := range hosts {
		id := int(host.Id)
		h.byID[id] = &hostInfo{
			hostname:  host.Hostname,
			failCount: int(host.FailCount),
		}

		h.byName[host.Hostname] = id
	}

	return h
}
