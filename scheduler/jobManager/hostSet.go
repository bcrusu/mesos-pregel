package jobManager

import (
	"fmt"
)

type hostSet struct {
	byID   map[int]string // map[HOST_ID]hostname
	byName map[string]int // map[hostname]HOST_ID
}

func (h *hostSet) Add(hostname string) int {
	h.ensureMaps()

	id, ok := h.byName[hostname]
	if ok {
		return id
	}

	id = h.nextID()
	h.byID[id] = hostname
	h.byName[hostname] = id
	return id
}

func (h *hostSet) AddWithID(hostname string, id int) error {
	h.ensureMaps()

	_, ok := h.byID[id]
	if ok {
		return fmt.Errorf("duplicate id %d", id)
	}

	h.byID[id] = hostname
	h.byName[hostname] = id
	return nil
}

func (h *hostSet) GetID(hostname string) (int, bool) {
	id, ok := h.byName[hostname]
	return id, ok
}

func (h *hostSet) GetHostname(id int) (string, bool) {
	name, ok := h.byID[id]
	return name, ok
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

	h.byID = make(map[int]string)
	h.byName = make(map[string]int)
}
