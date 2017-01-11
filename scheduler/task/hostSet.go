package task

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

func (h *hostSet) GetHostname(id int) (string, bool) {
	info, ok := h.byID[id]
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
