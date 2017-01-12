package graph

type Graph struct {
	key       string
	superstep int
	vertices  map[string]*vertex
}

type vertex struct {
	value interface{}
	edges map[string]interface{}
}

func NewGraph(capacity int) *Graph {
	return &Graph{vertices: make(map[string]*vertex, capacity)}
}

func (g *Graph) setVertexValue(id string, value interface{}) {
	v, ok := g.vertices[id]
	if !ok {
		v = &vertex{edges: make(map[string]interface{})}
		g.vertices[id] = v
	}

	v.value = value
}

func (g *Graph) setEdgeValue(from string, to string, value interface{}) {
	v, ok := g.vertices[from]
	if !ok {
		v = &vertex{edges: make(map[string]interface{})}
		g.vertices[from] = v
	}

	v.edges[to] = value
}

func (g *Graph) removeVertex(id string) {
	_, ok := g.vertices[id]
	if !ok {
		return
	}

	delete(g.vertices, id)

	for _, v := range g.vertices {
		delete(v.edges, id)
	}
}

func (g *Graph) removeEdge(from string, to string) {
	v, ok := g.vertices[from]
	if !ok {
		return
	}

	delete(v.edges, to)
}

func (g *Graph) VertexValue(id string) (interface{}, bool) {
	v, ok := g.vertices[id]
	if !ok {
		return nil, false
	}

	return v.value, true
}

func (g *Graph) HasVertex(id string) bool {
	_, ok := g.VertexValue(id)
	return ok
}

func (g *Graph) EdgeValue(from string, to string) (interface{}, bool) {
	v, ok := g.vertices[from]
	if !ok {
		return nil, false
	}

	return v.edges[to], true
}

func (g *Graph) HasEdge(from string, to string) bool {
	_, ok := g.EdgeValue(from, to)
	return ok
}

func (g *Graph) Vertices() []string {
	result := make([]string, len(g.vertices))
	i := 0
	for k := range g.vertices {
		result[i] = k
		i++
	}

	return result
}

func (g *Graph) EdgesFrom(id string) []string {
	v, ok := g.vertices[id]
	if !ok {
		return make([]string, 0)
	}

	result := make([]string, len(v.edges))
	i := 0
	for k := range v.edges {
		result[i] = k
		i++
	}

	return result
}
