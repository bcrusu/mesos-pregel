package graph

type Graph struct {
	vertices map[string]*vertex
}

type vertex struct {
	value interface{}
	edges map[string]interface{}
}

func NewGraph(capacity int) *Graph {
	return &Graph{vertices: make(map[string]*vertex, capacity)}
}

func (g *Graph) SetVertexValue(id string, value interface{}) {
	v, ok := g.vertices[id]
	if !ok {
		v = &vertex{edges: make(map[string]interface{})}
		g.vertices[id] = v
	}

	v.value = value
}

func (g *Graph) SetEdgeValue(from string, to string, value interface{}) {
	v, ok := g.vertices[from]
	if !ok {
		v = &vertex{edges: make(map[string]interface{})}
		g.vertices[from] = v
	}

	v.edges[to] = value
}

func (g *Graph) RemoveVertex(id string) {
	_, ok := g.vertices[id]
	if !ok {
		return
	}

	delete(g.vertices, id)

	for _, v := range g.vertices {
		delete(v.edges, id)
	}
}

func (g *Graph) RemoveEdge(from string, to string) {
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

func (g *Graph) EdgeValue(from string, to string) (interface{}, bool) {
	v, ok := g.vertices[from]
	if !ok {
		return nil, false
	}

	return v.edges[to], true
}

func (g *Graph) Clone() *Graph {
	vertices := make(map[string]*vertex)
	for k, v := range g.vertices {
		vertices[k] = v.Clone()
	}

	return &Graph{vertices}
}

func (v *vertex) Clone() *vertex {
	edges := make(map[string]interface{})
	for k, v := range v.edges {
		edges[k] = v
	}

	return &vertex{v.value, edges}
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

func (g *Graph) Edges(from string) []string {
	v, ok := g.vertices[from]
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
