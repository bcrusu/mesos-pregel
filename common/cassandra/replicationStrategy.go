package cassandra

import "github.com/gocql/gocql"

type replicationStrategy interface {
	computeTokenToReplicaMap(ring *tokenRing) map[Token][]string
}

type simpleStrategy struct {
	replicationFactor int
}

type networkTopologyStrategy struct {
	replicationFactors map[string]int
}

type noReplicationStrategy struct {
}

func newReplicationStrategy(session *gocql.Session, keyspace string) (replicationStrategy, error) {
	return &noReplicationStrategy{}, nil
}

func (rs *simpleStrategy) computeTokenToReplicaMap(ring *tokenRing) map[Token][]string {
	//TODO: com.datastax.driver.core.SimpleStrategy
	return nil
}

func (rs *networkTopologyStrategy) computeTokenToReplicaMap(ring *tokenRing) map[Token][]string {
	// details here: com.datastax.driver.core.NetworkTopologyStrategy
	panic("NetworkTopologyStrategy not implemented atm.")
}

func (rs *noReplicationStrategy) computeTokenToReplicaMap(ring *tokenRing) map[Token][]string {
	result := make(map[Token][]string)
	for i, token := range ring.Tokens {
		host := ring.Hosts[i]
		result[token] = []string{host.Address}
	}

	return result
}
