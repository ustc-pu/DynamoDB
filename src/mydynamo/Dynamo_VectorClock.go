package mydynamo

type VectorClock struct {
	//todo 
	Clock map[string]int
}

//Creates a new VectorClock
func NewVectorClock() VectorClock {
	aClock := make(map[string]int)
	return VectorClock{
		Clock: aClock,
	}
	//return VectorClock{}
}

//Returns true if the other VectorClock is causally descended from this one
//if otherClokc is newer
func (s VectorClock) LessThan(otherClock VectorClock) bool {
	// panic("todo")
	sClockMap := s.Clock
	otherClockMap := otherClock.Clock
	//s 8080:2
	//other 8082:1
	for node, id := range sClockMap {
		if otherID, ok := otherClockMap[node]; ok {
			if otherID >= id {
				continue
			} else {
				return false
			}
		} else {
			return false
		}
	}
	return !s.Equals(otherClock)
}

//Returns true if neither VectorClock is causally descended from the other
func (s VectorClock) Concurrent(otherClock VectorClock) bool {
	// panic("todo")
	return !s.LessThan(otherClock) && !otherClock.LessThan(s)
}

//Increments this VectorClock at the element associated with nodeId
func (s *VectorClock) Increment(nodeId string) {
	// panic("todo")
	sClockMap := (*s).Clock
	if _, ok := sClockMap[nodeId]; ok {
		sClockMap[nodeId]++
	} else {
		sClockMap[nodeId] = 1
	}
}

//Changes this VectorClock to be causally descended from all VectorClocks in clocks
func (s *VectorClock) Combine(clocks []VectorClock) {
	
	selfClockMap := s.Clock
	for _, otherClock := range clocks {
		otherClockMap := otherClock.Clock
		for otherId, otherVersion := range otherClockMap {
			if selfVersion, ok := selfClockMap[otherId]; !ok {
				selfClockMap[otherId] = otherVersion
			} else if selfVersion < otherVersion {
				selfClockMap[otherId] = otherVersion
			}
		}
	}

}

//Tests if two VectorClocks are equal
func (s *VectorClock) Equals(otherClock VectorClock) bool {
	//panic("todo")
	sClockMap := s.Clock
	otherClockMap := otherClock.Clock
	if(len(sClockMap) != len(otherClockMap)) {
		return false
	}

	for node, id := range sClockMap {
		if otherID, ok := otherClockMap[node];  ok{
			if otherID == id {
				continue
			} else {
				return false
			}
		} else {
			return false
		}
	}
	return true
}
