package mydynamo
import (
	"log"
	"bytes"
)
//Removes an element at the specified index from a list of ObjectEntry structs
func remove(list []ObjectEntry, index int) []ObjectEntry {
	return append(list[:index], list[index+1:]...)
}

//Returns true if the specified list of ints contains the specified item
func contains(list []int, item int) bool {
	for _, v := range list {
		if v == item {
			return true
		}
	}
	return false
}

//return true if two objectentry are equal
func (self ObjectEntry) Equals(other ObjectEntry) bool {
	if bytes.Compare(self.Value, other.Value) != 0 {
		return false
	}

	//check context
	selfClcokMap := self.Context.Clock.Clock
	otherClockMap := other.Context.Clock.Clock
	if len(selfClcokMap) != len(otherClockMap) {
		return false
	}

	for selfK, selfV := range selfClcokMap {
		if otherV, ok := otherClockMap[selfK]; !ok {
			return false
		} else if(otherV != selfV) {
			return false
		}
	}

	return true
}

//Rotates a preference list by one, so that we can give each node a unique preference list
func RotateServerList(list []DynamoNode) []DynamoNode {
	return append(list[1:], list[0])
}

//Creates a new Context with the specified Vector Clock
func NewContext(vClock VectorClock) Context {
	return Context{
		Clock: vClock,
	}
}

//Creates a new PutArgs struct with the specified members.
func NewPutArgs(key string, context Context, value []byte) PutArgs {
	return PutArgs{
		Key:     key,
		Context: context,
		Value:   value,
	}
}

func PrintServerDataMap(selfData map[string]DynamoResult) {
	log.Println("print server.selfData map")
	for key, dynamoRes := range selfData {
		log.Printf("key = %v\n", key)
		PrintDynamoResult(dynamoRes)
	}
}

func PrintDynamoResult(dynamoRes DynamoResult) {
	for _, objectEntry := range dynamoRes.EntryList {
		log.Printf("value = %v\n", string(objectEntry.Value[:]))
		log.Println("----Print the vectoe clock map----")
		for nodeId,version := range objectEntry.Context.Clock.Clock {
			log.Printf("node = %v, version = %d\n", nodeId, version)
		}
		log.Println("----End of vectoe clock map----")
	}
}

func PrintValue(value PutArgs) {
	log.Println("key = ", value.Key)
	log.Printf("value = %v\n", string(value.Value[:]))
	log.Println("----Print the vectoe clock map----")
	for nodeId,version := range value.Context.Clock.Clock {
		log.Printf("node = %v, version = %d\n", nodeId, version)
	}
	log.Println("----End of vectoe clock map----")
}


//Creates a new DynamoNode struct with the specified members
func NewDynamoNode(addr string, port string) DynamoNode {
	return DynamoNode{
		Address: addr,
		Port:    port,
	}
}
