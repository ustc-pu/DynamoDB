package mydynamo

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type DynamoServer struct {
	/*------------Dynamo-specific-------------*/
	wValue         int          //Number of nodes to write to on each Put
	rValue         int          //Number of nodes to read from on each Get
	preferenceList []DynamoNode //Ordered list of other Dynamo nodes to perform operations o
	selfNode       DynamoNode   //This node's address and port info
	nodeID         string       //ID of this node
	selfData	   map[string]DynamoResult   //key: "S1"
	isCrashed	   bool
}

func (s *DynamoServer) SendPreferenceList(incomingList []DynamoNode, _ *Empty) error {
	s.preferenceList = incomingList
	return nil
}

// Forces server to gossip
// As this method takes no arguments, we must use the Empty placeholder
func (s *DynamoServer) Gossip(_ Empty, _ *Empty) error {
	// panic("todo")
	log.Println("gossip to other nodes")
	preferenceList := RotateServerList(s.preferenceList)
	for key, dynamoRes := range s.selfData {
		for _, node := range preferenceList {
			//this client connects to the server on port 8080
			if s.selfNode.Address == node.Address && s.selfNode.Port == node.Port {
				continue
			}
			clientInstance := NewDynamoRPCClient(node.Address + ":" + node.Port)
			clientInstance.RpcConnect()
			var value PutArgs
			value.Key = key
			for _, objEntry := range dynamoRes.EntryList {
				value.Context = objEntry.Context
				value.Value = objEntry.Value
				//TODO: check value here??
				// PrintValue(value)
				clientInstance.PutToSingleNode(value)
			}
		}
	}
	log.Println("gossip finished")

	return nil
}

//Makes server unavailable for some seconds
func (s *DynamoServer) Crash(seconds int, success *bool) error {
	// panic("todo")
	log.Println("set " + s.selfNode.Address + ":" + s.selfNode.Port + " to crash state")
	go func() {
		s.isCrashed = true
		*success = true
		time.Sleep(time.Duration(seconds) * time.Second)
		s.isCrashed = false
	}()
	return nil
}

// Put a file to this server and W other servers
func (s *DynamoServer) Put(value PutArgs, result *bool) error {
	// panic("todo")
	if s.isCrashed {
		*result = false
		return errors.New("Error: server " + s.selfNode.Address + ":" + s.selfNode.Port + " is currently crashed")
	}
	selfNodeID := s.selfNode.Address + ":" + s.selfNode.Port
	log.Printf("client call Put, write data to node %v and w-1 other nodes\n", selfNodeID)

	objEntry := ObjectEntry{value.Context, value.Value}
	vectorClock := value.Context.Clock

	log.Println("print vector clock before increment")
	for k,v := range vectorClock.Clock {
		log.Println(k)
		log.Println(v)
	}
	if len(vectorClock.Clock) == 0{
		(&vectorClock).Increment(selfNodeID)
	}  else {
		for nodeId, _ := range vectorClock.Clock {
			(&vectorClock).Increment(nodeId)
		}
	}
	log.Println("print vector clock after increment")
	for k,v := range vectorClock.Clock {
		log.Println(k)
		log.Println(v)
	}

	entryList := make([]ObjectEntry,0)
	dynamoRes := DynamoResult{entryList}
	key := value.Key
	err := s.GetFromSingleNode(key, &dynamoRes)
	log.Println("Print local dynamoResult:")
	PrintDynamoResult(dynamoRes)
	if err != nil {
		newEntryList := make([]ObjectEntry, 0)
		newEntryList = append(newEntryList, objEntry)
		s.selfData[key] = DynamoResult{newEntryList}
	} else {
		// context := value.Context
		for index, oldObjectEntry := range dynamoRes.EntryList {
			oldContext := oldObjectEntry.Context
			if oldContext.Clock.Equals(vectorClock) {
				continue
			} else if oldContext.Clock.LessThan(vectorClock) {
				log.Println("less than")
				//oldContext = Context{vectorClock}
				//oldObjectEntry.Value = value.Value
				// log.Println(string(oldObjectEntry.Value[:]))
				dynamoRes.EntryList[index] = ObjectEntry{Context{vectorClock}, value.Value}
			} else if oldContext.Clock.Concurrent(vectorClock) {
				log.Println("concurrent")
				//entryList = append(entryList, ObjectEntry{value.Context, value.Value})
				dynamoRes.EntryList = append(dynamoRes.EntryList, ObjectEntry{value.Context, value.Value})
			}
		}
		PrintDynamoResult(dynamoRes)
		duplicateObjEntryIdx := make([]int, 0)
		log.Println("remove duplicate from dynamores")
		for index, _ := range dynamoRes.EntryList {
			//check if we have duplicates
			if index >= 1 {
				if dynamoRes.EntryList[index].Equals(dynamoRes.EntryList[index-1]) {
					duplicateObjEntryIdx = append(duplicateObjEntryIdx, index)
				}
			}
		}

		for _, idx := range duplicateObjEntryIdx {
			log.Printf("find duplicate at idx %d\n", idx)
			dynamoRes.EntryList = remove(dynamoRes.EntryList, idx)
		}
		PrintDynamoResult(dynamoRes)
		s.selfData[key] = dynamoRes
	}

	log.Println("after local put")
	PrintServerDataMap(s.selfData)

	//write to W-1 nodes
	preferenceList := RotateServerList(s.preferenceList)
	writeToPreferListCounter := 0
	log.Println("iterate preference list, write to other w-1 nodes")
	for _, node := range preferenceList {
		if writeToPreferListCounter == s.wValue - 1 {
			log.Println("write enough nodes")
			break
		}
		var value PutArgs
		value.Key = key
		log.Println("write data to node " + node.Address + ":" + node.Port)
		clientInstance := NewDynamoRPCClient(node.Address + ":" + node.Port)
		clientInstance.RpcConnect()
		serverNormal := true
		for _, objEntry := range s.selfData[key].EntryList {
			value.Context = objEntry.Context
			value.Value = objEntry.Value
			serverNormal := clientInstance.PutToSingleNode(value)
			if !serverNormal {
				log.Println("server " + node.Address + ":" + node.Port + " is crashed")
				break
			}
		}
		if serverNormal {
			writeToPreferListCounter++
		}
	}
	log.Println("iterate preference list finished")
	if writeToPreferListCounter < s.wValue - 1 {
		*result = false
		//TODO: return what??
		// return errors.New("Error: not enough write")
		log.Println("not enough w nodes writes")
	} else {
		*result = true
	}

	//PrintServerDataMap(s.selfData)
	log.Printf("client call Put finished")
	return nil
}

// Put a file to this server
func (s *DynamoServer) PutToSingleNode(value PutArgs, result *bool) error {
	// panic("todo")
	if s.isCrashed {
		*result = false
		return errors.New("Error: server " + s.selfNode.Address + ":" + s.selfNode.Port + " is currently crashed")
	}
	log.Printf("PutToSingleNode %v\n", s.selfNode.Address + ":" + s.selfNode.Port)

	//objEntry := ObjectEntry{value.Context, value.Value}
	// nodeId := s.selfNode.Address + ":" + s.selfNode.Port
	// vectorClock := objEntry.Context.Clock
	// (&vectorClock).Increment(nodeId)

	entryList := make([]ObjectEntry,0)
	dynamoRes := DynamoResult{entryList}
	key := value.Key
	err := s.GetFromSingleNode(key, &dynamoRes)

	if err != nil {
		newEntryList := make([]ObjectEntry, 0)
		objEntry := ObjectEntry{value.Context, value.Value}
		newEntryList = append(newEntryList, objEntry)
		s.selfData[key] = DynamoResult{newEntryList}
	} else {
		context := value.Context
		for index, oldObjectEntry := range dynamoRes.EntryList {
			oldContext := oldObjectEntry.Context
			if oldContext.Clock.Equals(context.Clock) {
				continue
			}else if oldContext.Clock.LessThan(context.Clock) {
				//oldContext = context
				//oldObjectEntry.Value = value.Value
				vectorClock := value.Context.Clock
				dynamoRes.EntryList[index] = ObjectEntry{Context{vectorClock}, value.Value}
			} else if oldContext.Clock.Concurrent(context.Clock) {
				//entryList = append(entryList, ObjectEntry{value.Context, value.Value})
				dynamoRes.EntryList = append(dynamoRes.EntryList, ObjectEntry{value.Context, value.Value})
			}
		}
		PrintDynamoResult(dynamoRes)
		s.selfData[key] = dynamoRes
	}

	*result = true
	return nil
}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) Get(key string, result *DynamoResult) error {
	// panic("todo")
	if s.isCrashed {
		return errors.New("Error: server " + s.selfNode.Address + ":" + s.selfNode.Port + " is currently crashed")
	}
	// if data, ok := s.selfData[key]; ok {
	// 	*result = data
	// }
	// else {
	// 	err := errors.New("key does not exist!")
	// 	return err
	// }
	//log.Printf("get data from node %v\n", s.selfNode.Address + ":" + s.selfNode.Port)
	preferenceList := RotateServerList(s.preferenceList)

	//log.Println("print preference list in Get:")
	//for _, dynamoNode := range preferenceList {
	//	log.Println(dynamoNode.Address + ":" + dynamoNode.Port)
	//}

	returnEntryList := make([]ObjectEntry,0)
	for _, objEntry := range s.selfData[key].EntryList {
		returnEntryList = append(returnEntryList, objEntry)
	}
	//log.Printf("the size of returnEntryList = %d\n", len(returnEntryList))
	readFromPrefListCounter := 0
	// oldContext := s.selfData[key]

	for _, node := range preferenceList {
		if readFromPrefListCounter == s.rValue - 1 {
			break
		}
		//this client connects to the server on port 8080
		clientInstance := NewDynamoRPCClient(node.Address + ":" + node.Port)
		clientInstance.RpcConnect()
		// entryList := make([]ObjectEntry,0)
		// dynamoRes := DynamoResult{entryList}
		dynamoRes := (clientInstance.GetFromSingleNode(key))


		if dynamoRes != nil {
			log.Println("dynamoRes is not nil, read data from " + node.Address + ":" + node.Port)
			PrintDynamoResult(*dynamoRes)
			readFromPrefListCounter++
		} else {
			log.Println("dynamoRes is nil")
			continue //if dynamoRes is nil, node is crashed
		}

		for index, otherObjEntry := range (*dynamoRes).EntryList {
			otherContext := otherObjEntry.Context
			otherValue := otherObjEntry.Value
			for _, oldObjEntry := range returnEntryList {
				oldContext := oldObjEntry.Context
				if oldContext.Clock.Equals(otherContext.Clock) {
					continue
				} else if oldContext.Clock.LessThan(otherContext.Clock) {
					//deep copy 
					//TODO: while read, will local data be replaced by newer data from other nodes
					// oldContext = otherContext
					// oldObjEntry.Value = otherValue
					returnEntryList[index] = ObjectEntry{otherContext, otherValue} 
				} else if oldContext.Clock.Concurrent(otherContext.Clock) {
					returnEntryList = append(returnEntryList, otherObjEntry)
				} 
			}
		}
	}

	if len(returnEntryList) == 0 {
		err := errors.New("Error: key does not exist!")
		return err
	}

	*result = DynamoResult{returnEntryList}
	log.Println("print dynamores before return from get")
	PrintDynamoResult(*result)
	
	return nil
}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) GetFromSingleNode(key string, result *DynamoResult) error {
	// panic("todo")
	//log.Printf("get data from node %v\n", s.selfNode.Address + ":" + s.selfNode.Port)
	if s.isCrashed {
		return errors.New("Error: server " + s.selfNode.Address + ":" + s.selfNode.Port + " is currently crashed")
	}
	if data, ok := s.selfData[key]; ok {
		*result = data
		
	} else {
		err := errors.New("Error: key does not exist!")
		return err
	}
	return nil
}

/* Belows are functions that implement server boot up and initialization */
func NewDynamoServer(w int, r int, hostAddr string, hostPort string, id string) DynamoServer {
	preferenceList := make([]DynamoNode, 0)
	selfNodeInfo := DynamoNode{
		Address: hostAddr,
		Port:    hostPort,
	}
	selfdata := make(map[string]DynamoResult)
	return DynamoServer{
		wValue:         w,
		rValue:         r,
		preferenceList: preferenceList,
		selfNode:       selfNodeInfo,
		nodeID:         id,
		selfData:	 	selfdata,
		isCrashed:		false,
	}
}

func ServeDynamoServer(dynamoServer DynamoServer) error {
	rpcServer := rpc.NewServer()
	e := rpcServer.RegisterName("MyDynamo", &dynamoServer)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Name Registration")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Registered the RPC Interfaces")

	l, e := net.Listen("tcp", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Port Listening")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Listening to Target Port ", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	log.Println(DYNAMO_SERVER, "Serving Server Now")

	return http.Serve(l, rpcServer)
}
