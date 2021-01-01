package mydynamotest

import (
	"log"
	"mydynamo"
	"testing"
	"time"
)

func TestBasicPut(t *testing.T) {
	t.Logf("Starting basic Put test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(3 * time.Second)
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	clientInstance := MakeConnectedClient(8080)

	//Put a value on key "s1"
	clientInstance.Put(PutFreshContext("s1", []byte("abcde")))

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
}

func TestPutTwice(t *testing.T) {
	t.Logf("Starting basic PutTwice test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(3 * time.Second)
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	clientInstance0 := MakeConnectedClient(8080)
	clientInstance1 := MakeConnectedClient(8081)

	//Put a value on key "s1"
	
	clientInstance0.Put(PutFreshContext("s1", []byte("abcde"))) //s0 s1: { {<8080:1>, "abcde"}}

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestPutTwice: Returned nil")
	}
	gotValue := *gotValuePtr //{<8080:1>, "abcde"}
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestPutTwice: Failed to get value")
	}

	clientInstance0.Put(mydynamo.NewPutArgs("s1",gotValue.EntryList[0].Context ,[]byte("efghi")))
	//s0 s1: { {<8080:2>, "efghi"}}

	clientInstance1.Put(PutFreshContext("s1", []byte("12345")))
	//s1 s1: { {<8081:1>, "12345"}}

	gotValuePtr = clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestPutTwice: Returned nil")
	}
	gotValue = *gotValuePtr //{<8081:1>, "12345"}
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("12345")) {
		t.Fail()
		t.Logf("TestPutTwice: Failed to get value")
	}

	objectEntry := gotValue.EntryList[0]
	log.Println("objEntry.value = " + string(objectEntry.Value[:]))
	for k,v := range objectEntry.Context.Clock.Clock {
		log.Println(k)
		log.Println(v)
	}

	clientInstance0.Put(mydynamo.NewPutArgs("s1",gotValue.EntryList[0].Context ,[]byte("6789")))
	//s0 s1: { {<8080:2>, "efghi"}, {<8081:2>, "6789"}}

	gotValuePtr = clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 2 || !valuesEqual(gotValue.EntryList[0].Value, []byte("efghi")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}

	if len(gotValue.EntryList) != 2 || !valuesEqual(gotValue.EntryList[1].Value, []byte("6789")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}


}
