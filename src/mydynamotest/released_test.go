package mydynamotest

import (
    "log"
    "mydynamo"
    "testing"
    "time"
)

func TestPutW2(t *testing.T){
    t.Logf("Starting PutW2 test")
    cmd := InitDynamoServer("./twoserver.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
    gotValuePtr := clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestPutW2: Failed to get")
    }
    gotValue := *gotValuePtr
    log.Println("print gotValue:")
    log.Printf("the length of gotValue.EntryList = %d\n", len(gotValue.EntryList))
    for _, objEntry := range gotValue.EntryList {
        log.Println("print value:")
        log.Println(string(objEntry.Value[:]))
        log.Println("print vector clock")
        for k,v := range objEntry.Context.Clock.Clock {
            log.Println(k)
            log.Println(v)
        }
    }
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestPutW2: Failed to get value")
    }

}

func TestGossip(t *testing.T){
    t.Logf("Starting Gossip test")
    cmd := InitDynamoServer("./myconfig.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
    clientInstance0.Gossip()
    gotValuePtr := clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestGossip: Failed to get")
    }
    gotValue := *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestGossip: Failed to get value")
    }

}

func TestMultipleKeys(t *testing.T){
    t.Logf("Starting MultipleKeys test")
    cmd := InitDynamoServer("./myconfig.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
    clientInstance0.Gossip()
    gotValuePtr := clientInstance0.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestMultipleKeys: Failed to get")
    }
    gotValue := *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestMultipleKeys: Failed to get value")
    }

    gotValuePtr = clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestMultipleKeys: Failed to get")
    }
    gotValue = *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestMultipleKeys: Failed to get value")
    }   
    
    //log.Println("print gotValue before put efghi:")
    //log.Printf("the length of gotValue.EntryList = %d\n", len(gotValue.EntryList))
    //for _, objEntry := range gotValue.EntryList {
    //    log.Println("print value:")
    //    log.Println(string(objEntry.Value[:]))
    //    log.Println("print vector clock")
    //    for k,v := range objEntry.Context.Clock.Clock {
    //        log.Println(k)
    //        log.Println(v)
    //    }
    //}

    clientInstance1.Put(mydynamo.NewPutArgs("s1",gotValue.EntryList[0].Context ,[]byte("efghi")))
    gotValuePtr = clientInstance1.Get("s1")
    if gotValuePtr == nil {
       t.Fail()
       t.Logf("TestMultipleKeys: Failed to get")
    }
    gotValue = *gotValuePtr
    log.Println("print gotValue after put efghi:")
    log.Printf("the length of gotValue.EntryList = %d\n", len(gotValue.EntryList))
    for _, objEntry := range gotValue.EntryList {
       log.Println("print value:")
       log.Println(string(objEntry.Value[:]))
       log.Println("print vector clock")
       for k,v := range objEntry.Context.Clock.Clock {
           log.Println(k)
           log.Println(v)
       }
    }

    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("efghi"))){
       t.Fail()
       t.Logf("TestMultipleKeys: Failed to get value")
    }
}

func TestDynamoPaper(t *testing.T){
    t.Logf("DynamoPaper test")
    cmd := InitDynamoServer("./myconfig.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance2 := MakeConnectedClient(8082)

    clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
    gotValuePtr := clientInstance0.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestDynamoPaper: Failed to get first value")
    }

    gotValue := *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestDynamoPaper: First value doesn't match")
    }
    clientInstance0.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("bcdef")))
    gotValuePtr = clientInstance0.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestDynamoPaper: Failed to get second value")
    }
    gotValue = *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("bcdef"))){
        t.Fail()
        t.Logf("TestDynamoPaper: Second value doesn't match")
    }

    clientInstance0.Gossip()
    clientInstance1.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("cdefg")))
    clientInstance2.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("defgh")))
    gotValuePtr = clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestDynamoPaper: Failed to get third value")
    }
    gotValue = *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("cdefg"))){
        t.Fail()
        t.Logf("TestDynamoPaper: Third value doesn't match")
    }
    gotValuePtr = clientInstance2.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestDynamoPaper: Failed to get fourth value")
    }
    gotValue = *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("defgh"))){
        t.Fail()
        t.Logf("TestDynamoPaper: Fourth value doesn't match")
    }
    clientInstance1.Gossip()
    clientInstance2.Gossip()
    gotValuePtr = clientInstance0.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestDynamoPaper: Failed to get fifth value")
    }
    gotValue = *gotValuePtr
    clockList := make([]mydynamo.VectorClock, 0)
    for _, a := range gotValue.EntryList {
        clockList = append(clockList, a.Context.Clock)
    }
    clockList[0].Combine(clockList)
    combinedClock := clockList[0]
    combinedContext := mydynamo.Context {
        Clock:combinedClock,
    }
    clientInstance0.Put(mydynamo.NewPutArgs("s1", combinedContext, []byte("zyxw")))
    gotValuePtr = clientInstance0.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestDynamoPaper: Failed to get sixth value")
    }
    gotValue = *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("zyxw"))){
        t.Fail()
        t.Logf("TestDynamoPaper: Sixth value doesn't match")
    }

}

func TestInvalidPut(t *testing.T){
    t.Logf("Starting repeated Put test")
    cmd := InitDynamoServer("./myconfig.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready
    clientInstance := MakeConnectedClient(8080)

    clientInstance.Put(PutFreshContext("s1", []byte("abcde")))
    clientInstance.Put(PutFreshContext("s1", []byte("efghi")))
    gotValue := clientInstance.Get("s1")
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestInvalidPut: Got wrong value")
    }
}

func TestGossipW2(t *testing.T){
    t.Logf("Starting GossipW2 test")
    cmd := InitDynamoServer("./twoserver.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
    clientInstance0.Gossip()
    gotValuePtr := clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestGossipW2: Failed to get first element")
    }
    gotValue := *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestGossipW2: Failed to get value")
    }
    clientInstance1.Put(mydynamo.NewPutArgs("s1",gotValue.EntryList[0].Context, []byte("efghi")))

    gotValuePtr = clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestGossipW2: Failed to get")
    }
    gotValue = *gotValuePtr

    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("efghi"))){
        t.Fail()
        t.Logf("GossipW2: Failed to get value")
    }
    gotValuePtr = clientInstance0.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestGossipW2: Failed to get")
    }
    gotValue = *gotValuePtr
    
    log.Println("print gotValue client0 get:")
    log.Printf("the length of gotValue.EntryList = %d\n", len(gotValue.EntryList))
    for _, objEntry := range gotValue.EntryList {
       log.Println("print value:")
       log.Println(string(objEntry.Value[:]))
       log.Println("print vector clock")
       for k,v := range objEntry.Context.Clock.Clock {
           log.Println(k)
           log.Println(v)
       }
    }
    if(len(gotValue.EntryList) != 1) || !valuesEqual(gotValue.EntryList[0].Value, []byte("efghi")){
        t.Fail()
        t.Logf("GossipW2: Failed to get value")
    }

}

func TestReplaceMultipleVersions(t *testing.T){
    t.Logf("Starting ReplaceMultipleVersions test")
    cmd := InitDynamoServer("./myconfig.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance0.Put(PutFreshContext("s1", []byte("abcde"))) //s0 s1 : [ {<8080:1>, "abcde"} ]
    clientInstance1.Put(PutFreshContext("s1", []byte("efghi"))) //s1 s1 : [ {<8081:1>, "efghi"} ]
    clientInstance0.Gossip() //s1 s1 -> [ {<8081:1>, "efghi"}, {<8080:1>, "abcde"} ]
    gotValuePtr := clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestReplaceMultipleVersions: Failed to get")
    }

    gotValue := *gotValuePtr
    log.Println("Print gotValue before combining")

    clockList := make([]mydynamo.VectorClock, 0)
    for _, a := range gotValue.EntryList {
        clockList = append(clockList, a.Context.Clock)
        log.Println("value = " + string(a.Value[:]))
        log.Println(len(a.Context.Clock.Clock))
        for k,v := range a.Context.Clock.Clock {
            log.Println(k)
            log.Println(v)
        }
    }
    //8081:1 
    clockList[0].Combine(clockList) //8081:1, 8080:1
    combinedClock := clockList[0]
    combinedContext := mydynamo.Context {
        Clock:combinedClock,
    }
    log.Println("print combinedContext.Clock.Clock")
    log.Println(len(combinedContext.Clock.Clock))
    for k,v := range combinedContext.Clock.Clock {
        log.Println(k)
        log.Println(v)
    }
    //{ <8080:1>, <8081:1> }
    clientInstance1.Put(mydynamo.NewPutArgs("s1", combinedContext, []byte("zxyw")))
    gotValuePtr = nil
    gotValuePtr = clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestReplaceMultipleVersions: Failed to get")
    }


    gotValue = *gotValuePtr
    log.Println("len of gotValue.EntryList = ", len(gotValue.EntryList))
    log.Println("gotValue.EntryList[0].Value = ", string(gotValue.EntryList[0].Value[:]))
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("zxyw"))){
        t.Fail()
        t.Logf("testReplaceMultipleVersions: Values don't match")
    }


}

func TestConsistent(t *testing.T){
    t.Logf("Starting Consistent test")
    cmd := InitDynamoServer("./consistent.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance2 := MakeConnectedClient(8082)
    clientInstance3 := MakeConnectedClient(8083)
    clientInstance4 := MakeConnectedClient(8084)

    clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
    gotValuePtr := clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestConsistent: Failed to get")
    }
    gotValue := *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestConsistent: Failed to get value")
    }


    clientInstance3.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("zyxw")))
    clientInstance0.Crash(3)
    clientInstance1.Crash(3)
    clientInstance4.Crash(3)
    gotValuePtr = clientInstance2.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestConsistent: Failed to get")
    }
    gotValue = *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("zyxw"))){
        t.Fail()
        t.Logf("TestConsistent: Failed to get value")
    }
}

func TestCrashRecoverGet(t *testing.T){
    t.Logf("Starting Consistent test")
    cmd := InitDynamoServer("./crashconfig.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance2 := MakeConnectedClient(8082)

    clientInstance2.Crash(3)
    clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
    gotValuePtr := clientInstance1.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestConsistent: Failed to get")
    }
    gotValue := *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestConsistent: Failed to get value")
    }

    //server 2 is still crashed
    gotValuePtr = clientInstance2.Get("s1")
    if gotValuePtr != nil {
        t.Fail()
        t.Logf("TestCrashNotRecoverGet: Can not GET while crash")
    }

    
    time.Sleep(time.Duration(5) * time.Second)
    //server 2 is now up but empty
    gotValuePtr = clientInstance2.Get("s1")
    if gotValuePtr != nil {
        t.Fail()
        t.Logf("TestCrashNotRecoverGet: server 2 should be empty")
    }

    clientInstance0.Gossip()
    gotValuePtr = clientInstance2.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestCrashNotRecoverGet: Failed to get")
    }
    gotValue = *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestCrashNotRecoverGet: Failed to get value")
    }
}

func TestCrashBasic(t *testing.T){
    t.Logf("Starting Consistent test")
    cmd := InitDynamoServer("./myconfig.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)

    clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
    gotValuePtr := clientInstance0.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestConsistent: Failed to get")
    }
    gotValue := *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestConsistent: Failed to get value")
    }


    //server 0 is set to crash state
    clientInstance0.Crash(3)

    gotValuePtr = clientInstance0.Get("s1")
    //server 0  is still empty
    if gotValuePtr != nil {
        t.Fail()
        t.Logf("TestCrashNotRecoverGet: Can not GET while crash")
    }

    time.Sleep(time.Duration(5) * time.Second)
    //server 0 is running
    gotValuePtr = clientInstance0.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestConsistent: Failed to get")
    }
    gotValue = *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
        t.Fail()
        t.Logf("TestConsistent: Failed to get value")
    }
}

func TestGetMultiple(t *testing.T){
    t.Logf("Starting get from mulptiple nodes test")
    cmd := InitDynamoServer("./getmultiple.ini")
    ready := make(chan bool)
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    time.Sleep(3 * time.Second)
    <-ready

    clientInstance0 := MakeConnectedClient(8080)
    clientInstance1 := MakeConnectedClient(8081)
    clientInstance2 := MakeConnectedClient(8082)
    // clientInstance3 := MakeConnectedClient(8083)

    clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
    gotValuePtr := clientInstance0.Get("s1")
    if gotValuePtr == nil {
       t.Fail()
       t.Logf("TestGetMultiple: Failed to get")
    }
    gotValue := *gotValuePtr
    if(len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde"))){
       t.Fail()
       t.Logf("TestGetMultiple: Failed to get value")
    }
    clientInstance1.Put(mydynamo.NewPutArgs("s1",gotValue.EntryList[0].Context ,[]byte("efghi")))

    clientInstance2.Put(PutFreshContext("s1", []byte("xyzw")))
    //server 0 is set to crash state

    gotValuePtr = clientInstance0.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestGetMultiple: Failed to get")
    }
    gotValue = *gotValuePtr
    log.Println("print gotValue:")
    log.Printf("the length of gotValue.EntryList = %d\n", len(gotValue.EntryList))
    for _, objEntry := range gotValue.EntryList {
        log.Println("print value:")
        log.Println(string(objEntry.Value[:]))
        log.Println("print vector clock")
        for k,v := range objEntry.Context.Clock.Clock {
            log.Println(k)
            log.Println(v)
        }
    }
    if(len(gotValue.EntryList) != 2 || !valuesEqual(gotValue.EntryList[0].Value, []byte("efghi"))){
        t.Fail()
        t.Logf("TestGetMultiple: Failed to get value")
    }

    if(len(gotValue.EntryList) != 2 || !valuesEqual(gotValue.EntryList[1].Value, []byte("xyzw"))){
        t.Fail()
        t.Logf("TestGetMultiple: Failed to get value")
    }
}