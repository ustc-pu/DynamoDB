package mydynamotest

import (
	"log"
	"mydynamo"
	"testing"
	"fmt"
)

func TestBasicVectorClock(t *testing.T) {
	t.Logf("Starting TestBasicVectorClock")

	//create two vector clocks
	clock1 := mydynamo.NewVectorClock()
	clock2 := mydynamo.NewVectorClock()

	//Test for equality
	log.Println("test for equal")
	if !clock1.Equals(clock2) {
		t.Fail()
		t.Logf("Vector Clocks were not equal")
	}

	//Test for less than
	clock1Map := clock1.Clock
	clock2Map := clock2.Clock
	clock1Map["x"] = 1
	clock2Map["x"] = 2
	log.Println("test for less than")
	if !clock1.LessThan(clock2) {
		t.Fail()
		t.Logf("cleck1 is not less than clock2")
	}

	//Test for concurrent
	clock1Map["y"] = 2
	clock2Map["y"] = 1
	log.Println("test for concurrent")
	if !clock1.Concurrent(clock2) {
		t.Fail()
		t.Logf("Vector Clock1 and clock2 should be concurrent")
	}

	clock2.Increment("x")
	clocks := make([]mydynamo.VectorClock, 0)
	clocks = append(clocks, clock2)
	clock1.Combine(clocks)
	for k, v := range clock1.Clock {
		fmt.Println(k)
		fmt.Println(v)
	}
}
