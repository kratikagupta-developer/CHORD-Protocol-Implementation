package chord

import (
	"fmt"
	"github.com/ahrtr/logrus"
	"time"
	"os"
	"encoding/csv"
	"strconv"
)

type CorrectnessParams struct {
	Version                   string
	NumNodes                  int
	NumSuccessors             int
	N                         int
	MinStabilizationTime      int
	MaxStabilizationTime      int
	StabilizationTimeSteps    int
	NumberStabilizationSteps  int
	EventFireDelay            int
	EventFireDelaySteps       int
	NumberEventFireDelaySteps int
}

type CorrectnessResult struct {
	NumNodes             int
	NumSuccessors        int
	N                    int
	MinStabilizationTime int
	MaxStabilizationTime int
	EventFireDelay       int
	NumberOfFailures     int
}

func TestCorrectness(params CorrectnessParams) (bool, error) {
	finalPass := true
	stabilizeMin := time.Duration(params.MinStabilizationTime) * time.Second
	stabilizeMax := time.Duration(params.MaxStabilizationTime) * time.Second
	for i := 0; i < params.NumberStabilizationSteps; i++ {
		sleep := time.Duration(params.EventFireDelay) * time.Second
		for j := 0; j < params.NumberEventFireDelaySteps; j++ {
			failures := 0
			for k := 0; k < params.N; k++ {
				logrus.Infoln("--------------------Scenario: ", i, j, k)
				config := DefaultConfig("local")
				//trans := InitLocalTransport(nil)
				config.StabilizeMax = stabilizeMax
				config.StabilizeMin = stabilizeMin
				config.NumVnodes = params.NumNodes
				config.NumSuccessors = params.NumSuccessors
				logrus.Infoln("Parameters: ", params)
				logrus.Infoln("Configuration: ", config)
				ring, err := Create(config, nil)
				if err != nil {
					fmt.Println("error in creating ring:", err.Error())
					return false, err
				}
				logrus.Infoln(ring.PrintNodes())
				pass := ring.CheckCorrectness(25, sleep, params.Version)
				if !pass {
					failures++
					finalPass = pass
				}
			}
			correctnessResult := CorrectnessResult{
				NumNodes:             params.NumNodes,
				NumSuccessors:        params.NumSuccessors,
				N:                    params.N,
				MinStabilizationTime: int(stabilizeMin),
				MaxStabilizationTime: int(stabilizeMax),
				EventFireDelay:       int(sleep),
				NumberOfFailures:     failures,
			}
			CorrectnessResults = append(CorrectnessResults, correctnessResult)
			sleep += time.Duration(params.EventFireDelaySteps) * time.Second
		}
		stabilizeMax += time.Duration(params.StabilizationTimeSteps) * time.Second
		stabilizeMin += time.Duration(params.StabilizationTimeSteps) * time.Second
	}
	return finalPass, nil
}


/*
	Check for the correctness invariants as given by Pamela Zave in her paper
	"Using Lightweight Modeling To Understand Chord"
 */
func CheckCorrectnessInvariants(ring *Ring) bool {
	ok := connectedAppendages(ring)
	if !ok {
		fmt.Println("Connected Appendages Invariant failed")
		logrus.Infoln("Connected Appendages Invariant failed")
		return false
	}
	ok = atleastOneRing(ring)
	if !ok {
		fmt.Println("Atleast One Ring Invariant failed")
		logrus.Infoln("Atleast One Ring Invariant failed")
		return false
	}
	ok = atmostOneRing(ring)
	if !ok {
		fmt.Println("Atmost One Ring Invariant failed")
		logrus.Infoln("Atmost One Ring Invariant failed")
		return false
	}
	ok = orderedRing(ring)
	if !ok {
		fmt.Println("Ordered Ring Invariant failed")
		logrus.Infoln("Ordered Ring Invariant failed")
		return false
	}
	return true
}

/*
	The following functions are based on the invariants mentioned in Pamela Zave's
	paper "Using Lightweight Modeling to Understand Chord"
	These functions check the truth value of the 4 correctness invariants on the ring:
	1. Connected Appendages
	2. At Least One Ring
	3. At Most One Ring
	4. Ordered Ring
*/
func connectedAppendages(ring *Ring) bool {
	/*
		An appendage is a node that is connected to the ring externally, that is by only one other node.
		Addition of a new node results in an appendage.
		This invariant asserts whether all the appendages are connected.
	*/
	succMap := make(map[string]*localVnode)
	for _, vnode := range ring.vnodes {
		succMap[(*vnode).Vnode.String()] = vnode
	}

	for i := range ring.vnodes {
		fail := true
		for j := range ring.vnodes[i].successors {
			if ring.vnodes[i].successors[j] != nil {
				if _, ok := succMap[ring.vnodes[i].successors[j].String()]; ok {
					fail = false
					break
				}
			}
		}
		if fail {
			return false
		}
	}

	return true
}

func atleastOneRing(ring *Ring) bool {
	/*
		This invariant asserts that the ring has at least one node, or in other words the ring exists.
	*/
	var ringList []string
	succMap := make(map[string]*localVnode)
	for _, vnode := range ring.vnodes {
		ringList = append(ringList, (*vnode).Vnode.String())
		succMap[(*vnode).Vnode.String()] = vnode
	}
	for i := range ring.vnodes {
		firstNode := ring.vnodes[i]
		node := (*firstNode).successors[0]
		count := 0
		for node != nil && count < len(ringList) {
			if node.Num == firstNode.Num {
				return true
			}
			if succMap[node.String()] == nil {
				node = nil
			} else {
				node = succMap[node.String()].successors[0]
			}
			count++
		}
	}
	return false
}

func atmostOneRing(ring *Ring) bool {
	/*
		This invariant asserts that only one ring is formed in the system.
	*/
	var succList []string
	var ringList []string
	succMap := make(map[string]*localVnode)
	for _, vnode := range ring.vnodes {
		ringList = append(ringList, (*vnode).Vnode.String())
		succMap[(*vnode).Vnode.String()] = vnode
	}
	firstNode := ring.vnodes[0]
	if firstNode == nil {
		return false
	}
	succList = append(succList, (*firstNode).Vnode.String())
	node := (*firstNode).successors[0]
	count := 0
	for node != nil && node.String() != (*firstNode).Vnode.String() && count < len(ringList) {
		succList = append(succList, node.String())
		if succMap[node.String()] == nil {
			node = nil
		} else {
			node = succMap[node.String()].successors[0]
		}
		count++
	}
	if len(succList) != len(ringList) {
		return false
	}
	return true
}

func orderedRing(ring *Ring) bool {
	/*
		This invariant asserts whether the ring is always ordered by identifiers.
	*/
	var succList []string
	var ringList []string
	succMap := make(map[string]*localVnode)
	for _, vnode := range ring.vnodes {
		ringList = append(ringList, (*vnode).Vnode.String())
		succMap[(*vnode).Vnode.String()] = vnode
	}
	firstNode := ring.vnodes[0]
	if firstNode == nil {
		return false
	}
	succList = append(succList, (*firstNode).Vnode.String())
	node := (*firstNode).successors[0]
	count := 0
	for node != nil && node.String() != (*firstNode).Vnode.String() && count < len(ringList) {
		succList = append(succList, node.String())
		if succMap[node.String()] == nil {
			node = nil
		} else {
			node = succMap[node.String()].successors[0]
		}
		count++
	}
	if len(ringList) != len(succList) {
		return false
	}
	for i := range ringList {
		if ringList[i] != succList[i] {
			return false
		}
	}
	return true
}


/*
	This function generates logs based on the metrics collected and
	checks performed during correctness testing.
*/
func LogCorrectness() {
	correctnessFile, err := os.Create("correctnessResults.csv")
	if err != nil {
		logrus.Errorln("Cannot create file", err.Error())
		return
	}
	defer correctnessFile.Close()

	correctnessWriter := csv.NewWriter(correctnessFile)

	var correctnessHeader []string
	correctnessHeader = append(correctnessHeader, "Number of Nodes")
	correctnessHeader = append(correctnessHeader, "Number of Successors")
	correctnessHeader = append(correctnessHeader, "Number of Runs")
	correctnessHeader = append(correctnessHeader, "Minimum Stabilization Time")
	correctnessHeader = append(correctnessHeader, "Maximum Stabilization Time")
	correctnessHeader = append(correctnessHeader, "Event Fire Delay")
	correctnessHeader = append(correctnessHeader, "Number of Failures")
	err = correctnessWriter.Write(correctnessHeader)
	if err != nil {
		logrus.Errorln("Unable to write correctness header:", err.Error())
		return
	}

	var correctnessData [][]string
	for _, correctnessResult := range CorrectnessResults {
		var data []string
		data = append(data, strconv.Itoa(correctnessResult.NumNodes))
		data = append(data, strconv.Itoa(correctnessResult.NumSuccessors))
		data = append(data, strconv.Itoa(correctnessResult.N))
		data = append(data, strconv.Itoa(correctnessResult.MinStabilizationTime / 1000000000))
		data = append(data, strconv.Itoa(correctnessResult.MaxStabilizationTime / 1000000000))
		data = append(data, strconv.Itoa(correctnessResult.EventFireDelay / 1000000000))
		data = append(data, strconv.Itoa(correctnessResult.NumberOfFailures))
		correctnessData = append(correctnessData, data)
	}

	for _, data := range correctnessData {
		err = correctnessWriter.Write(data)
		if err != nil {
			logrus.Errorln("Unable to write query record:", err.Error())
			return
		}
	}
	correctnessWriter.Flush()
}
