package chord

import (
	"fmt"
	"github.com/ahrtr/logrus"
	"time"
	"sort"
)

/*
	This function is used to simulate the 4 scenarios as mentioned in the report.
	It sequentially calls simulateScenario1, simulateScenario2, simulateScenario3,
	and simulateScenario4 to achieve this.
*/
func (r *Ring) Simulate(n, nN int) {
	//Scenario 1
	r.simulateScenario1(n)
	ring, err := createRing(nN)
	if err != nil {
		fmt.Println("Cannot create ring for second scenario")
		return
	}
	ring.simulateScenario2()
	logrus.Infoln("Invariants test result", CheckCorrectnessInvariants(r))
	ring, err = createRing(nN)
	if err != nil {
		fmt.Println("Cannot create ring for third scenario")
		return
	}
	ring.simulateScenario3()
	logrus.Infoln("Invariants test result", CheckCorrectnessInvariants(r))

	ring, err = createRing(nN)
	if err != nil {
		fmt.Println("Cannot create ring for fourth scenario")
		return
	}
	ring.simulateScenario4()
	logrus.Infoln("Invariants test result", CheckCorrectnessInvariants(r))
}


/*
	This function simulates the first scenario that is mentioned in the report.
*/
func (r *Ring) simulateScenario1(n int) {
	time.Sleep(10 * time.Second)
	logrus.Infoln("Scenario 1 started")
	logrus.Infoln(r.PrintNodes())
	pass := r.CheckCorrectness(n, 5*time.Second, "old")
	if !pass {
		fmt.Println("Invariants failed")
	}
	logrus.Infoln("Scenario 1 Ended")
	return
}

/*
	This function simulates the second scenario that is mentioned in the report.
*/
func (r *Ring) simulateScenario2() {
	time.Sleep(10 * time.Second)
	logrus.Infoln("Scenario 2 started")
	logrus.Infoln(r.PrintNodes())
	// JOIN a node
	id := len(r.vnodes)
	vn := &localVnode{}
	vn.ring = r
	vn.init(id)
	id++
	logrus.Infof("Join New %d requested %d", vn.Num, r.vnodes[0].Num)
	successor, err := vn.join(r.vnodes[0])
	if err != nil {
		logrus.Errorln("could not join the ring, found no valid successor")
		return
	}
	r.vnodes = append(r.vnodes, vn)
	go r.scheduleNode(vn)
	sort.Sort(r)
	logrus.Infoln(r.PrintNodes())

	// Successor Node leaves before joined node stabilizes
	val := -1
	for i := range r.vnodes {
		if r.vnodes[i].Num == successor.Num {
			logrus.Infoln("leave", r.vnodes[i].Num)
			r.vnodes[i].leave()
			val = i
			break
		}
	}
	r.vnodes = append(r.vnodes[:val], r.vnodes[val+1:]...)

	//Give enough time for stabilization and log the nodes
	time.Sleep(20 * time.Second)
	logrus.Infoln(r.PrintNodes())
	logrus.Infoln("Scenario 2 Ended")

	return
}

/*
	This function simulates the third scenario that is mentioned in the report.
*/
func (r *Ring) simulateScenario3() {
	time.Sleep(10 * time.Second)
	logrus.Infoln("Scenario 3 started")
	logrus.Infoln(r.PrintNodes())
	// JOIN a node
	id := len(r.vnodes)
	vn := &localVnode{}
	vn.ring = r
	vn.init(id)
	id++
	logrus.Infof("Join New %d requested %d", vn.Num, r.vnodes[0].Num)
	successor, err := vn.joinNew(r.vnodes[0])
	if err != nil {
		logrus.Errorln("could not join the ring, found no valid successor")
		return
	}
	r.vnodes = append(r.vnodes, vn)
	go r.scheduleNode(vn)
	sort.Sort(r)
	logrus.Infoln(r.PrintNodes())

	// Successor Node leaves before joined node stabilizes
	val := -1
	for i := range r.vnodes {
		if r.vnodes[i].Num == successor.Num {
			logrus.Infoln("leave", r.vnodes[i].Num)
			r.vnodes[i].leave()
			val = i
			break
		}
	}
	r.vnodes = append(r.vnodes[:val], r.vnodes[val+1:]...)

	//Give enough time for stabilization and log the nodes
	time.Sleep(20 * time.Second)
	logrus.Infoln(r.PrintNodes())
	logrus.Infoln("Scenario 3 Ended")

	return
}

/*
	This function simulates the fourth scenario that is mentioned in the report.
*/
func (r *Ring) simulateScenario4() {
	time.Sleep(10 * time.Second)
	logrus.Infoln("Scenario 4 Started")
	logrus.Infoln(r.PrintNodes())
	var successors []int
	for i := range r.vnodes[0].successors {
		successors = append(successors, r.vnodes[0].successors[i].Num)
	}

	count := 0
	for count < 3 {
		for i := range r.vnodes {
			if r.vnodes[i].Num == successors[count] {
				logrus.Infoln("leave", r.vnodes[i].Num)
				r.vnodes[i].leave()
				count++
				r.vnodes = append(r.vnodes[:i], r.vnodes[i+1:]...)
				break
			}
		}
	}

	time.Sleep(20 * time.Second)
	logrus.Infoln(r.PrintNodes())
	logrus.Infoln("Scenario 4 Ended")
	return
}

func createRing(nN int) (*Ring, error) {
	config := DefaultConfig("local")
	config.StabilizeMin = 5 * time.Second
	config.StabilizeMax = 10 * time.Second
	config.NumSuccessors = 3
	config.NumVnodes = nN

	ring, err := Create(config, nil)
	if err != nil {
		fmt.Println("error in creating ring:", err.Error())
		return nil, err
	}

	return ring, nil
}
