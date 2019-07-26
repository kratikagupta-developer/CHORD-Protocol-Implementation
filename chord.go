package main

import (
	"bufio"
	"correct-chord-go/chord"
	"fmt"
	"github.com/ahrtr/logrus"
	"os"
	"strconv"
	"strings"
)

func main() {

	/*
		This is the entry point of the project. 4 modes are possible, which is described in detail later on.
		1. DHT
		2. Simulation
		3. Correctness Testing
		4. Performance Testing
	 */

	arguments := os.Args[1:]
	caseRunning := ""
	if len(arguments) == 0 {
		caseRunning = "dht"
	} else {
		caseRunning = arguments[0]
		if caseRunning != "dht" && caseRunning != "correctness" && caseRunning != "simulation" && caseRunning != "performance" {
			fmt.Println("Unknown argument for the case to run.")
			return
		}
	}

	config := chord.DefaultConfig("local")
	ring, err := chord.Create(config, nil)
	if err != nil {
		fmt.Println("error in creating ring:", err.Error())
		return
	}
	fmt.Println(ring)
	for i := 0; i < 10; i++ {
		nodes, err := ring.Lookup(1, []byte("any thing to test"))
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Println(nodes)
	}

	/*
		The application supports four modes, namely DHT, Simulation, Correctness, and Performance. They as described as below:

		1. DHT (dht)
			Runs a command line interface to interact with the Distributed Hash Table.
			It understands the following commands:
			a. GET (Input: <key>, Output: <value>)
				- Performs ring lookup for the <key> provided.
				- Returns the node that may contain the key based on the hash function.
				- Looks for the <key:value> pair in the DataStore of that node.
				- Returns the value if it finds the key
				- Returns "Key Not Found" if the key doesn't exist.
			b. SET (Input: <key> <value>, Output: True/False)
				- Performs ring lookup for the <key> provided.
				- Returns the node that may contain the key based on the hash function.
				- Sets (or overwrites if the key exists) the provided key to the provided value.
			c. DELETE (Input: <key>, Output: True/False)
				- Performs ring lookup for the <key> provided.
				- Returns the node that may contain the key based on the hash function.
				- Deletes the key if it is present on the said node.
				- Doesn't do anything if the key is not found.

		2. Simulation
			Input: [Mode="simulation", numNodes, numEvents]
			Output: [simulation_logs.txt]
			- Will execute a set of queries and show the trace for accessing of nodes for each one of them.

		3. Correctness (correctness)
		    Input: [Mode="correctness", version, numNodes, numSuccessors, numRuns, MinStabilizationTime, MaxStabilizationTime,
			StabilizationTimeSteps, nStabilizationTimeSteps, EventFireDelay, EventFireDelaySteps, NumberEventFireDelaySteps]
			Output: [correctnessResults.csv, correctness_logs.txt]
			- Constructs various chord rings based on the different configurations provided.
			- For each ring that is generated, a series of random events is fired at variable time periods.
			- The random events under consideration are JOIN, LEAVE, FAILURE.
			- LEAVE and FAILURE are called on random nodes each time.
			- JOIN operation adds a new node to the ring.
			- Varying the stabilization time and the intervals between firing of events, this section evaluates
			  the correctness of the ring after all the events are finished.
			- Correctness is determined by the following invariants explained in detail in correctness.go:
			  a. Connected Appendages
			  b. At least one Ring
			  c. Ordered Ring
			  d. At most one Ring
			- Key-Data consistency is also tested using the following invariants:
			  a. Ordered Merges
			  b. Ordered Appendages
			  c. Valid Successor List
			- Logs generated show the sequence of events, final ring state, and the invariants that were violated in the run.

		4. Performance (performance)
		   Input: [mode="performance", numNodes, numRuns, NumQueries, querySteps, NumQuerySteps]
		   Output: [cpuPerformance.csv, queryPerformance.csv]
		   Performance will be evaluated on the following metrics:
		   a. CPU Time: The time taken by the ring to stabilize.
		   b. Average Jump Number: The mean length of the paths followed to retrieve a particular node.
		   c. Average Finger Table Lookup: The average number of lookups made in the finger table of each node.
		   d. Query Performance: The time taken to execute the queries GET, SET, and DELETE.

		   Each run generates a number of objects that store logs such as CPU time, total elapsed time, etc.
		   At the end of the run, a STATS() (name not final) function consolidates the information generated and
		   presents it as tables.

	*/
	if caseRunning == "dht" {
		for {
			//ring.PrintData()
			fmt.Print("dht>")
			reader := bufio.NewReader(os.Stdin)
			command, _ := reader.ReadString('\n')
			command = strings.TrimSuffix(command, "\n")
			carr := strings.Split(command, " ")
			if len(carr) == 0 {
				fmt.Println("Enter a valid command")
			}
			switch carr[0] {
			case "GET":
				if len(carr) != 2 {
					fmt.Println("Invalid Get Command")
				}
				nodes, err := ring.Lookup(1, []byte(carr[1]))
				if err != nil {
					fmt.Println("Error in Lookup:", err.Error())
				}
				localNode, err := ring.GetLocalNode(nodes[0])
				if err != nil {
					fmt.Println("Node not found")
				}
				value, err := localNode.DataStore.Get(carr[1])
				if err != nil {
					fmt.Println(err.Error())
				} else {
					fmt.Println(string(value))
				}
				break
			case "SET":
				if len(carr) != 3 {
					fmt.Println("Invalid Set Command")
				}
				nodes, err := ring.Lookup(1, []byte(carr[1]))
				if err != nil {
					fmt.Println("Error in Lookup:", err.Error())
				}
				localNode, err := ring.GetLocalNode(nodes[0])
				if err != nil {
					fmt.Println("Node not found")
				}
				err = localNode.DataStore.Set(carr[1], carr[2])
				if err != nil {
					fmt.Println(err.Error())
				} else {
					fmt.Println("True")
				}
				break
			case "DELETE":
				if len(carr) != 2 {
					fmt.Println("Invalid Delete Command")
				}
				nodes, err := ring.Lookup(1, []byte(carr[1]))
				if err != nil {
					fmt.Println("Error in Lookup:", err.Error())
				}
				localNode, err := ring.GetLocalNode(nodes[0])
				if err != nil {
					fmt.Println("Node not found")
				}
				err = localNode.DataStore.Delete(carr[1])
				if err != nil {
					fmt.Println(err.Error())
				} else {
					fmt.Println("True")
				}
				break
			}
		}
	} else if caseRunning == "simulation" {
		filename := "simulation_logs.txt"
		f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0755)
		if err != nil {
			fmt.Println("Couldn't open file, got: ", err.Error())
			return
		}
		formatter := &logrus.JSONFormatter{
			DisableTimestamp: true,
		}
		logrus.SetFormatter(formatter)
		logrus.SetOutput(f)
		nN, _ := strconv.Atoi(arguments[1])
		n, _ := strconv.Atoi(arguments[2])
		config := chord.DefaultConfig("local")
		config.NumVnodes = nN
		config.NumSuccessors = 3
		ring, err := chord.Create(config, nil)
		if err != nil {
			fmt.Println("error in creating ring:", err.Error())
			return
		}
		ring.Simulate(n, nN)
	} else if caseRunning == "correctness" {
		//This check will be run on multiple values changing the stabilization time and other parameters.
		filename := "correctness_logs.txt"
		f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0755)
		if err != nil {
			fmt.Println("Couldn't open file, got: ", err.Error())
			return
		}
		formatter := &logrus.TextFormatter{
			DisableQuoteFields: true,
			DisableKeyFields:   true,
		}
		logrus.SetFormatter(formatter)
		logrus.SetOutput(f)
		chord.InitPerformance()
		version := arguments[1]
		if version != "new" && version != "old" {
			fmt.Println("Not a valid version to test for correctness")
			return
		}
		nN, _ := strconv.Atoi(arguments[2])
		numSuccessors, _ := strconv.Atoi(arguments[3])
		n, _ := strconv.Atoi(arguments[4])
		minST, _ := strconv.Atoi(arguments[5])
		maxST, _ := strconv.Atoi(arguments[6])
		sTS, _ := strconv.Atoi(arguments[7])
		nSTS, _ := strconv.Atoi(arguments[8])
		eFD, _ := strconv.Atoi(arguments[9])
		eFDS, _ := strconv.Atoi(arguments[10])
		nEFDS, _ := strconv.Atoi(arguments[11])
		params := chord.CorrectnessParams{
			Version:                   version,
			NumNodes:                  nN,
			NumSuccessors:             numSuccessors,
			N:                         n,
			MinStabilizationTime:      minST,
			MaxStabilizationTime:      maxST,
			StabilizationTimeSteps:    sTS,
			NumberStabilizationSteps:  nSTS,
			EventFireDelay:            eFD,
			EventFireDelaySteps:       eFDS,
			NumberEventFireDelaySteps: nEFDS,
		}
		pass, err := chord.TestCorrectness(params)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Println(pass)
		fmt.Println(chord.CorrectnessResults)
		chord.LogCorrectness()
	} else if caseRunning == "performance" {
		filename := "performance_logs.txt"
		f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0755)
		if err != nil {
			fmt.Println("Couldn't open file, got: ", err.Error())
			return
		}
		formatter := &logrus.TextFormatter {
			DisableQuoteFields: true,
			DisableKeyFields:   true,
		}
		logrus.SetFormatter(formatter)
		logrus.SetOutput(f)
		chord.InitPerformance()
		nN, _ := strconv.Atoi(arguments[1])
		n, _ := strconv.Atoi(arguments[2])
		nQ, _ := strconv.Atoi(arguments[3])
		qS, _ := strconv.Atoi(arguments[4])
		nQS, _ := strconv.Atoi(arguments[5])

		config := chord.DefaultConfig("local")
		config.NumVnodes = nN
		ring, err := chord.Create(config, nil)
		if err != nil {
			fmt.Println("error in creating ring:", err.Error())
			return
		}

		params := chord.PerformanceParams{
			N:             n,
			NumQueries:    nQ,
			QuerySteps:    qS,
			NumQuerySteps: nQS,
		}
		ring.TestPerformance(params)
		logrus.Infoln(chord.QueryPerformanceMetrics)
		logrus.Infoln(chord.CPUPerformanceMetrics)
		chord.LogStats(n, nN)
		logrus.Infoln(ring.PrintNodes())
	}
}
