package chord

import (
	"fmt"
	"github.com/ahrtr/logrus"
	"math/rand"
	"time"
	"os"
	"encoding/csv"
	"strconv"
)

type PerformanceParams struct {
	N             int
	NumQueries    int
	QuerySteps    int
	NumQuerySteps int
}

type CPUPerformance struct {
	/*
		CPU Utilization from a node will be stored in an object of this type
		Node (string): The identifier of the node creating the object
		TimeElapsed (time.Duration): CPU Utilization of the node
		Operation (string): The operation that the CPU Utilization corresponds to
		Configuration (Config): The details of the run (number of runs, number of events, order of events, etc.)
	*/
	Node          int
	TimeElapsed   time.Duration
	Operation     string
	Configuration Config
}

type QueryPerformance struct {
	/*
		Performance metric of batch queries is stored in an object of this type
		NumberOfQueries (int): The number of queries in the batch
		TimeElapsed (time.Duration): Time elapsed in processing the batch
	*/
	NumberOfQueries int
	TimeElapsed     time.Duration
	NumJumps        float64
	Lookups         float64
}

var CPUPerformanceMetrics []CPUPerformance
var QueryPerformanceMetrics []QueryPerformance
var CorrectnessResults []CorrectnessResult
var Events []Event
var States []State

type State struct {
	SingleState []Nodes
}

type Event struct {
	Name   string
	Cause  int
	Affect int
}

func InitPerformance() {
	rand.Seed(time.Now().UnixNano())
}

func GetEvents() []Event {
	return Events
}

func GetStates() []State {
	return States
}

func MetricsHandler(node int, timeElapsed time.Duration, operation string, config Config) {
	performance := CPUPerformance{
		Node:          node,
		TimeElapsed:   timeElapsed,
		Operation:     operation,
		Configuration: config,
	}
	CPUPerformanceMetrics = append(CPUPerformanceMetrics, performance)
}

/*
	This function collects the following performance metrics:
	1. Average Jump Number/Mean Path Length (as in Tsukamoto et. al.,
	   "Implementation and Evaluation of Distributed Hash Table Using MPI" and
	   Stoica et. al., "Chord: A scalable peer-to-peer lookup service for internet applications")
	2. Average Finger Table Lookup (as in Tsukamoto et. al.,
	   "Implementation and Evaluation of Distributed Hash Table Using MPI")
	3 Average Lookup Latency (as in Stoica et. al.,
	   "Chord: A scalable peer-to-peer lookup service for internet applications")
	4. Average Stabilization Time
)
*/
func (r *Ring) TestPerformance(params PerformanceParams) {
	for i := 0; i < params.NumQuerySteps; i++ {
		start := time.Now()
		jumps := 0
		lookups := 0
		for j := 0; j < params.N; j++ {
			queries := generateQueries(params.NumQueries)
			for _, query := range queries {
				successors, val, lookup, err := r.vnodes[0].FindSuccessors(1, []byte(query))
				if err != nil {
					logrus.Errorln("Cannot find successors:", err.Error())
				} else {
					logrus.Infof("Node %d found for key %s", successors[0].Num, query)
				}
				jumps += val
				lookups += lookup
			}
		}
		jumpsFloat := float64(jumps) / float64(params.NumQueries * params.N)
		lookupsFloat := float64(lookups) / float64(params.NumQueries * params.N)
		queryPerformanceMetric := QueryPerformance{
			NumberOfQueries: params.NumQueries,
			TimeElapsed:     time.Since(start),
			NumJumps:        jumpsFloat,
			Lookups:         lookupsFloat,
		}
		QueryPerformanceMetrics = append(QueryPerformanceMetrics, queryPerformanceMetric)
		params.NumQueries += params.QuerySteps
	}
	time.Sleep(20 * time.Second)
}

/*
	Random queries are generated for simulation.
	Input:
		num (int): Number of queries required
*/
func generateQueries(num int) []string {
	var queries []string
	for i := 0; i < num; i++ {
		queries = append(queries, RandStringRunes(8))
	}
	return queries
}

func RandStringRunes(n int) string {
	/*
		Generates a random string of required length
		Input:
			n (int): Length of the string required
		Output:
			b (string): A random string of length n
	*/
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

/*
	This function is used to generate logs regarding query performance and CPU utilization
	of the run.
	Results are saved in queryPerformance.csv and cpuPerformance.csv
*/
func LogStats(num int, numNodes int) {
	queryFile, err := os.Create("queryPerformance.csv")
	if err != nil {
		logrus.Errorln("Cannot create file", err.Error())
		return
	}
	defer queryFile.Close()

	cpuFile, err := os.Create("cpuPerformance.csv")
	if err != nil {
		logrus.Errorln("Cannot create file", err.Error())
		return
	}
	defer cpuFile.Close()

	queryWriter := csv.NewWriter(queryFile)

	var queryHeader []string
	queryHeader = append(queryHeader, "Number of Nodes")
	queryHeader = append(queryHeader, "Number of Queries (nQ)")
	queryHeader = append(queryHeader, "Number of Runs (n)")
	queryHeader = append(queryHeader, "Average Lookup Latency (for all queries)")
	queryHeader = append(queryHeader, "Average Lookup Latency (per query)")
	queryHeader = append(queryHeader, "Average Jump Number")
	queryHeader = append(queryHeader, "Average Lookup Finger Table Number")
	err = queryWriter.Write(queryHeader)
	if err != nil {
		logrus.Errorln("Unable to write query header:", err.Error())
		return
	}

	var queryData [][]string
	for _, queryPerformance := range QueryPerformanceMetrics {
		var data []string
		data = append(data, strconv.Itoa(numNodes))
		data = append(data, strconv.Itoa(queryPerformance.NumberOfQueries))
		data = append(data, strconv.Itoa(num))
		data = append(data, strconv.Itoa(int(queryPerformance.TimeElapsed) / num))
		data = append(data, strconv.Itoa(int(queryPerformance.TimeElapsed)/(queryPerformance.NumberOfQueries * num)))
		data = append(data, fmt.Sprintf("%f", queryPerformance.NumJumps))
		data = append(data, fmt.Sprintf("%f", queryPerformance.Lookups))
		queryData = append(queryData, data)
	}

	for _, data := range queryData {
		err = queryWriter.Write(data)
		if err != nil {
			logrus.Errorln("Unable to write query record:", err.Error())
			return
		}
	}
	queryWriter.Flush()

	cpuWriter := csv.NewWriter(cpuFile)

	var cpuHeader []string
	cpuHeader = append(cpuHeader, "Node")
	cpuHeader = append(cpuHeader, "Number of schedules")
	cpuHeader = append(cpuHeader, "Average Schedule Time")
	cpuHeader = append(cpuHeader, "Number of stabilizations")
	cpuHeader = append(cpuHeader, "Average Stabilization Time")
	err = cpuWriter.Write(cpuHeader)
	if err != nil {
		logrus.Errorln("Unable to write cpu header:", err.Error())
		return
	}

	var cpuData [][]string
	nodeScheduleMap := make(map[int]time.Duration)
	nodeScheduleNumberMap := make(map[int]int)
	totalScheduleTime := time.Duration(0)
	schedule := 0
	nodeStabilizationMap := make(map[int]time.Duration)
	nodeStabilizationNumberMap := make(map[int]int)
	totalStabilizationTime := time.Duration(0)
	stabilization := 0
	for _, cpuPerformance := range CPUPerformanceMetrics {
		if cpuPerformance.Operation == "schedule" {
			if val, ok := nodeScheduleMap[cpuPerformance.Node]; ok {
				val += cpuPerformance.TimeElapsed
				nodeScheduleMap[cpuPerformance.Node] = val
				nodeScheduleNumberMap[cpuPerformance.Node] = nodeScheduleNumberMap[cpuPerformance.Node] + 1
			} else {
				nodeScheduleMap[cpuPerformance.Node] = cpuPerformance.TimeElapsed
				nodeScheduleNumberMap[cpuPerformance.Node] = 1
			}
			totalScheduleTime += cpuPerformance.TimeElapsed
			schedule++
		} else if cpuPerformance.Operation == "stabilization" {
			if val, ok := nodeStabilizationMap[cpuPerformance.Node]; ok {
				val += cpuPerformance.TimeElapsed
				nodeStabilizationMap[cpuPerformance.Node] = val
				nodeStabilizationNumberMap[cpuPerformance.Node] = nodeStabilizationNumberMap[cpuPerformance.Node] + 1
			} else {
				nodeStabilizationMap[cpuPerformance.Node] = cpuPerformance.TimeElapsed
				nodeStabilizationNumberMap[cpuPerformance.Node] = 1
			}
			totalStabilizationTime += cpuPerformance.TimeElapsed
			stabilization++
		}
	}

	for node, value := range nodeScheduleMap {
		var data []string
		data = append(data, strconv.Itoa(node))
		data = append(data, strconv.Itoa(nodeScheduleNumberMap[node]))
		data = append(data, strconv.Itoa(int(value)/nodeScheduleNumberMap[node]))
		data = append(data, strconv.Itoa(nodeStabilizationNumberMap[node]))
		data = append(data, strconv.Itoa(int(nodeStabilizationMap[node])/nodeStabilizationNumberMap[node]))
		cpuData = append(cpuData, data)
	}

	var data []string
	data = append(data, "All")
	data = append(data, strconv.Itoa(schedule))
	data = append(data, strconv.Itoa(int(totalScheduleTime)/schedule))
	data = append(data, strconv.Itoa(stabilization))
	data = append(data, strconv.Itoa(int(totalStabilizationTime)/stabilization))
	cpuData = append(cpuData, data)

	for _, data := range cpuData {
		err = cpuWriter.Write(data)
		if err != nil {
			logrus.Errorln("Unable to write cpu record:", err.Error())
			return
		}
	}
	cpuWriter.Flush()

	return
}
