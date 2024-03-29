# Correcting, testing, and evaluating Chord in Go
<https://sites.google.com/a/stonybrook.edu/sbcs535/projects/correct-chord-go>

### 1. Simulation
We have found the test cases where the original chord implementation fails, reproduced the scenarios and tested the correctness of corrected chord on same scenarios. We have simulated four scenarios which produced interesting results. More details regarding scenarios, how they are reproduced and what are the parameters can be found in the project report (Project_17_Report.pdf) section 4.

#### Input
1. **Mode**: (value=“simulation”), this is to notify the driver program that it should run performance testing on the chord ring.
2. **Number of Nodes (numNodes)**: Number of nodes that the chord ring should be initialized with for simulation. All the scenarios are simulated on this number of nodes.
3. **Number of Events (n)**: This is the number of events to simulate “scenario 1”.

#### Output
The output is a log file named simulation_logs.txt which has details about which scenario is running, what is the state of the ring after each event happening in a scenario and details of invariants failed if any.

#### Sample Run
go run chord.go simulation 10 5 <br />
In the above example, <br />
numNodes = 10 <br />
n = 5

### 2. Correctness
As mentioned earlier, the pseudo-code given in the original Chord paper <https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf> fails the invariants: Connected Appendages, At Least One Ring, At Most One Ring, Ordered Ring, as mentioned in the paper <http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.304.5725&rep=rep1&type=pdf>. Pamela Zave in her paper “How To Make Chord Correct” <https://arxiv.org/pdf/1502.06461v2.pdf> suggested some changes to the original pseudo-code. We incorporated those changes and tested the correctness of Original and Corrected Implementations. More details can be found in the report (Project_17_Report.pdf) section 5.

#### Input
1. **Mode**: (value=“correctness”) This is to notify the driver program that it should run Correctness testing on the chord ring.
2. **Version**: (value=“old” or “new”) version of chord to test correctness for.
3. **Number of Nodes (numNodes)**: Number of nodes that the chord ring should be initialized with for correctness testing.
4. **Number of Successors (numSuccessors)**: Size of successor list of a node.
5. **Number of runs (n)**: The number of times the program will run for a set of parameters. 
6. **Minimum Stabilization Time (minST)**: The Lower limit of time after which a node will start to stabilize.
7. **Maximum Stabilization Time (maxST)**: The Upper limit of the time before which the node should stabilize.
8. **Stabilization Time Steps (sTS)**: The increase in Stabilization times for the next tests.
9. **Number of Stabilization Time Steps (nSTS)**: The time between the firing of successive events.
10. **Event Fire Delay (eFD)**: Time between the firing of successive events.
11. **Event Fire Delay Steps (eFDS)**: The increase in Event fire delay for next test.
12. **Number of Event Fire Delay Steps (nEFDS)**: The total number of Event Fire Delay Steps.

#### Output
The output is a csv file named correctnessResults.csv showing the input parameters and number of times invariants have failed for n runs. A log file named correctness_logs.txt is also generated which shows the traces of the event and state of the ring after the event.

#### Sample Run
go run chord.go correctness new 10 3 10 2 4 2 3 4 1 3 <br />
In the above example, <br />
version = “new” <br />
numNodes = 10 <br />
numSuccessors = 3 <br />
n = 1, minST = 2s <br />
maxST = 4s <br />
sTS = 2s <br />
nSTS = 3 <br />
eFD = 4s <br />
eFDS = 1s <br />
nEFDS = 3

### 3. Performance
Details about the metrics used to test the performance of chord protocol can be found in the report (Project_17_Report.pdf) section 6.

#### Input
1. **Mode**: (value=“performance”), this is to notify the driver program that it should run performance testing on the chord ring.
2. **Number of Nodes (numNodes)**: Number of nodes that the chord ring should be initialized with for performance testing.
3. **Number of Runs (n)**: This is the number of times a specific number of queries on which the performance is being tested should be run. Even though the number of queries are same, different keys (queries) are generated randomly and the performance is tested and throughput values are averaged over n runs. This is to simulate the real-time scenario where we don’t know what queries can come in.
4. **Number of Queries (nQ)**: This is the number of queries that should be generated by the testing module to test the performance of chord ring. Each number of queries is run n times and results are averaged out over n runs.
5. **Query Steps (qS)**: This is the number by which we increase the number of queries (nQ) after n sample runs on nQ. For example, if nQ = 1000 and qS = 100, then 1000 queries will be generated the first time, they will be run n times and results are averaged out over n runs. Next, testing will be done by generating 1100 queries, they will be run n times and the results are averaged out over n runs.
6. **Number of Query Steps (nQS)**: This is the number of times we increase the number of queries by query steps so that we will know when we should terminate the program. For example, if nQS = 10, nQ = 1000 and qS = 100, steps explained in inputs 3 and 4 are run on 1000 queries, then on 1100 queries and so on until number of queries goes till 1900.

#### Output
The outputs of running performance testing are two csv files and a logs text file. The csv files have metrics of cpu performance and query performance respectively and logs file has information about what node is found for a particular query.

#### Sample Run
go run chord.go performance 128 100 1000 100 20 <br />
In the above example, <br />
numNodes = 128 <br />
n = 100 <br />
nQ = 1000 <br />
qS = 100 <br />
nQS = 20 <br />
