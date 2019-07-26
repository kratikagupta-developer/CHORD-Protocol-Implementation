package chord

import (
	"bytes"
	"correct-chord-go/global"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"time"
	"github.com/ahrtr/logrus"
	"math/rand"
)

// Represents an Vnode, local or remote
type Vnode struct {
	Num  int
	Id   []byte // Virtual ID
	Host string // Host identifier
}

// Represents a local Vnode
type localVnode struct {
	Vnode
	ring        *Ring
	successors  []*Vnode
	finger      []*Vnode
	last_finger int
	predecessor *Vnode
	stabilized  time.Time
	timer       *time.Timer
	DataStore   Storage
	Shutdown    bool
}

// Converts the ID to string
func (vn *Vnode) String() string {
	return fmt.Sprintf("%x", vn.Id)
}

// Initializes a local vnode
func (vn *localVnode) init(idx int) {
	// Generate an ID
	vn.genId(uint16(idx))
	vn.Num = idx

	// Set our host
	vn.Host = vn.ring.config.Hostname

	// Initialize all state
	vn.successors = make([]*Vnode, vn.ring.config.NumSuccessors)
	vn.finger = make([]*Vnode, vn.ring.config.hashBits)

	// Register with the RPC mechanism
	vn.ring.transport.Register(&vn.Vnode, vn)
	vn.DataStore = NewDataStore(vn.ring.config.HashFunc)
}

// Schedules the Vnode to do regular maintenence
func (vn *localVnode) schedule(fail chan bool) {
	// Setup our stabilize timer
	defer vn.sendTimeToPerformanceMonitor(time.Now(), "schedule")
	f := func() {
		vn.stabilize(fail)
	}
	vn.timer = time.AfterFunc(RandStabilize(vn.ring.config), f)
}

func (vn *localVnode) sendTimeToPerformanceMonitor(start time.Time, key string) {
	MetricsHandler(vn.Num, time.Since(start), key, *vn.ring.config)
}

// Generates an ID for the chord
func (vn *localVnode) genId(idx uint16) {
	// Use the hash funciton
	conf := vn.ring.config
	hash := conf.HashFunc()
	hash.Write([]byte(conf.Hostname))
	binary.Write(hash, binary.BigEndian, idx)

	// Use the hash as the ID
	vn.Id = hash.Sum(nil)
}

// Called to periodically stabilize the vnode
func (vn *localVnode) stabilize(fail chan bool) {
	start := time.Now()
	// Clear the timer
	vn.timer = nil

	// Check for shutdown
	if vn.ring.shutdown != nil {
		vn.ring.shutdown <- true
		return
	}

	if vn.Shutdown {
		return
	}

	defer vn.sendTimeToPerformanceMonitor(start, "stabilization")
	// Setup the next stabilize timer
	defer vn.schedule(fail)

	// Check for new successor
	if err := vn.checkNewSuccessor(fail); err != nil {
		log.Printf("[ERR] Error checking for new successor: %s", err)
	}

	// Notify the successor
	if err := vn.notifySuccessor(); err != nil {
		log.Printf("[ERR] Error notifying successor: %s", err)
	}

	// Finger table fix up
	if err := vn.fixFingerTable(); err != nil {
		log.Printf("[ERR] Error fixing finger table: %s", err)
	}

	// Check the predecessor
	if err := vn.checkPredecessor(); err != nil {
		log.Printf("[ERR] Error checking predecessor: %s", err)
	}

	// Set the last stabilized time
	vn.stabilized = time.Now()
}

// Checks for a new successor
func (vn *localVnode) checkNewSuccessor(fail chan bool) error {
	// Ask our successor for it's predecessor
	trans := vn.ring.transport

CHECK_NEW_SUC:
	succ := vn.successors[0]
	if succ == nil {
		//panic("Node has no successor!")
		fmt.Println("Node has no successor!")
		if vn.predecessor == nil {
			fail <- true
		}
		return errors.New("node has no successor" + vn.String())
	}
	maybe_suc, err := trans.GetPredecessor(succ)
	if err != nil {
		// Check if we have succ list, try to contact next live succ
		known := vn.knownSuccessors()
		if known > 1 {
			for i := 0; i < known; i++ {
				if alive, _ := trans.Ping(vn.successors[0]); !alive {
					// Don't eliminate the last successor we know of
					if i+1 == known {
						return fmt.Errorf("All known successors dead!")
					}

					// Advance the successors list past the dead one
					copy(vn.successors[0:], vn.successors[1:])
					vn.successors[known-1-i] = nil
				} else {
					// Found live successor, check for new one
					goto CHECK_NEW_SUC
				}
			}
		}
		return err
	}

	// Check if we should replace our successor
	if maybe_suc != nil && global.Between(vn.Id, succ.Id, maybe_suc.Id) {
		// Check if new successor is alive before switching
		//alive, err := trans.Ping(maybe_suc)
		if err == nil {
			vn.successors[0] = maybe_suc
			successors, _, _, err := trans.FindSuccessors(maybe_suc, vn.ring.config.NumSuccessors-1, maybe_suc.Id)
			if err != nil {
				return err
			}
			copy(vn.successors[1:], successors[:vn.ring.config.NumSuccessors-1])
		} else {
			return err
		}
	}
	return nil
}

// RPC: Invoked to return out predecessor
func (vn *localVnode) GetPredecessor() (*Vnode, error) {
	return vn.predecessor, nil
}

// Notifies our successor of us, updates successor list
func (vn *localVnode) notifySuccessor() error {
	// Notify successor
	succ := vn.successors[0]
	if succ == nil {
		return errors.New("successor dead")
	}
	succ_list, err := vn.ring.transport.Notify(succ, &vn.Vnode)
	if err != nil {
		return err
	}

	// Trim the successors list if too long
	max_succ := vn.ring.config.NumSuccessors
	if len(succ_list) > max_succ-1 {
		succ_list = succ_list[:max_succ-1]
	}

	// Update local successors list
	for idx, s := range succ_list {
		if s == nil {
			break
		}
		// Ensure we don't set ourselves as a successor!
		if s == nil || s.String() == vn.String() {
			break
		}
		vn.successors[idx+1] = s
	}
	return nil
}

// RPC: Notify is invoked when a Vnode gets notified
func (vn *localVnode) Notify(maybe_pred *Vnode) ([]*Vnode, error) {
	// Check if we should update our predecessor
	if vn.predecessor == nil || global.Between(vn.predecessor.Id, vn.Id, maybe_pred.Id) {
		// Inform the delegate
		conf := vn.ring.config
		old := vn.predecessor
		vn.ring.invokeDelegate(func() {
			conf.Delegate.NewPredecessor(&vn.Vnode, maybe_pred, old)
		})

		vn.predecessor = maybe_pred
	}

	// Return our successors list
	return vn.successors, nil
}

// Fixes up the finger table
func (vn *localVnode) fixFingerTable() error {
	// Determine the offset
	hb := vn.ring.config.hashBits
	offset := global.PowerOffset(vn.Id, vn.last_finger, hb)

	// Find the successor
	nodes, _, _, err := vn.FindSuccessors(1, offset)
	if nodes == nil || len(nodes) == 0 || err != nil {
		return err
	}
	node := nodes[0]
	if node == nil {
		return errors.New("no known successors")
	}

	// Update the finger table
	vn.finger[vn.last_finger] = node

	// Try to skip as many finger entries as possible
	for {
		next := vn.last_finger + 1
		if next >= hb {
			break
		}
		offset := global.PowerOffset(vn.Id, next, hb)

		// While the chord is the successor, update the finger entries
		if global.BetweenRightIncl(vn.Id, node.Id, offset) {
			vn.finger[next] = node
			vn.last_finger = next
		} else {
			break
		}
	}

	// Increment to the index to repair
	if vn.last_finger+1 == hb {
		vn.last_finger = 0
	} else {
		vn.last_finger++
	}

	return nil
}

// Checks the health of our predecessor
func (vn *localVnode) checkPredecessor() error {
	// Check predecessor
	if vn.predecessor != nil {
		res, err := vn.ring.transport.Ping(vn.predecessor)
		if err != nil {
			return err
		}

		// Predecessor is dead
		if !res {
			vn.predecessor = nil
		}
	}
	return nil
}

// Finds next N successors. N must be <= NumSuccessors
func (vn *localVnode) FindSuccessors(n int, key []byte) ([]*Vnode, int, int, error) {
	// Check if we are the immediate predecessor
	if bytes.Compare(key, vn.Id) == 0 || vn.successors[0] == nil {
		return vn.successors[:n], 1, rand.Intn(3) + vn.ring.config.NumSuccessors - 3, nil
	}
	if vn.successors[0] != nil && global.BetweenRightIncl(vn.Id, vn.successors[0].Id, key) {
		return vn.successors[:n], 1, rand.Intn(3) + vn.ring.config.NumSuccessors - 3, nil
	}
	//if vn.successors[len(vn.successors)-1] != nil && bytes.Compare(key, vn.successors[len(vn.successors)-1].Id) == 1 {
	//	res, val, fingerLookup, err := vn.ring.transport.FindSuccessors(vn.successors[len(vn.successors)-1], n, key)
	//	if err == nil {
	//		return res, 1+val, fingerLookup, nil
	//	} else {
	//		log.Printf("[ERR] Failed to contact %s. Got %s", vn.successors[len(vn.successors)-1].String(), err)
	//	}
	//}

	// Try the closest preceeding nodes
	cp := closestPreceedingVnodeIterator{}
	cp.init(vn, key)
	for {
		// Get the next closest chord
		closest, _ := cp.Next()
		if closest == nil {
			break
		}

		// Try that chord, break on success
		res, val, _, err := vn.ring.transport.FindSuccessors(closest, n, key)
		if err == nil {
			return res, 1+val, (1+val)*(rand.Intn(3) + vn.ring.config.NumSuccessors - 3), nil
		} else {
			log.Printf("[ERR] Failed to contact %s. Got %s", closest.String(), err)
		}
	}

	// Determine how many successors we know of
	successors := vn.knownSuccessors()

	// Check if the ID is between us and any non-immediate successors
	for i := 1; i <= successors-n; i++ {
		if global.BetweenRightIncl(vn.Id, vn.successors[i].Id, key) {
			remain := vn.successors[i:]
			if len(remain) > n {
				remain = remain[:n]
			}
			return remain, 1, 0, nil
		}
	}

	// Checked all closer nodes and our successors!
	return nil, 0, 0, fmt.Errorf(vn.Vnode.String() + ": Exhausted all preceeding nodes! and %d", successors)
}

// Instructs the vnode to leave
func (vn *localVnode) leave() error {
	// Inform the delegate we are leaving
	conf := vn.ring.config
	pred := vn.predecessor
	succ := vn.successors[0]
	vn.ring.invokeDelegate(func() {
		conf.Delegate.Leaving(&vn.Vnode, pred, succ)
	})

	// Notify predecessor to advance to their next successor
	var err error
	trans := vn.ring.transport
	if vn.predecessor != nil {
		err = trans.SkipSuccessor(vn.predecessor, &vn.Vnode)
	}
	//go vn.deregister()

	// Notify successor to clear old predecessor
	if vn.successors[0] != nil {
		err = trans.ClearPredecessor(vn.successors[0], &vn.Vnode)
	}
	vn.Shutdown = true
	vn.ring.transport.Deregister(&vn.Vnode)
	return err
}

func (vn *localVnode) deregister() {
	time.Sleep(2 * time.Second)
	vn.ring.transport.Deregister(&((*vn).Vnode))
}

func (vn *localVnode) join(node *localVnode) (*Vnode, error) {
	successors, _, _, err := node.FindSuccessors(1, vn.Id)
	if err != nil {
		return nil, err
	}
	(*vn).successors[0] = successors[0]
	successor := -1
	if successors[0] != nil {
		successor = successors[0].Num
	} else {
		return nil, errors.New("no successors found")
	}
	logrus.Infof("Node %d joining before Node %d", vn.Num, successor)
	return successors[0], nil
}

func (vn *localVnode) joinNew(node *localVnode) (*Vnode, error) {
	successors, _, _, err := node.FindSuccessors(vn.ring.config.NumSuccessors, vn.Id)
	if err != nil {
		return nil, err
	}
	for i := range successors {
		(*vn).successors[i] = successors[i]
	}
	logrus.Infof("Node %d joining before Node %d", vn.Num, successors[0].Num)
	return successors[0], nil
}

func (vn *localVnode) fail() error {
	vn.Shutdown = true
	vn.ring.transport.Deregister(&vn.Vnode)
	return nil
}

// Used to clear our predecessor when a chord is leaving
func (vn *localVnode) ClearPredecessor(p *Vnode) error {
	if vn.predecessor != nil && vn.predecessor.String() == p.String() {
		// Inform the delegate
		conf := vn.ring.config
		old := vn.predecessor
		vn.ring.invokeDelegate(func() {
			conf.Delegate.PredecessorLeaving(&vn.Vnode, old)
		})
		vn.predecessor = nil
	}
	return nil
}

// Used to skip a successor when a chord is leaving
func (vn *localVnode) SkipSuccessor(s *Vnode) error {
	// Skip if we have a match
	if vn.successors[0].String() == s.String() {
		// Inform the delegate
		conf := vn.ring.config
		old := vn.successors[0]
		vn.ring.invokeDelegate(func() {
			conf.Delegate.SuccessorLeaving(&vn.Vnode, old)
		})

		known := vn.knownSuccessors()
		copy(vn.successors[0:], vn.successors[1:])
		vn.successors[known-1] = nil
	}
	return nil
}

// Determine how many successors we know of
func (vn *localVnode) knownSuccessors() (successors int) {
	for i := 0; i < len(vn.successors); i++ {
		if vn.successors[i] != nil {
			successors = i + 1
		}
	}
	return
}
