package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) InsertServer(addr string) {
	c.ServerMap[c.Hash(addr)] = addr
}

func (c ConsistentHashRing) DeleteServer(addr string) {
	delete(c.ServerMap, c.Hash(addr))
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// Find the next largest key from ServerMap
	lowestkey := ""
	lowestval := ""

	retkey := ""
	retval := ""

	for k, v := range c.ServerMap {
		if lowestkey == "" {
			lowestkey = k
			lowestval = v
		} else {
			if compareHexString(lowestkey, k) {
				lowestkey = k
				lowestval = v
			}
		}

	}

	for k, v := range c.ServerMap {
		if compareHexString(k, blockId) {
			if retkey == "" {
				retkey = k
				retval = v
			} else {
				if compareHexString(retkey, k) {
					retkey = k
					retval = v
				}
			}
		}
	}
	if retval != "" {
		return retval
	} else {
		return lowestval
	}
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func (c ConsistentHashRing) OutputMap(blockHashes *[]string) map[string]string {
	res := make(map[string]string)
	for i := 0; i < len(*blockHashes); i++ {
		res[((*blockHashes)[i])] = c.GetResponsibleServer((*blockHashes)[i])
	}
	return res
}

func NewConsistentHashRing(numServers int, downServer []int) *ConsistentHashRing {
	c := &ConsistentHashRing{
		ServerMap: make(map[string]string),
	}

	for i := 0; i < numServers; i++ {
		c.InsertServer("blockstore" + strconv.Itoa(i))
	}

	for i := 0; i < len(downServer); i++ {
		c.DeleteServer("blockstore" + strconv.Itoa(downServer[i]))
	}

	return c
}

func compareHexString(hex1 string, hex2 string) bool {
	for i := 0; i < len(hex1); i++ {
		if hex1[i] != hex2[i] {
			return hex1[i] > hex2[i]
		}
	}
	return false
}
