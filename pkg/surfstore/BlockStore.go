package surfstore

import (
	context "context"
	"errors"
	"log"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// log.Printf("Get block called, block hash: %v", blockHash)
	log.Printf("Get block %v", blockHash.GetHash())
	blk, ok := bs.BlockMap[blockHash.GetHash()]
	if ok {
		// log.Printf("Block found: %v", string(blk.BlockData[:blk.BlockSize]))
		return blk, nil
	} else {
		log.Println("Block not found")
		return &Block{}, errors.New("Block not found")
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	log.Printf("Put block called, block len: %v, hash: %v", block.BlockSize, GetBlockHashString(block.BlockData[:block.BlockSize]))
	bs.BlockMap[GetBlockHashString(block.BlockData[:block.BlockSize])] = &Block{BlockData: block.BlockData, BlockSize: block.BlockSize}
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	log.Println("Has blocks called")
	var blockHashesString []string
	hashes := blockHashesIn.GetHashes()
	for i := 0; i < len(hashes); i++ {
		_, ok := bs.BlockMap[hashes[i]]
		if ok {
			blockHashesString = append(blockHashesString, hashes[i])
		}
	}
	return &BlockHashes{Hashes: hashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
