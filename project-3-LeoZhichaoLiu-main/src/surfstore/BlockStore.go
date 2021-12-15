package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
)

type BlockStore struct {
	BlockMap map[string]Block
}

func (bs *BlockStore) GetBlock(blockHash string, blockData *Block) error {
	//panic("todo")
	block := bs.BlockMap[blockHash]
	(*blockData) = block

	return nil
}

func (bs *BlockStore) PutBlock(block Block, succ *bool) error {
	//panic("todo")
	hashBytes := sha256.Sum256(block.BlockData)
	hashString := hex.EncodeToString(hashBytes[:])
	bs.BlockMap[hashString] = block

	*succ = true

	return nil
}

func (bs *BlockStore) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	//panic("todo")
	for _, item := range blockHashesIn {
		_, ok := bs.BlockMap[item]
		if ok == true {
			*blockHashesOut = append(*blockHashesOut, item)
		}
	}
	return nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() BlockStore {
	return BlockStore{BlockMap: map[string]Block{}}
}
