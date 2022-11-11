package surfstore

import (
	context "context"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	log.Println("Get File Info Map Called")
	// PrintMetaMap(m.FileMetaMap)
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	log.Println("Update File called")
	log.Printf("Updating file: %v", fileMetaData.Filename)
	current_meta, ok := m.FileMetaMap[fileMetaData.Filename]
	if !ok || current_meta.Version+1 == fileMetaData.Version {
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		return &Version{Version: fileMetaData.Version}, nil
	} else {
		return &Version{Version: -1}, nil
	}
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	log.Println("Get block store addr called")
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
