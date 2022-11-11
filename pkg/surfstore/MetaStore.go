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
	// defer func() {
	// 	for k := range m.FileMetaMap {
	// 		m.FileMetaMap[k].Version += 1
	// 		log.Printf("Increment the file %v version to %v", m.FileMetaMap[k].Filename, m.FileMetaMap[k].Version)
	// 	}
	// }()
	// PrintMetaMap(m.FileMetaMap)
	return &FileInfoMap{FileInfoMap: CloneFileMetaMap(m.FileMetaMap)}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	log.Println("Update File called")
	log.Printf("Updating file: %v", fileMetaData.Filename)
	current_meta, ok := m.FileMetaMap[fileMetaData.Filename]
	if !ok || current_meta.Version+1 == fileMetaData.Version {
		m.FileMetaMap[fileMetaData.Filename] = &FileMetaData{Filename: fileMetaData.Filename,
			Version:       fileMetaData.Version,
			BlockHashList: fileMetaData.BlockHashList}
		return &Version{Version: fileMetaData.Version}, nil
	} else {
		// when current file is at least up-to-date
		*fileMetaData = FileMetaData{Filename: current_meta.Filename,
			Version:       current_meta.Version,
			BlockHashList: current_meta.BlockHashList}
		log.Printf("Updating rejected!")
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
