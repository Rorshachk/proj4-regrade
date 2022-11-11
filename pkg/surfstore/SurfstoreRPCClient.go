package surfstore

import (
	context "context"
	"log"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddr string
	BaseDir       string
	BlockSize     int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})

	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {

	log.Printf("Putting block, size %v, real len %v", block.BlockSize, len(block.BlockData))
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	success, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		panic(err)
	}
	log.Printf("Success: %v\n", success.GetFlag())
	*succ = success.GetFlag()
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	in_hashes := &BlockHashes{Hashes: blockHashesIn}
	out_hashes, err := c.HasBlocks(ctx, in_hashes)
	if err != nil {
		return err
	}
	*blockHashesOut = out_hashes.Hashes
	return nil
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	c := NewMetaStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	file_info_map, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
	// PrintMetaMap(file_info_map.FileInfoMap)
	if err != nil {
		return err
	}
	*serverFileInfoMap = file_info_map.FileInfoMap
	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	c := NewMetaStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	log.Printf("Version before update: %v", fileMetaData.Version)
	version, err := c.UpdateFile(ctx, fileMetaData)
	if err != nil {
		return err
	}
	log.Printf("Version after update: %v", fileMetaData.Version)
	*latestVersion = version.Version
	return nil
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	c := NewMetaStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	block_addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	*blockStoreAddr = block_addr.Addr
	return nil
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(hostPort, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddr: hostPort,
		BaseDir:       baseDir,
		BlockSize:     blockSize,
	}
}
