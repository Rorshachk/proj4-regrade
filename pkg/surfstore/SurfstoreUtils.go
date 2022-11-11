package surfstore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// Scan base directory
	local_files := make(map[string]*[]string)
	entries, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		panic(err)
	}

	for _, file := range entries {
		if file.Name() == "index.txt" {
			continue
		}
		hash_list := ComputeHashList(client.BaseDir, file.Name(), client.BlockSize)
		local_files[file.Name()] = hash_list
	}

	// Read metadata from index.txt
	local_metadata, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		panic(err)
	}

	// Check if there is any new added/changed/deleted file
	// If filemeta.version = 0, then it is deleted
	// If filemeta.version = 1, then it is newly added
	// Otherwise, the filemeta.version will increment by 1
	updated := make(map[string]*FileMetaData)
	unchanged := make(map[string]*FileMetaData)
	deleted_mark := []string{"deleted"}
	for _, v := range local_metadata {
		local_hashlist, ok := local_files[v.Filename]
		if !ok {
			updated[v.Filename] = &FileMetaData{Version: v.Version + 1, BlockHashList: deleted_mark}
		} else if !reflect.DeepEqual(*local_hashlist, v.BlockHashList) {
			updated[v.Filename] = &FileMetaData{Filename: v.Filename, Version: v.Version + 1, BlockHashList: *local_hashlist}
		} else {
			unchanged[v.Filename] = v
		}
	}
	for k, v := range local_files {
		if _, ok := local_metadata[k]; !ok {
			updated[k] = &FileMetaData{Filename: k, Version: 1, BlockHashList: *v}
		}
	}

	// Get remote filemap
	remote_file_map := make(map[string]*FileMetaData)
	final_filemeta := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remote_file_map)
	if err != nil {
		panic(err)
	}

	PrintMetaMap(remote_file_map)

	// Get block store addr
	var blockStoreAddr string
	err = client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		panic(err)
	}

	// Iterate over all remote files
	// to check if they can be updated
	var willupdate_metadata []FileMetaData
	for filename, metadata := range remote_file_map {
		update_file, ok := updated[filename]
		final_filemeta[filename] = metadata
		if ok {
			if update_file.Version == metadata.Version+1 {
				// Upload all blocks
				err = UploadFileBlocks(client, update_file, blockStoreAddr)
				if err != nil {
					panic(err)
				}

				// try to update fileinto
				var latestVersion int32
				err = client.UpdateFile(metadata, &latestVersion)
				if err != nil {
					panic(err)
				}
				if latestVersion == -1 {
					// Then it is rejected
					// We add it to update list
					willupdate_metadata = append(willupdate_metadata, FileMetaData{Filename: metadata.Filename,
						Version:       metadata.Version,
						BlockHashList: metadata.BlockHashList})
				}
			} else {
				willupdate_metadata = append(willupdate_metadata, FileMetaData{Filename: metadata.Filename,
					Version:       metadata.Version,
					BlockHashList: metadata.BlockHashList})
			}
		} else {
			unchanged_file, ok := unchanged[filename]
			if !ok || unchanged_file.Version < metadata.Version {
				willupdate_metadata = append(willupdate_metadata, FileMetaData{Filename: metadata.Filename,
					Version:       metadata.Version,
					BlockHashList: metadata.BlockHashList})
			}
		}
	}

	// Check if there are any new files that can be uploaded
	for filename, metadata := range updated {
		_, ok := remote_file_map[filename]
		if !ok {
			final_filemeta[filename] = metadata
			err = UploadFileBlocks(client, metadata, blockStoreAddr)
			if err != nil {
				panic(err)
			}

			var latestVersion int32
			err = client.UpdateFile(metadata, &latestVersion)
			if latestVersion == -1 {
				willupdate_metadata = append(willupdate_metadata, FileMetaData{Filename: metadata.Filename,
					Version:       metadata.Version,
					BlockHashList: metadata.BlockHashList})
			}
		}
	}

	UpdateFiles(client, willupdate_metadata, blockStoreAddr)
	// Write index back
	err = WriteMetaFile(final_filemeta, client.BaseDir)
	if err != nil {
		panic(err)
	}
}

func ComputeHashList(baseDir string, filename string, blockSize int) *[]string {
	file, err := os.OpenFile(filepath.Join(baseDir, filename), os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}

	reader := bufio.NewReader(file)
	buf := make([]byte, blockSize)
	var hash_list []string
	for {
		n, err := io.ReadFull(reader, buf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			hash_list = append(hash_list, GetBlockHashString(buf[:n]))
			break
		} else if err == nil {
			hash_list = append(hash_list, GetBlockHashString(buf))
		} else {
			fmt.Print(n)
			panic(err)
		}
	}

	return &hash_list
}

func UploadFileBlocks(client RPCClient, file *FileMetaData, blockStoreAddr string) error {
	// skip if file is marked deleted
	if len(file.BlockHashList) == 1 {
		if file.BlockHashList[0] == "deleted" {
			return nil
		}
	}
	var blockHashesOut []string
	err := client.HasBlocks(file.BlockHashList, blockStoreAddr, &blockHashesOut)
	if err != nil {
		panic(err)
	}

	block_set := make(map[string]bool)
	for _, blk_hash := range blockHashesOut {
		block_set[blk_hash] = true
	}

	// Upload any block that is not exist
	f, err := os.OpenFile(filepath.Join(client.BaseDir, file.Filename), os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(f)
	buf := make([]byte, client.BlockSize)
	var blk Block
	for {
		n, err := io.ReadFull(reader, buf)
		var flg bool
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			panic(err)
		}
		blk.BlockData = buf
		blk.BlockSize = int32(n)
		put_err := client.PutBlock(&blk, blockStoreAddr, &flg)
		if !flg || put_err != nil {
			return errors.New("Unable to upload blocks")
		}
		if err != nil {
			break
		}
	}

	return nil
}

func UpdateFiles(client RPCClient, update_files []FileMetaData, blockStoreAddr string) {
	for i := 0; i < len(update_files); i++ {
		if len(update_files[i].BlockHashList) == 1 && update_files[i].BlockHashList[i] == "deleted" {
			_, err := os.Stat(update_files[i].Filename)
			if err == nil {
				_ = os.Remove(update_files[i].Filename)
				continue
			}
		}
		f, err := os.OpenFile(filepath.Join(client.BaseDir, update_files[i].Filename), os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		writer := bufio.NewWriter(f)

		for _, hash := range update_files[i].BlockHashList {
			blk := &Block{}
			err = client.GetBlock(hash, blockStoreAddr, blk)
			if err != nil {
				panic(err)
			}
			fmt.Printf("Writing block data: %v", string(blk.BlockData[:blk.BlockSize]))
			_, err := writer.Write(blk.BlockData[:blk.BlockSize])
			if err != nil {
				panic(err)
			}
		}
		err = writer.Flush()
		if err != nil {
			panic(err)
		}
	}
}
