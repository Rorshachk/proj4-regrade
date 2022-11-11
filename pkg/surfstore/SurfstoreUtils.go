package surfstore

import (
	"bufio"
	"errors"
	"io"
	"io/ioutil"
	"log"
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
		hash_list := ComputeHashList(filepath.Join(client.BaseDir, file.Name()), client.BlockSize)
		local_files[file.Name()] = hash_list
	}

	// Read metadata from index.txt
	local_metadata, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		panic(err)
	}

	// Check if there is any new added/changed/deleted file
	// Otherwise, the filemeta.version will increment by 1
	log.Println("Consulting local index file")
	updated := make(map[string]*FileMetaData)
	unchanged := make(map[string]*FileMetaData)
	deleted_mark := []string{"deleted"}
	for _, v := range local_metadata {
		local_hashlist, ok := local_files[v.Filename]
		if !ok {
			log.Printf("File %v is deleted since last time", v.Filename)
			updated[v.Filename] = &FileMetaData{Filename: v.Filename, Version: v.Version + 1, BlockHashList: deleted_mark}
		} else if !reflect.DeepEqual(*local_hashlist, v.BlockHashList) {
			log.Printf("File %v is changed since last time", v.Filename)
			updated[v.Filename] = &FileMetaData{Filename: v.Filename, Version: v.Version + 1, BlockHashList: *local_hashlist}
		} else {
			log.Printf("File %v doesn't change since last time", v.Filename)
			unchanged[v.Filename] = v
		}
	}
	for k, v := range local_files {
		if _, ok := local_metadata[k]; !ok {
			log.Printf("File %v is added since last time", k)
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
		use_local := false
		if ok {
			if update_file.Version == metadata.Version+1 {
				// Upload all blocks
				log.Printf("Changing remote file %v, uploading...", update_file.Filename)
				err = UploadFileBlocks(client, update_file, blockStoreAddr)
				if err != nil {
					panic(err)
				}

				// try to update fileinto
				var latestVersion int32
				err = client.UpdateFile(update_file, &latestVersion)
				if err != nil {
					panic(err)
				}
				if latestVersion == -1 {
					log.Printf("Updating for %v is rejected, add to download list", update_file.Filename)
					// Then it is rejected
					// We add it to update list
					willupdate_metadata = append(willupdate_metadata, FileMetaData{Filename: metadata.Filename,
						Version:       metadata.Version,
						BlockHashList: metadata.BlockHashList})
				} else {
					log.Printf("Successfully sync local changes to cloud")
					use_local = true
				}
			} else {
				log.Printf("Local file %v changed but stale, overwrite it", update_file.Filename)
				willupdate_metadata = append(willupdate_metadata, FileMetaData{Filename: metadata.Filename,
					Version:       metadata.Version,
					BlockHashList: metadata.BlockHashList})
			}
		} else {
			unchanged_file, ok := unchanged[filename]
			if !ok || unchanged_file.Version < metadata.Version {
				log.Printf("Remote file %v not found or stale, overwrite it", filename)
				willupdate_metadata = append(willupdate_metadata, FileMetaData{Filename: metadata.Filename,
					Version:       metadata.Version,
					BlockHashList: metadata.BlockHashList})
			} else {
				log.Printf("Remote file %v is identical to local, use_local: %v", filename, use_local)
			}
		}
		if !use_local {
			final_filemeta[filename] = metadata
		} else {
			final_filemeta[filename] = update_file
		}
	}

	// Check if there are any new files that can be uploaded
	for filename, metadata := range updated {
		_, ok := remote_file_map[filename]
		if !ok {
			log.Printf("local file %v is newly created, uploading...", filename)
			final_filemeta[filename] = metadata
			err = UploadFileBlocks(client, metadata, blockStoreAddr)
			if err != nil {
				panic(err)
			}

			var latestVersion int32
			err = client.UpdateFile(metadata, &latestVersion)
			if latestVersion == -1 {
				log.Printf("Updating remote index for file %v is rejected, overwrite it", filename)
				willupdate_metadata = append(willupdate_metadata, FileMetaData{Filename: metadata.Filename,
					Version:       metadata.Version,
					BlockHashList: metadata.BlockHashList})
			}
		}
	}

	UpdateLocalFiles(client, willupdate_metadata, blockStoreAddr)
	// Write index back
	err = WriteMetaFile(final_filemeta, client.BaseDir)
	if err != nil {
		panic(err)
	}
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

func UpdateLocalFiles(client RPCClient, update_files []FileMetaData, blockStoreAddr string) {
	log.Printf("Start updating all local files")
	for i := 0; i < len(update_files); i++ {
		err := RemoveIfExist(filepath.Join(client.BaseDir, update_files[i].Filename))
		if err != nil {
			panic(err)
		}

		if len(update_files[i].BlockHashList) == 1 && update_files[i].BlockHashList[0] == "deleted" {
			continue
		}
		// remove the file if exists

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
			log.Printf("Writing block data: %v", string(blk.BlockData[:blk.BlockSize]))
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
