package surfstore

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"strconv"
)

/*
Implement the logic for a client syncing with the server here.
*/
func ClientSync(client RPCClient) {

	// We first check whether index.txt exist, if not, create it
	_, err := os.Stat(client.BaseDir + "/index.txt")
	if err != nil || os.IsNotExist(err) {
		createIndex(client)
	} 

	// Parse the index file into file-Data mapping
	index_fileData := parse_index(client)
	index_fileData_reference := parse_index(client)

	// We loop through the local files, and store their names with their hash content
	items, _ := ioutil.ReadDir(client.BaseDir)
	// We store the information in file_hash
	file_hash := make(map[string][]string)
	file_block := make(map[string][][]byte)
	for _, file := range items {
		if file.Name() == "index.txt" {
			continue
		}
		var hashList []string
		blockList := readBlock(client.BaseDir+"/"+file.Name(), client.BlockSize)
		for _, data := range blockList {
			hashList = append(hashList, computeHash(data))
		}
		file_hash[file.Name()] = hashList
		file_block[file.Name()] = blockList
	}

	// We use rpc call again to get the remote mapping for file and data
	succ2 := true
	serverFileInfoMap2 := make(map[string]FileMetaData)
	client.GetFileInfoMap(&succ2, &serverFileInfoMap2)
    PrintMetaMap (serverFileInfoMap2)

	// Trying to looping over the remote index, to check whether need download
	for name, data := range serverFileInfoMap2 {
		_, ok := file_hash[name]
		_, ok2 := index_fileData_reference[name]
		// If there is new file in remote, we download to local
		if ok == false && ok2 == false {
			download(client, name, data)
			index_fileData[name] = data
		} else if ok == true && ok2 == true {
			// If the remote version is new, we download the new version from remote
			if data.Version > index_fileData_reference[name].Version {
				download(client, name, data)
				index_fileData[name] = data
			}
		} else if ok == false && ok2 == true {
            if len(data.BlockHashList) != 1 || data.BlockHashList[0] != "0" {
                download(client, name, data)
				index_fileData[name] = data
			}
		}
	}

	// Trying to looping over the current files locally, to check whether need update
	for file_name, hashList := range file_hash {
		_, ok := serverFileInfoMap2[file_name]
		if ok == false {
			// If we have new file locally not found in remote, update it
			fmt.Printf ("New File update\n")
			version := 1
			err := update(client, file_name, hashList, file_block[file_name], &version)
			if err == nil {
				tb := index_fileData[file_name]
				tb.Filename = file_name
				tb.BlockHashList = hashList
				tb.Version = 1
				index_fileData[file_name] = tb
			}

		} else {
	
			if len(serverFileInfoMap2[file_name].BlockHashList) == 1 && 
			        serverFileInfoMap2[file_name].BlockHashList[0] == "0" {
				version := index_fileData_reference[file_name].Version+1
				err := update(client, file_name, hashList, file_block[file_name], &version)
				// If we successfully update files (no error), we record the new index file.
				if err == nil {
					tb := index_fileData[file_name]
					tb.BlockHashList = hashList
					tb.Version = version
					index_fileData[file_name] = tb
				}

			 // If we make change to local, and remote unchanged, then we update local to remote
			} else if serverFileInfoMap2[file_name].Version == index_fileData_reference[file_name].Version &&
			     compare_array (index_fileData_reference[file_name].BlockHashList, hashList) == false {
                
				fmt.Printf ("Add One\n")
				// We also change the index file
				version := index_fileData_reference[file_name].Version+1
				err := update(client, file_name, hashList, file_block[file_name], &version)
				// If we successfully update files (no error), we record the new index file.
				if err == nil {
					tb := index_fileData[file_name]
					tb.BlockHashList = hashList
					tb.Version = version
					index_fileData[file_name] = tb
				}

			// If we make change to local, but the remote changed by others, we download the new change from remote
			} else if serverFileInfoMap2[file_name].Version > index_fileData_reference[file_name].Version &&
			    compare_array (index_fileData_reference[file_name].BlockHashList, hashList) == false {

				// We download the remote files to local, and change index file
				download(client, file_name, serverFileInfoMap2[file_name])
				index_fileData[file_name] = serverFileInfoMap2[file_name]
			}
		}
	}

	// We loop through the index file, if any file not found in local, delete it from remote
	for file_name, fileData := range index_fileData_reference {
		fmt.Printf (file_name + "\n")
		_, ok := file_hash[file_name]
		if ok == false && (len(fileData.BlockHashList) != 1 || fileData.BlockHashList[0] != "0" ){
			version := fileData.Version+1
			err := delete(client, file_name, fileData, &version)
			if err == nil {
                var delete_hash []string
	            delete_hash = append(delete_hash, "0")
			    tb := index_fileData[file_name]
			    tb.BlockHashList = delete_hash
			    tb.Version = version
			    index_fileData[file_name] = tb
			}
		}
	}
	// Finally, write the updating index_file-data mapping into index files.
	write_index(client, index_fileData)
}

func delete(client RPCClient, name string, data FileMetaData, version *int) error {
    fmt.Printf ("Entering delete\n")
	var delete_hash []string
	delete_hash = append(delete_hash, "0")
	fileMetaData := &FileMetaData{
		Filename:      name,
		Version:       *version,
		BlockHashList: delete_hash,
	}

	// We update the information of remote file as hash_list = ["0"]
	err := client.UpdateFile(fileMetaData, version)
	return err
}

// Write the file mapping back to the index file
func write_index(client RPCClient, index_file map[string]FileMetaData) {

	f, err := os.Create(client.BaseDir+"/index.txt")//, os.O_TRUNC, 0755
	if err != nil {
		panic(err)
	}
	defer f.Close()

	writer := bufio.NewWriter(f)

	message := ""
	for key, val := range index_file {
		message += key + "," + strconv.Itoa(val.Version) + ","
		if len(val.BlockHashList) == 0 {
			message += "\n"
			continue
		}
		message = message + val.BlockHashList[0]
		if len(val.BlockHashList) == 1 {
			message += "\n"
			continue
		}
		for _, hash_element := range val.BlockHashList[1:] {
			message = message + " " + hash_element
		}
		message += "\n"
	}
	fmt.Printf(message)
	_, err = writer.WriteString(message)
	if err != nil {
		return
	}
	err = writer.Flush()
}

func parse_index(client RPCClient) map[string]FileMetaData {
	content, err := ioutil.ReadFile(client.BaseDir + "/index.txt")
	if err != nil {
		panic(err)
	}
	// Convert []byte to string and print to screen
	bodyString := string(content)
	index_file := make(map[string]FileMetaData)
	entries := strings.Split(bodyString, "\n")
	for _, e := range entries {
        
		//fmt.Printf(strconv.Itoa(len(e)))
		if len(e) == 0 {
			break;
		}
		parts := strings.Split(e, ",")
		block_list := strings.Split(parts[2], " ")
		version, err := strconv.Atoi(parts[1])
		if err != nil {
			panic (err)
		}
		fileData := FileMetaData{
			Filename:      parts[0],
			Version:       version,
			BlockHashList: block_list,
		}
		index_file[parts[0]] = fileData
		if len(e) == 0 {
			break
		}
	}
	return index_file
}

// We update the new local files to remote (both meta and block)
func update(client RPCClient, name string, hashList []string, blockList [][]byte, version *int) error {
    fmt.Printf ("Entering update\n")
	blockHashesIn := []string{}
	blockStoreMap := make(map[string][]string)
	client.GetBlockStoreMap(blockHashesIn, &blockStoreMap)
	block_address := ""
	for key, _ := range blockStoreMap {
		block_address = key
	}

	fmt.Println(block_address+"\n")

    outputBlock := []string{}
	client.HasBlocks(hashList, block_address, &outputBlock)

	for num, blockData := range blockList {
		succ := true
		block := Block{
			BlockData: blockData,
			BlockSize: len(blockData),
		}
		if contains (hashList[num], outputBlock) == false {
			// We store the block data into BlockStore
			//fmt.Println("push one block\n")
			client.PutBlock(block, block_address, &succ)
		}
	}
	fileMetaData := &FileMetaData{
		Filename:      name,
		Version:       *version,
		BlockHashList: hashList,
	}
	//fmt.Printf(strconv.Itoa(*version) +"\n")
	// We update the information of mapping file to data into MetaStore
	err := client.UpdateFile(fileMetaData, version)

	return err
}


// We download the files from remote to local.
func download(client RPCClient, name string, data FileMetaData) {
	fmt.Printf ("Entering download\n")
	// We create a file with name locally, and try to write string in it.
	f, e := os.Create(client.BaseDir + "/" + name)
	if e != nil {
		panic(e)
	}
	defer f.Close()
	writer := bufio.NewWriter(f)

	blockHashesIn := []string{}
	blockStoreMap := make(map[string][]string)
	client.GetBlockStoreMap(blockHashesIn, &blockStoreMap)
	block_address := ""
	for key, _ := range blockStoreMap {
		block_address = key
	}
	// Loop through the hash list for each file, and convert each hash to string
	for _, hash := range data.BlockHashList {

		var block *Block
		block = new(Block)
		// Get the block as byte[] from blockStore
		client.GetBlock(hash, block_address, block)

		// Convert byte[] to string, and write into file
		content := string(block.BlockData)
		_, err := writer.WriteString(content)
		if err != nil {
			return
		}
		err = writer.Flush()
	}
}

// If there is no index file, we create it and initialize based on remote mapping
func createIndex(client RPCClient) {
	f, e := os.Create(client.BaseDir + "/index.txt")
	if e != nil {
		panic(e)
	}
	defer f.Close()
}

// We convert the files into several blocks, each with byte array data
func readBlock(filePath string, blockSize int) (blockList [][]byte) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	for {
		buf := make([]byte, blockSize)
		n, err := io.ReadFull(reader, buf)
		if err == nil || err == io.ErrUnexpectedEOF {
			blockList = append(blockList, buf[:n])
		} else if err == io.EOF {
			break
		} else {
			panic(err)
		}
	}
	return blockList
}

// We compute the hash value for each bytes array data
func computeHash(blockBytes []byte) (hashString string) {
	// h := sha256.New()
	// h.Write(blockBytes)
	// hashBytes := h.Sum(nil)
	hashBytes := sha256.Sum256(blockBytes)
	hashString = hex.EncodeToString(hashBytes[:])
	return hashString
}


func compare_array (a []string, b []string) bool {
    if len(a) != len(b) {
        return false
    }

    for i, v := range a {
        if v != b[i] {
            return false
        }
    }
    return true
}

// Function to check whether a string array contains a string.
func contains (str string, arr[]string) bool {
	for _, a := range arr {
		if a == str {
		   return true
		}
	}
	return false
}

/*
Helper function to print the contents of the metadata map.
*/
func PrintMetaMap(metaMap map[string]FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version, filemeta.BlockHashList)
	}

	fmt.Println("---------END PRINT MAP--------")

}
