package surfstore

import (
	"errors"
)

type MetaStore struct {
	FileMetaMap map[string]FileMetaData
	// Add additional data structure(s) to maintain BlockStore addresses
	BlockAddress string
}

func (m *MetaStore) GetFileInfoMap(_ignore *bool, serverFileInfoMap *map[string]FileMetaData) error {

	// We loop through the fileMetaMap, and record the filename with the fileMetaData
	for key, value := range m.FileMetaMap {
		
		(*serverFileInfoMap)[key] = value
	}

	return nil
}

func (m *MetaStore) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) (err error) {

	// We try to find whether the update file exist in remote
	_, ok := m.FileMetaMap[fileMetaData.Filename]

	if ok == true {
		// If it exists in remote, and previously deleted (hashlist=[0]), retrieve it
		if len(m.FileMetaMap[fileMetaData.Filename].BlockHashList) == 1 &&
			m.FileMetaMap[fileMetaData.Filename].BlockHashList[0] == "0" {

			e_b := m.FileMetaMap[fileMetaData.Filename]
			e_b.BlockHashList = fileMetaData.BlockHashList
			e_b.Version += 1
			m.FileMetaMap[fileMetaData.Filename] = e_b

		// If we update an old file, check version = old version+1, then update content/version
		} else if fileMetaData.Version == m.FileMetaMap[fileMetaData.Filename].Version+1 {

			//m.FileMetaMap[fileMetaData.Filename] = *fileMetaData
			e_v := m.FileMetaMap[fileMetaData.Filename]
			e_v.BlockHashList = fileMetaData.BlockHashList
			e_v.Version = fileMetaData.Version
			m.FileMetaMap[fileMetaData.Filename] = e_v

			// If the version != old version + 1, sending back error version too old.
		} else {
			return errors.New("Version too old")
		}

		// If the file doesn't exists, meaning we need to create a new file in remote
	} else {
        
		// If the version is 1, we just record its file:content information.
		//if fileMetaData.Version == 1 {
			//m.FileMetaMap[fileMetaData.Filename] = *fileMetaData
			e_f := m.FileMetaMap[fileMetaData.Filename]
			e_f.Filename = fileMetaData.Filename
			e_f.BlockHashList = fileMetaData.BlockHashList
			e_f.Version = fileMetaData.Version
			m.FileMetaMap[fileMetaData.Filename] = e_f

			// If the version is wrong we return errors.
		//} else {
		//	return errors.New("File not found")
		//}
	}
	*latestVersion = m.FileMetaMap[fileMetaData.Filename].Version
	return nil
}

func (m *MetaStore) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {

	// Since there is only one blockAddress, simply record that.
	(*blockStoreMap)[m.BlockAddress] = blockHashesIn
	//fmt.Println("ggggg" + m.BlockAddress+"\n")
	return nil
}

var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreList []string) MetaStore {

	return MetaStore{
		// Initialize an empty map (no file)
		FileMetaMap: map[string]FileMetaData{},
		//Just use the first element as the block address.
		BlockAddress: blockStoreList[0],
	}

}
