package chord

import (
	"correct-chord-go/global"
	"errors"
	"hash"
)

type dataStore struct {
	/*
		All the data in the Distributed Hash Table will be stored in objects of this type
		data (map[string]string): A hashmap to store key-value pairs
		Hash (func() hash.Hash): A hash function to hash keys
	*/
	data map[string]string
	Hash func() hash.Hash
}

func NewDataStore(hashFunc func() hash.Hash) Storage {
	/*
		Creates a dataStore object for a node
		Input:
			hashFunc (func() hash.Hash): hash function to be used for hashing keys
		Output:
			dataStore: with the provided hash function
	*/
	return &dataStore{
		data: make(map[string]string),
		Hash: hashFunc,
	}
}

func (d *dataStore) Get(key string) ([]byte, error) {
	/*
		This function returns the key-value pair with key provided as input
		Input:
			key (string): The key that needs to be found
		Output:
			val ([]byte): The value corresponding to the key
	*/
	val, ok := d.data[key]
	if !ok {
		return nil, errors.New(global.ERROR_KEY_NOT_FOUND)
	}
	return []byte(val), nil
}

func (d *dataStore) Set(key string, value string) error {
	/*
		This function sets a key-value pair provided as input in a node
		Input:
			key (string): The key that needs to be set
			value (string): They value corresponding to the key provided
	*/

	d.data[key] = value
	return nil
}

func (d *dataStore) Delete(key string) error {
	/*
		This function deletes a key-value pair with key provided
		Input:
			key (string): The key that needs to be deleted
	*/

	delete(d.data, key)
	return nil
}
