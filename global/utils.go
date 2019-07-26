package global

import (
	"bytes"
	"fmt"
	"math/big"
)

// Checks if a key is STRICTLY between two ID's exclusively
func Between(id1, id2, key []byte) bool {
	// Check for ring wrap around
	if bytes.Compare(id1, id2) == 1 {
		return bytes.Compare(id1, key) == -1 ||
			bytes.Compare(id2, key) == 1
	}

	// Handle the normal case
	return bytes.Compare(id1, key) == -1 &&
		bytes.Compare(id2, key) == 1
}

// Checks if a key is between two ID's, right inclusive
func BetweenRightIncl(id1, id2, key []byte) bool {
	// Check for ring wrap around
	if bytes.Compare(id1, id2) == 1 {
		return bytes.Compare(id1, key) == -1 ||
			bytes.Compare(id2, key) >= 0
	}

	return bytes.Compare(id1, key) == -1 &&
		bytes.Compare(id2, key) >= 0
}

// Computes the offset by (n + 2^exp) % (2^mod)
func PowerOffset(id []byte, exp int, mod int) []byte {
	// Copy the existing slice
	off := make([]byte, len(id))
	copy(off, id)

	// Convert the ID to a bigint
	idInt := big.Int{}
	idInt.SetBytes(id)

	// Get the offset
	two := big.NewInt(2)
	offset := big.Int{}
	offset.Exp(two, big.NewInt(int64(exp)), nil)

	// Sum
	sum := big.Int{}
	sum.Add(&idInt, &offset)

	// Get the ceiling
	ceil := big.Int{}
	ceil.Exp(two, big.NewInt(int64(mod)), nil)

	// Apply the mod
	idInt.Mod(&sum, &ceil)

	// Add together
	return idInt.Bytes()
}

// max returns the max of two ints
func Max(a, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

// min returns the min of two ints
func Min(a, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

// Merges errors together
func MergeErrors(err1, err2 error) error {
	if err1 == nil {
		return err2
	} else if err2 == nil {
		return err1
	} else {
		return fmt.Errorf("%s\n%s", err1, err2)
	}
}
