package utils

import "encoding/json"

func Serialize(o any) ([]byte, error) {
	return json.Marshal(o)
}

func Unserialize(b []byte, o any) error {
	return json.Unmarshal(b, o)
}
