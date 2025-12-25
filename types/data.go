package types

import (
	"encoding/json"

	"github.com/juju/errors"
	"github.com/spf13/cast"
)

type Data map[string]any

func (d *Data) Get(key string) (any, bool) {
	v, exists := (*d)[key]
	return v, exists
}

func (d *Data) GetString(key string) (string, bool) {
	v, exists := d.Get(key)
	return cast.ToString(v), exists
}

func (d *Data) GetInt(key string) (int, bool) {
	v, exists := d.Get(key)
	return cast.ToInt(v), exists
}

func (d *Data) GetInt16(key string) (int16, bool) {
	v, exists := d.Get(key)
	return cast.ToInt16(v), exists
}

func (d *Data) GetInt32(key string) (int32, bool) {
	v, exists := d.Get(key)
	return cast.ToInt32(v), exists
}

func (d *Data) GetBool(key string) (bool, bool) {
	v, exists := d.Get(key)
	return cast.ToBool(v), exists
}

func (d *Data) GetFloat32(key string) (float32, bool) {
	v, exists := d.Get(key)
	return cast.ToFloat32(v), exists
}

func (d *Data) GetFloat64(key string) (float64, bool) {
	v, exists := d.Get(key)
	return cast.ToFloat64(v), exists
}

func (d *Data) GetStruct(key string, s any) error {
	v, exists := d.Get(key)
	if !exists {
		return errors.NotFound
	}
	b, err := json.Marshal(v)
	if err != nil {
		return errors.Wrap(err, errors.New("marshal failed"))
	}
	return json.Unmarshal(b, s)
}

func (d *Data) Set(key string, value any) {
	(*d)[key] = value
}
