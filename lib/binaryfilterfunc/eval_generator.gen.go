// Code generated by tmpl; DO NOT EDIT.
// https://github.com/benbjohnson/tmpl
//
// Source: eval_generator.gen.go.tmpl

/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package binaryfilterfunc

import (
	"bytes"

	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/util"
)

func GetFloatLTConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetFloatLTConditionBitMapWithoutNull(params)
	}
	return GetFloatLTConditionBitMapWithNull(params)
}

func GetIntegerLTConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetIntegerLTConditionBitMapWithoutNull(params)
	}
	return GetIntegerLTConditionBitMapWithNull(params)
}

func GetStringLTConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetStringLTConditionBitMapWithoutNull(params)
	}
	return GetStringLTConditionBitMapWithNull(params)
}

func GetFloatLTConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	var values []float64
	if !params.int2float {
		values = col.FloatValues()
	} else {
		values = Int64ToFloat64Slice(col.IntegerValues())
	}
	cmpData, _ := compare.(float64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if values[i] >= cmpData {
			bitmap.SetBitMap(pos, idx)
		}
	}
	return pos
}

func GetIntegerLTConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	values := col.IntegerValues()
	cmpData, _ := compare.(int64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if values[i] >= cmpData {
			bitmap.SetBitMap(pos, idx)
		}
	}
	return pos
}

func GetStringLTConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	cmpData := util.Str2bytes(compare.(string))
	for i := 0; i < col.Len-1; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if bytes.Compare(col.Val[col.Offset[i]:col.Offset[i+1]], cmpData) >= 0 {
			bitmap.SetBitMap(pos, idx)
		}
	}
	idx = offset + col.Len - 1
	if bitmap.IsNil(pos, idx) {
		return pos
	}
	if bytes.Compare(col.Val[col.Offset[col.Len-1]:], cmpData) >= 0 {
		bitmap.SetBitMap(pos, idx)
	}
	return pos
}

func GetFloatLTConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	var values []float64
	if !params.int2float {
		values = col.FloatValues()
	} else {
		values = Int64ToFloat64Slice(col.IntegerValues())
	}
	var idx int
	var index int
	cmpData, _ := compare.(float64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			if !bitmap.IsNil(bitMap, idx) {
				index++
			}
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if values[index] >= cmpData {
			bitmap.SetBitMap(pos, idx)
		}
		index++
	}
	return pos
}

func GetIntegerLTConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	values := col.IntegerValues()
	var idx int
	var index int
	cmpData, _ := compare.(int64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			if !bitmap.IsNil(bitMap, idx) {
				index++
			}
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if values[index] >= cmpData {
			bitmap.SetBitMap(pos, idx)
		}
		index++
	}
	return pos
}

func GetStringLTConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	var idx int
	cmpData := util.Str2bytes(compare.(string))
	for i := 0; i < col.Len-1; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if bytes.Compare(col.Val[col.Offset[i]:col.Offset[i+1]], cmpData) >= 0 {
			bitmap.SetBitMap(pos, idx)
		}
	}
	idx = offset + col.Len - 1
	if bitmap.IsNil(pos, idx) {
		return pos
	}
	if bitmap.IsNil(bitMap, idx) || bytes.Compare(col.Val[col.Offset[col.Len-1]:], cmpData) >= 0 {
		bitmap.SetBitMap(pos, idx)
	}
	return pos
}

func GetFloatLTEConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetFloatLTEConditionBitMapWithoutNull(params)
	}
	return GetFloatLTEConditionBitMapWithNull(params)
}

func GetIntegerLTEConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetIntegerLTEConditionBitMapWithoutNull(params)
	}
	return GetIntegerLTEConditionBitMapWithNull(params)
}

func GetStringLTEConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetStringLTEConditionBitMapWithoutNull(params)
	}
	return GetStringLTEConditionBitMapWithNull(params)
}

func GetFloatLTEConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	var values []float64
	if !params.int2float {
		values = col.FloatValues()
	} else {
		values = Int64ToFloat64Slice(col.IntegerValues())
	}
	cmpData, _ := compare.(float64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if values[i] > cmpData {
			bitmap.SetBitMap(pos, idx)
		}
	}
	return pos
}

func GetIntegerLTEConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	values := col.IntegerValues()
	cmpData, _ := compare.(int64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if values[i] > cmpData {
			bitmap.SetBitMap(pos, idx)
		}
	}
	return pos
}

func GetStringLTEConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	cmpData := util.Str2bytes(compare.(string))
	for i := 0; i < col.Len-1; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if bytes.Compare(col.Val[col.Offset[i]:col.Offset[i+1]], cmpData) > 0 {
			bitmap.SetBitMap(pos, idx)
		}
	}
	idx = offset + col.Len - 1
	if bitmap.IsNil(pos, idx) {
		return pos
	}
	if bytes.Compare(col.Val[col.Offset[col.Len-1]:], cmpData) > 0 {
		bitmap.SetBitMap(pos, idx)
	}
	return pos
}

func GetFloatLTEConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	var values []float64
	if !params.int2float {
		values = col.FloatValues()
	} else {
		values = Int64ToFloat64Slice(col.IntegerValues())
	}
	var idx int
	var index int
	cmpData, _ := compare.(float64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			if !bitmap.IsNil(bitMap, idx) {
				index++
			}
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if values[index] > cmpData {
			bitmap.SetBitMap(pos, idx)
		}
		index++
	}
	return pos
}

func GetIntegerLTEConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	values := col.IntegerValues()
	var idx int
	var index int
	cmpData, _ := compare.(int64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			if !bitmap.IsNil(bitMap, idx) {
				index++
			}
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if values[index] > cmpData {
			bitmap.SetBitMap(pos, idx)
		}
		index++
	}
	return pos
}

func GetStringLTEConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	var idx int
	cmpData := util.Str2bytes(compare.(string))
	for i := 0; i < col.Len-1; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if bytes.Compare(col.Val[col.Offset[i]:col.Offset[i+1]], cmpData) > 0 {
			bitmap.SetBitMap(pos, idx)
		}
	}
	idx = offset + col.Len - 1
	if bitmap.IsNil(pos, idx) {
		return pos
	}
	if bitmap.IsNil(bitMap, idx) || bytes.Compare(col.Val[col.Offset[col.Len-1]:], cmpData) > 0 {
		bitmap.SetBitMap(pos, idx)
	}
	return pos
}

func GetFloatGTConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetFloatGTConditionBitMapWithoutNull(params)
	}
	return GetFloatGTConditionBitMapWithNull(params)
}

func GetIntegerGTConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetIntegerGTConditionBitMapWithoutNull(params)
	}
	return GetIntegerGTConditionBitMapWithNull(params)
}

func GetStringGTConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetStringGTConditionBitMapWithoutNull(params)
	}
	return GetStringGTConditionBitMapWithNull(params)
}

func GetFloatGTConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	var values []float64
	if !params.int2float {
		values = col.FloatValues()
	} else {
		values = Int64ToFloat64Slice(col.IntegerValues())
	}
	cmpData, _ := compare.(float64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if values[i] <= cmpData {
			bitmap.SetBitMap(pos, idx)
		}
	}
	return pos
}

func GetIntegerGTConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	values := col.IntegerValues()
	cmpData, _ := compare.(int64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if values[i] <= cmpData {
			bitmap.SetBitMap(pos, idx)
		}
	}
	return pos
}

func GetStringGTConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	cmpData := util.Str2bytes(compare.(string))
	for i := 0; i < col.Len-1; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if bytes.Compare(col.Val[col.Offset[i]:col.Offset[i+1]], cmpData) <= 0 {
			bitmap.SetBitMap(pos, idx)
		}
	}
	idx = offset + col.Len - 1
	if bitmap.IsNil(pos, idx) {
		return pos
	}
	if bytes.Compare(col.Val[col.Offset[col.Len-1]:], cmpData) <= 0 {
		bitmap.SetBitMap(pos, idx)
	}
	return pos
}

func GetFloatGTConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	var values []float64
	if !params.int2float {
		values = col.FloatValues()
	} else {
		values = Int64ToFloat64Slice(col.IntegerValues())
	}
	var idx int
	var index int
	cmpData, _ := compare.(float64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			if !bitmap.IsNil(bitMap, idx) {
				index++
			}
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if values[index] <= cmpData {
			bitmap.SetBitMap(pos, idx)
		}
		index++
	}
	return pos
}

func GetIntegerGTConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	values := col.IntegerValues()
	var idx int
	var index int
	cmpData, _ := compare.(int64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			if !bitmap.IsNil(bitMap, idx) {
				index++
			}
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if values[index] <= cmpData {
			bitmap.SetBitMap(pos, idx)
		}
		index++
	}
	return pos
}

func GetStringGTConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	var idx int
	cmpData := util.Str2bytes(compare.(string))
	for i := 0; i < col.Len-1; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if bytes.Compare(col.Val[col.Offset[i]:col.Offset[i+1]], cmpData) <= 0 {
			bitmap.SetBitMap(pos, idx)
		}
	}
	idx = offset + col.Len - 1
	if bitmap.IsNil(pos, idx) {
		return pos
	}
	if bitmap.IsNil(bitMap, idx) || bytes.Compare(col.Val[col.Offset[col.Len-1]:], cmpData) <= 0 {
		bitmap.SetBitMap(pos, idx)
	}
	return pos
}

func GetFloatGTEConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetFloatGTEConditionBitMapWithoutNull(params)
	}
	return GetFloatGTEConditionBitMapWithNull(params)
}

func GetIntegerGTEConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetIntegerGTEConditionBitMapWithoutNull(params)
	}
	return GetIntegerGTEConditionBitMapWithNull(params)
}

func GetStringGTEConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetStringGTEConditionBitMapWithoutNull(params)
	}
	return GetStringGTEConditionBitMapWithNull(params)
}

func GetFloatGTEConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	var values []float64
	if !params.int2float {
		values = col.FloatValues()
	} else {
		values = Int64ToFloat64Slice(col.IntegerValues())
	}
	cmpData, _ := compare.(float64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if values[i] < cmpData {
			bitmap.SetBitMap(pos, idx)
		}
	}
	return pos
}

func GetIntegerGTEConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	values := col.IntegerValues()
	cmpData, _ := compare.(int64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if values[i] < cmpData {
			bitmap.SetBitMap(pos, idx)
		}
	}
	return pos
}

func GetStringGTEConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	cmpData := util.Str2bytes(compare.(string))
	for i := 0; i < col.Len-1; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if bytes.Compare(col.Val[col.Offset[i]:col.Offset[i+1]], cmpData) < 0 {
			bitmap.SetBitMap(pos, idx)
		}
	}
	idx = offset + col.Len - 1
	if bitmap.IsNil(pos, idx) {
		return pos
	}
	if bytes.Compare(col.Val[col.Offset[col.Len-1]:], cmpData) < 0 {
		bitmap.SetBitMap(pos, idx)
	}
	return pos
}

func GetFloatGTEConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	var values []float64
	if !params.int2float {
		values = col.FloatValues()
	} else {
		values = Int64ToFloat64Slice(col.IntegerValues())
	}
	var idx int
	var index int
	cmpData, _ := compare.(float64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			if !bitmap.IsNil(bitMap, idx) {
				index++
			}
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if values[index] < cmpData {
			bitmap.SetBitMap(pos, idx)
		}
		index++
	}
	return pos
}

func GetIntegerGTEConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	values := col.IntegerValues()
	var idx int
	var index int
	cmpData, _ := compare.(int64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			if !bitmap.IsNil(bitMap, idx) {
				index++
			}
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if values[index] < cmpData {
			bitmap.SetBitMap(pos, idx)
		}
		index++
	}
	return pos
}

func GetStringGTEConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	var idx int
	cmpData := util.Str2bytes(compare.(string))
	for i := 0; i < col.Len-1; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if bytes.Compare(col.Val[col.Offset[i]:col.Offset[i+1]], cmpData) < 0 {
			bitmap.SetBitMap(pos, idx)
		}
	}
	idx = offset + col.Len - 1
	if bitmap.IsNil(pos, idx) {
		return pos
	}
	if bitmap.IsNil(bitMap, idx) || bytes.Compare(col.Val[col.Offset[col.Len-1]:], cmpData) < 0 {
		bitmap.SetBitMap(pos, idx)
	}
	return pos
}

func GetFloatEQConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetFloatEQConditionBitMapWithoutNull(params)
	}
	return GetFloatEQConditionBitMapWithNull(params)
}

func GetIntegerEQConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetIntegerEQConditionBitMapWithoutNull(params)
	}
	return GetIntegerEQConditionBitMapWithNull(params)
}

func GetStringEQConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetStringEQConditionBitMapWithoutNull(params)
	}
	return GetStringEQConditionBitMapWithNull(params)
}

func GetBooleanEQConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetBooleanEQConditionBitMapWithoutNull(params)
	}
	return GetBooleanEQConditionBitMapWithNull(params)
}

func GetFloatEQConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	var values []float64
	if !params.int2float {
		values = col.FloatValues()
	} else {
		values = Int64ToFloat64Slice(col.IntegerValues())
	}
	cmpData, _ := compare.(float64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if values[i] != cmpData {
			bitmap.SetBitMap(pos, idx)
		}
	}
	return pos
}

func GetIntegerEQConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	values := col.IntegerValues()
	cmpData, _ := compare.(int64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if values[i] != cmpData {
			bitmap.SetBitMap(pos, idx)
		}
	}
	return pos
}

func GetStringEQConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	cmpData := util.Str2bytes(compare.(string))
	for i := 0; i < col.Len-1; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if !bytes.Equal(col.Val[col.Offset[i]:col.Offset[i+1]], cmpData) {
			bitmap.SetBitMap(pos, idx)
		}
	}
	idx = offset + col.Len - 1
	if bitmap.IsNil(pos, idx) {
		return pos
	}
	if !bytes.Equal(col.Val[col.Offset[col.Len-1]:], cmpData) {
		bitmap.SetBitMap(pos, idx)
	}
	return pos
}

func GetBooleanEQConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	values := col.BooleanValues()
	cmpData, _ := compare.(bool)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if values[i] != cmpData {
			bitmap.SetBitMap(pos, idx)
		}
	}
	return pos
}

func GetFloatEQConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	var values []float64
	if !params.int2float {
		values = col.FloatValues()
	} else {
		values = Int64ToFloat64Slice(col.IntegerValues())
	}
	var idx int
	var index int
	cmpData, _ := compare.(float64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			if !bitmap.IsNil(bitMap, idx) {
				index++
			}
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if values[index] != cmpData {
			bitmap.SetBitMap(pos, idx)
		}
		index++
	}
	return pos
}

func GetIntegerEQConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	values := col.IntegerValues()
	var idx int
	var index int
	cmpData, _ := compare.(int64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			if !bitmap.IsNil(bitMap, idx) {
				index++
			}
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if values[index] != cmpData {
			bitmap.SetBitMap(pos, idx)
		}
		index++
	}
	return pos
}

func GetStringEQConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	var idx int
	cmpData := util.Str2bytes(compare.(string))
	for i := 0; i < col.Len-1; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if !bytes.Equal(col.Val[col.Offset[i]:col.Offset[i+1]], cmpData) {
			bitmap.SetBitMap(pos, idx)
		}
	}
	idx = offset + col.Len - 1
	if bitmap.IsNil(pos, idx) {
		return pos
	}
	if bitmap.IsNil(bitMap, idx) || !bytes.Equal(col.Val[col.Offset[col.Len-1]:], cmpData) {
		bitmap.SetBitMap(pos, idx)
	}
	return pos
}

func GetBooleanEQConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	values := col.BooleanValues()
	var idx int
	var index int
	cmpData, _ := compare.(bool)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			if !bitmap.IsNil(bitMap, idx) {
				index++
			}
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if values[index] != cmpData {
			bitmap.SetBitMap(pos, idx)
		}
		index++
	}
	return pos
}

func GetFloatNEQConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetFloatNEQConditionBitMapWithoutNull(params)
	}
	return GetFloatNEQConditionBitMapWithNull(params)
}

func GetIntegerNEQConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetIntegerNEQConditionBitMapWithoutNull(params)
	}
	return GetIntegerNEQConditionBitMapWithNull(params)
}

func GetStringNEQConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetStringNEQConditionBitMapWithoutNull(params)
	}
	return GetStringNEQConditionBitMapWithNull(params)
}

func GetBooleanNEQConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetBooleanNEQConditionBitMapWithoutNull(params)
	}
	return GetBooleanNEQConditionBitMapWithNull(params)
}

func GetFloatNEQConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	var values []float64
	if !params.int2float {
		values = col.FloatValues()
	} else {
		values = Int64ToFloat64Slice(col.IntegerValues())
	}
	cmpData, _ := compare.(float64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if values[i] == cmpData {
			bitmap.SetBitMap(pos, idx)
		}
	}
	return pos
}

func GetIntegerNEQConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	values := col.IntegerValues()
	cmpData, _ := compare.(int64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if values[i] == cmpData {
			bitmap.SetBitMap(pos, idx)
		}
	}
	return pos
}

func GetStringNEQConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	cmpData := util.Str2bytes(compare.(string))
	for i := 0; i < col.Len-1; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if bytes.Equal(col.Val[col.Offset[i]:col.Offset[i+1]], cmpData) {
			bitmap.SetBitMap(pos, idx)
		}
	}
	idx = offset + col.Len - 1
	if bitmap.IsNil(pos, idx) {
		return pos
	}
	if bytes.Equal(col.Val[col.Offset[col.Len-1]:], cmpData) {
		bitmap.SetBitMap(pos, idx)
	}
	return pos
}

func GetBooleanNEQConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos := params.compare, params.col, params.offset, params.pos
	values := col.BooleanValues()
	cmpData, _ := compare.(bool)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if values[i] == cmpData {
			bitmap.SetBitMap(pos, idx)
		}
	}
	return pos
}

func GetFloatNEQConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	var values []float64
	if !params.int2float {
		values = col.FloatValues()
	} else {
		values = Int64ToFloat64Slice(col.IntegerValues())
	}
	var idx int
	var index int
	cmpData, _ := compare.(float64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			if !bitmap.IsNil(bitMap, idx) {
				index++
			}
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if values[index] == cmpData {
			bitmap.SetBitMap(pos, idx)
		}
		index++
	}
	return pos
}

func GetIntegerNEQConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	values := col.IntegerValues()
	var idx int
	var index int
	cmpData, _ := compare.(int64)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			if !bitmap.IsNil(bitMap, idx) {
				index++
			}
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if values[index] == cmpData {
			bitmap.SetBitMap(pos, idx)
		}
		index++
	}
	return pos
}

func GetStringNEQConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	var idx int
	cmpData := util.Str2bytes(compare.(string))
	for i := 0; i < col.Len-1; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if bytes.Equal(col.Val[col.Offset[i]:col.Offset[i+1]], cmpData) {
			bitmap.SetBitMap(pos, idx)
		}
	}
	idx = offset + col.Len - 1
	if bitmap.IsNil(pos, idx) {
		return pos
	}
	if bitmap.IsNil(bitMap, idx) || bytes.Equal(col.Val[col.Offset[col.Len-1]:], cmpData) {
		bitmap.SetBitMap(pos, idx)
	}
	return pos
}

func GetBooleanNEQConditionBitMapWithNull(params *TypeFunParams) []byte {
	compare, col, offset, pos, bitMap := params.compare, params.col, params.offset, params.pos, params.bitMap
	values := col.BooleanValues()
	var idx int
	var index int
	cmpData, _ := compare.(bool)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			if !bitmap.IsNil(bitMap, idx) {
				index++
			}
			continue
		}

		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if values[index] == cmpData {
			bitmap.SetBitMap(pos, idx)
		}
		index++
	}
	return pos
}

func GetStringMatchPhraseConditionBitMap(params *TypeFunParams) []byte {
	if params.col.NilCount == 0 {
		return GetStringMatchPhraseConditionBitMapWithoutNull(params)
	}
	return GetStringMatchPhraseConditionBitMapWithNull(params)
}

func GetStringMatchPhraseConditionBitMapWithoutNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, opt, pos := params.compare, params.col, params.offset, params.opt, params.pos
	goal := util.Str2bytes(compare.(string))
	var content []byte
	var tokensTable []byte
	measurements := opt.GetMeasurements()
	if len(measurements) == 0 {
		tokensTable = tokenizer.GetFullTextOption(nil).TokensTable
	} else {
		tokensTable = tokenizer.GetFullTextOption(measurements[0].IndexRelation).TokensTable
	}
	tokenFinder := tokenizer.NewSimpleTokenFinder(tokensTable)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if i == col.Len-1 {
			content = col.Val[col.Offset[i]:]
		} else {
			content = col.Val[col.Offset[i]:col.Offset[i+1]]
		}
		tokenFinder.InitInput(content, goal)
		if !tokenFinder.Next() {
			bitmap.SetBitMap(pos, idx)
		}
	}
	return pos
}

func GetStringMatchPhraseConditionBitMapWithNull(params *TypeFunParams) []byte {
	var idx int
	compare, col, offset, pos, opt, bitMap := params.compare, params.col, params.offset, params.pos, params.opt, params.bitMap
	goal := util.Str2bytes(compare.(string))
	var content []byte
	var tokensTable []byte
	measurements := opt.GetMeasurements()
	if len(measurements) == 0 {
		tokensTable = tokenizer.GetFullTextOption(nil).TokensTable
	} else {
		tokensTable = tokenizer.GetFullTextOption(measurements[0].IndexRelation).TokensTable
	}
	tokenFinder := tokenizer.NewSimpleTokenFinder(tokensTable)
	for i := 0; i < col.Len; i++ {
		idx = offset + i
		if bitmap.IsNil(pos, idx) {
			continue
		}
		if bitmap.IsNil(bitMap, idx) {
			bitmap.SetBitMap(pos, idx)
			continue
		}
		if i == col.Len-1 {
			content = col.Val[col.Offset[i]:]
		} else {
			content = col.Val[col.Offset[i]:col.Offset[i+1]]
		}
		tokenFinder.InitInput(content, goal)
		if !tokenFinder.Next() {
			bitmap.SetBitMap(pos, idx)
		}
	}
	return pos
}
