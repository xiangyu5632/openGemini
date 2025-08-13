// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compactor_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/immutable/compactor"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

var (
	testTimeStart = time.Date(2021, 11, 1, 0, 0, 0, 0, time.UTC)
	timeInterval  = time.Second
)

var schemaForColumnStore = []record.Field{
	{Name: "field1_float", Type: influx.Field_Type_Float},
	{Name: "field2_int", Type: influx.Field_Type_Int},
	{Name: "field3_bool", Type: influx.Field_Type_Boolean},
	{Name: "field4_string", Type: influx.Field_Type_String},
	{Name: "primaryKey_float1", Type: influx.Field_Type_Float},
	{Name: "primaryKey_string1", Type: influx.Field_Type_String},
	{Name: "primaryKey_string2", Type: influx.Field_Type_String},
	{Name: "sortKey_int1", Type: influx.Field_Type_Int},
	{Name: "time", Type: influx.Field_Type_Int},
}

func genTestDataForColumnStore(rows int, pkFlag int, starTime *time.Time) *record.Record {
	tm := *starTime

	value := 1.1

	genRecFn := func() *record.Record {
		bv := false
		b := record.NewRecordBuilder(schemaForColumnStore)
		for i := 1; i <= rows; i++ {
			b.Column(0).AppendFloat(value)
			b.Column(1).AppendInteger(int64(value))
			b.Column(2).AppendBoolean(bv)
			b.Column(3).AppendString(fmt.Sprintf("test-%f", value))
			b.Column(4).AppendFloat(float64(i % 100))
			b.Column(5).AppendString(fmt.Sprintf("pk-string-%d-%d", pkFlag, i%10))
			b.Column(6).AppendString(fmt.Sprintf("pk-string-%d,%d", pkFlag, 1%10+1))
			b.Column(7).AppendInteger(int64(value))
			b.Column(8).AppendInteger(tm.UnixNano()) // timestamp

			tm = tm.Add(timeInterval)
			value += 1.0
			bv = !bv
		}

		*starTime = tm
		return b
	}

	return genRecFn()
}

func defaultIdent() colstore.MeasurementIdent {
	ident := colstore.NewMeasurementIdent("db0", "rp0")
	ident.SetName("mst")
	return ident
}

func clearMstInfo() {
	colstore.MstManagerIns().Clear()
}

func defaultColumnStoreMstInfo(pk, sk []string, tc time.Duration) *meta.MeasurementInfo {
	fields := make(meta.CleanSchema)
	for i := range pk {
		for j := range schemaForColumnStore {
			if pk[i] == schemaForColumnStore[j].Name {
				fields[pk[i]] = meta.SchemaVal{Typ: int8(schemaForColumnStore[j].Type)}
			}
		}
	}

	return &meta.MeasurementInfo{
		Name:       "mst",
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			PrimaryKey:          pk,
			SortKey:             sk,
			TimeClusterDuration: tc,
		},
		Schema: &fields,
	}
}

func initColumnStoreMstInfo(mi *meta.MeasurementInfo) (*colstore.Measurement, bool) {
	ident := defaultIdent()
	colstore.MstManagerIns().Add(ident, mi)

	mst, ok := colstore.MstManagerIns().GetByIdent(ident)
	if !ok {
		return nil, false
	}

	return mst, true
}

func createColStore(t *testing.T, mst *colstore.Measurement, filesN int) (*immutable.MmsTables, *immutable.TSSPFiles, func()) {
	conf := immutable.NewColumnStoreConfig()
	tier := uint64(util.Hot)
	lockPath := ""

	store := immutable.NewTableStore(t.TempDir(), &lockPath, &tier, true, conf)
	store.SetDbRp("db0", "rp0")
	store.SetImmTableType(config.COLUMNSTORE)
	store.CompactionEnable()

	err := writeDefaultData(store, mst, filesN)
	require.NoError(t, err)

	files, ok := store.CSFiles["mst"]
	require.True(t, ok)
	require.Equal(t, filesN, len(files.Files()))

	return store, files, func() {
		_ = store.Close()
	}
}

func writeDefaultData(store *immutable.MmsTables, mst *colstore.Measurement, fileNum int) error {
	recRows := 1000
	tm := testTimeStart

	oldRec := record.NewRecordBuilder(schemaForColumnStore)
	oldRec.ReserveColumnRows(recRows * fileNum)

	recs := make([]*record.Record, 0, fileNum)

	for i := 0; i < fileNum; i++ {
		rec := genTestDataForColumnStore(recRows, i%2, &tm)
		ir := mst.IndexRelation()
		wa := mutable.NewWriteAttached("mst", mst.PrimaryKey(), mst.SortKey(), &ir)
		wa.FlushRecord(store, rec)
		recs = append(recs, rec)
	}
	return nil
}

func itrTSSPFile(f immutable.TSSPFile, hook func(sid uint64, rec *record.Record)) {
	fi := immutable.NewFileIterator(f, immutable.CLog)
	itr := immutable.NewChunkIterator(fi)

	for {
		if !itr.Next() {
			break
		}

		sid := itr.GetSeriesID()
		if sid == 0 {
			panic("series ID is zero")
		}
		rec := itr.GetRecord()
		record.CheckRecord(rec)

		hook(sid, rec)
	}
}

func TestColStoreCompactor_case1(t *testing.T) {
	primaryKey := []string{"primaryKey_string1"}
	sortKey := []string{"time"}
	defer clearMstInfo()
	mst, ok := initColumnStoreMstInfo(defaultColumnStoreMstInfo(primaryKey, sortKey, 0))
	require.True(t, ok)

	conf := immutable.GetColStoreConfig()

	t.Run("MaxRowsPerSegment=64", func(t *testing.T) {
		conf.SetMaxRowsPerSegment(64)
		runColStoreCompactor(t, mst)
	})

	t.Run("MaxRowsPerSegment=default", func(t *testing.T) {
		conf.SetMaxRowsPerSegment(util.DefaultMaxRowsPerSegment4ColStore)
		runColStoreCompactor(t, mst)
	})
}

func runColStoreCompactor(t *testing.T, mst *colstore.Measurement) {
	store, files, release := createColStore(t, mst, immutable.LeveLMinGroupFiles[0])
	defer release()

	var oldFiles []immutable.TSSPFile
	oldFiles = append(oldFiles, files.Files()...)

	ident := colstore.NewMeasurementIdent("db0", "rp0")
	ident.SetName("mst")
	comp := immutable.NewColStoreCompactor(store, "mst")
	newFiles, err := comp.Compact(ident, oldFiles)
	require.NoError(t, err)
	require.Equal(t, 1, len(newFiles))

	err = store.ReplaceFiles("mst", oldFiles, newFiles, true)
	require.NoError(t, err)

	files, ok := store.CSFiles["mst"]
	require.True(t, ok)
	require.Equal(t, 1, len(files.Files()))

	pki := files.Files()[0].GetPkInfo()
	require.NotEmpty(t, pki)

	itrTSSPFile(files.Files()[0], func(sid uint64, rec *record.Record) {
		record.CheckRecord(rec)
		require.Equal(t, colstore.SeriesID, sid)
		require.Equal(t, 8000, rec.RowNums())
	})
}

func TestInvalidPKInfo(t *testing.T) {
	primaryKey := []string{"primaryKey_string1"}
	sortKey := []string{"time"}
	defer clearMstInfo()
	mst, ok := initColumnStoreMstInfo(defaultColumnStoreMstInfo(primaryKey, sortKey, 0))
	require.True(t, ok)

	_, files, release := createColStore(t, mst, immutable.LeveLMinGroupFiles[0])
	defer release()

	rec := &record.Record{}
	rec.ResetWithSchema(mst.PrimaryKey())
	file := files.Files()[0]
	file.SetPkInfo(colstore.NewPKInfo(rec, &fragment.IndexFragmentVariableImpl{}, -1))

	_, err := compactor.NewCSFileIterator(file, mst.SortKey())
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid primary key")
}
