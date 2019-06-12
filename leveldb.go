package lua

import (
	"errors"
	adb "github.com/ayachain/go-aya-alvm-adb"
	"github.com/syndtr/goleveldb/leveldb"
	adbIt "github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
	"strings"
)

var (
	errEncodeKeyError = errors.New("ADB : encode key expected")
	errEncodeValueError = errors.New("ADB : encode value expected")
	errDecodeKeyError = errors.New("ADB : decode key expected")
	errDecodeValueError = errors.New("ADB : decode value expected")
)

var levelDBFuncs = map[string]LGFunction{
	"open"		:    dbOpenFile,
}

var iteratorMethods = map[string]LGFunction{
	"valid"		: itVaild,
	"first"		: itFirst,
	"last"		: itLast,
	"seek"		: itSeek,
	"next"		: itNext,
	"prev"		: itPrev,
	"key"		: itKey,
	"value"		: itValue,
	"error"		: itError,
	"release"	: itRelease,
}

var batchMethods = map[string]LGFunction {
	"put"		:	ldbBatchPut,
	"delete"	:	ldbBatchDel,
	"len"		:	ldbBatchLen,
	"reset"		:	ldbBatchReset,
	"write"		:	ldbBatchWrite,
}

var dbMethods = map[string]LGFunction {
	"put"			:	ldbPut,
	"get"			:	ldbGet,
	"has"			:	ldbHas,
	"delete"		:	ldbDelete,
	"write" 		:	ldbWrite,
	"close" 		:	ldbClose,
	"newBatch"		:	ldbBatch,
	"newIterator"	:	ldbIterator,
}

const lLevelIteratorClass = "abd.Iterator*"
const lLevelDBBatchClass = "adb.Batch*"
const lLevelDBClass = "adb*"

type adbBatch struct {
	*leveldb.Batch
	parent *leveldb.DB
}

func checkIterator(L *LState) adbIt.Iterator {

	ud := L.CheckUserData(1)

	if it, ok := ud.Value.(adbIt.Iterator); ok {
		return it
	}

	L.ArgError(1, "ADB.Iterator expected")

	return nil

}

func checkBatch(L *LState) *adbBatch {

	ud := L.CheckUserData(1)

	if batch, ok := ud.Value.(*adbBatch); ok {
		return batch
	}

	L.ArgError(1, "ADB.batch expected")

	return nil

}

func checkLevelDB(L *LState) *leveldb.DB {

	ud := L.CheckUserData(1)

	if db, ok := ud.Value.(*leveldb.DB); ok {
		return db
	}

	L.ArgError(1, "ADB.DB expected")

	return nil
}

func OpenLevelDB(L *LState) int {

	mod := L.RegisterModule(LevelDBLibName, map[string]LGFunction{}).(*LTable)

	mt := L.NewTypeMetatable(lLevelDBClass)
	mt.RawSetString("__index", mt)
	L.SetFuncs(mt, dbMethods)


	//batch
	batchMt := L.NewTypeMetatable(lLevelDBBatchClass)
	batchMt.RawSetString("__index", batchMt)
	L.SetFuncs(batchMt, batchMethods)


	//iterator
	itMt := L.NewTypeMetatable(lLevelIteratorClass)
	itMt.RawSetString("__index", itMt)
	L.SetFuncs(itMt, iteratorMethods)

	//mt.RawSetString("lines", L.NewClosure(fileLines, L.NewFunction(fileLinesIter)))
	//uv := L.CreateTable(2, 0)
	//uv.RawSetInt(fileDefOutIndex, mod.RawGetString("stdout"))
	//uv.RawSetInt(fileDefInIndex, mod.RawGetString("stdin"))
	for name, fn := range levelDBFuncs {
		mod.RawSetString( name, L.NewClosure(fn) )
	}

	//mod.RawSetString("lines", L.NewClosure(ioLines, uv, L.NewClosure(ioLinesIter, uv)))
	//mod.RawSetString("lines", L.NewClosure(ioLines, L.NewFunction(ioLinesIter)) )
	//Modifications are being made in-place rather than returned?
	L.Push(mod)
	return 1
}

func ldbBatch(L *LState) int {

	db := checkLevelDB( L )
	if db == nil {
		L.Push(LNil)
		return 1
	}

	batch := &adbBatch{
		Batch:&leveldb.Batch{},
		parent:db,
	}

	ud := L.NewUserData()
	ud.Value = batch
	L.SetMetatable(ud, L.GetTypeMetatable(lLevelDBBatchClass))
	L.Push(ud)

	return 1
}

// if path is exist open, if not created dir
func dbOpenFile(L *LState) int {

	path := L.CheckString(1)

	if !strings.HasPrefix(path, "/") {
		path = "/Data/" + path
	} else {
		path = "/Data" + path
	}

	dir, err := L.MFS_LookupDir( path )
	if err != nil {

		if err := L.MFS_Mkdir(path, true); err != nil {
			L.RaiseError("%v", err.Error())
			L.Push(LNil)
			return 1
		} else {

			dir, err = L.MFS_LookupDir(path)
			if err != nil {
				L.RaiseError("%v", err.Error())
				L.Push(LNil)
				return 1
			}

		}

	}

	mstorage := adb.NewMFSStorage(dir)

	db, err := leveldb.Open( mstorage, nil )
	if err != nil {
		L.RaiseError("%v", err.Error())
		L.Push(LNil)
		return 1
	}

	ud := L.NewUserData()

	ud.Value = db

	L.SetMetatable(ud, L.GetTypeMetatable(lLevelDBClass))

	L.Push(ud)

	return 1
}

func ldbGet(L *LState) int {

	switch L.GetTop() {
	case 1:
		L.ArgError(2,"Adb Get : miss key")
		L.Push(LNil)
		return 1

	default:
		break
	}

	db := checkLevelDB( L )
	if db == nil {
		L.Push(LNil)
		return 1
	}

	lvkey := L.Get(2)
	key, err := Encode(lvkey)
	if err != nil {
		L.RaiseError("%v", errEncodeKeyError)
		L.Push(LNil)
		return 1
	}

	if v, err := db.Get(key,nil); err != nil {
		L.RaiseError("%v", err.Error())
		L.Push(LNil)
	} else {

		lv, err := L.Decode(v)
		if err != nil {
			L.RaiseError("%v", err.Error())
			L.Push(LNil)
		} else {
			L.Push(lv)
		}

	}

	return 1
}

func ldbPut(L *LState) int {

	switch L.GetTop() {
	case 2:
		L.ArgError(2,"ADB Put : miss key or value")
		L.Push(LFalse)
		return 1
	default:
		break
	}

	db := checkLevelDB( L )
	if db == nil {
		L.Push(LFalse)
		return 1
	}

	lvkey := L.Get(2)
	lvvalue := L.Get(3)

	key, err := Encode(lvkey)
	if err != nil {
		L.RaiseError("%v", errEncodeKeyError)
		L.Push(LFalse)
		return 1
	}

	value, err := Encode(lvvalue)
	if err != nil {
		L.RaiseError("%v", errEncodeValueError)
		L.Push(LFalse)
		return 1
	}

	if err := db.Put(key, value, nil); err != nil {
		L.RaiseError("%v", err.Error())
		L.Push(LFalse)
		return 1
	}

	L.Push(LTrue)
	return 1
}

func ldbDelete(L *LState) int {

	switch L.GetTop() {
	case 1:
		L.ArgError(2,"ADB Delete : miss key")
		L.Push(LNil)
		return 1

	default:
		break
	}

	db := checkLevelDB( L )
	if db == nil {
		L.Push(LNil)
		return 1
	}

	lvkey := L.Get(2)
	key, err := Encode(lvkey)
	if err != nil {
		L.RaiseError("%v", errEncodeKeyError)
		L.Push(LNil)
		return 1
	}

	if err := db.Delete(key,nil); err != nil {
		L.RaiseError("%v", err.Error())
		L.Push(LFalse)
	} else {
		L.Push(LTrue)
	}

	return 1
}

func ldbHas(L *LState) int {

	switch L.GetTop() {
	case 1:
		L.ArgError(2,"ADB Get : miss key")
		L.Push(LNil)
		return 1

	default:
		break
	}

	db := checkLevelDB( L )
	if db == nil {
		L.Push(LNil)
		return 1
	}

	lvkey := L.Get(2)
	key, err := Encode(lvkey)
	if err != nil {
		L.RaiseError("%v", errEncodeKeyError)
		L.Push(LNil)
		return 1
	}

	exist, err := db.Has(key,nil)
	if err != nil {
		L.RaiseError("%v", err.Error())
		L.Push(LFalse)
	} else {
		L.Push(LBool(exist))
	}

	return 1

}

func ldbClose(L *LState) int {

	db := checkLevelDB( L )
	if db == nil {
		L.Push(LFalse)
		return 1
	}

	if err := db.Close(); err != nil {
		L.Push(LFalse)
		return 1
	} else {
		L.Push(LTrue)
		return 1
	}

}

func ldbWrite(L *LState) int {

	db := checkLevelDB( L )
	batch := checkBatch( L )

	if db == nil || batch == nil {
		L.Push(LFalse)
		return 1
	}

	if err := db.Write( batch.Batch, nil ); err != nil {
		L.RaiseError("%v", err.Error())
		L.Push(LFalse)
	} else {
		L.Push(LTrue)
	}

	return 1
}

///Batch
func ldbBatchPut(L *LState) int {

	switch L.GetTop() {
	case 2:
		L.ArgError(2,"ADB Batch Put : miss key or value")
		L.Push(LFalse)
		return 1
	default:
		break
	}

	batch := checkBatch( L )
	if batch == nil {
		L.Push(LFalse)
		return 1
	}

	lvkey := L.Get(2)
	lvvalue := L.Get(3)

	key, err := Encode(lvkey)
	if err != nil {
		L.RaiseError("%v", errEncodeKeyError)
		L.Push(LFalse)
		return 1
	}

	value, err := Encode(lvvalue)
	if err != nil {
		L.RaiseError("%v", errEncodeValueError)
		L.Push(LFalse)
		return 1
	}

	batch.Put(key, value)
	L.Push(LTrue)

	return 1

}

func ldbBatchDel(L *LState) int {

	switch L.GetTop() {
	case 1:
		L.ArgError(2,"ADB Batch Delete : miss key")
		L.Push(LNil)
		return 1

	default:
		break
	}

	batch := checkBatch( L )
	if batch == nil {
		L.Push(LNil)
		return 1
	}

	lvkey := L.Get(2)
	key, err := Encode(lvkey)
	if err != nil {
		L.RaiseError("%v", errEncodeKeyError)
		L.Push(LNil)
		return 1
	}

	batch.Delete(key)
	L.Push(LTrue)

	return 1
}

func ldbBatchLen(L *LState) int {

	batch := checkBatch( L )
	if batch == nil {
		L.Push(LNil)
		return 1
	}

	L.Push(LNumber(batch.Len()))
	return 1

}


func ldbBatchReset(L *LState) int {

	batch := checkBatch( L )
	if batch == nil {
		L.Push(LNil)
		return 1
	}

	batch.Reset()
	L.Push(LTrue)
	return 1
}

func ldbBatchWrite(L *LState) int {

	batch := checkBatch( L )
	if batch == nil {
		L.Push(LNil)
		return 1
	}

	if batch.parent == nil {
		L.RaiseError("%v", "ADB Batch Write : parent ADB is null")
		L.Push(LFalse)
		return 1
	}

	if err := batch.parent.Write(batch.Batch, nil); err != nil {
		L.RaiseError("%v", err.Error())
		L.Push(LFalse)
		return 1
	}

	L.Push(LTrue)
	return 1
}


//Iteraotr
func ldbIterator(L *LState) int {

	db := checkLevelDB( L )
	if db == nil {
		L.Push(LNil)
		return 1
	}

	var st, ed LValue

	n := L.GetTop()
	switch n {
	case 1:
		st = LNil
		ed = LNil
	case 2:
		st = L.Get(2)
		ed = LNil
	default:
		st = L.Get(2)
		ed = L.Get(3)
	}

	var sbs, ebs []byte
	var converr error

	if st.Type() != LTNil {

		if sbs, converr = Encode(st); converr != nil {
			L.RaiseError("%v", errEncodeKeyError)
			L.Push(LNil)
			return 1
		}

	}

	if ed.Type() != LTNil {

		if ebs, converr = Encode(ed); converr != nil {
			L.RaiseError("%v", errEncodeKeyError)
			L.Push(LNil)
			return 1
		}

	}

	rg := &util.Range{Start: sbs, Limit: ebs}
	it := db.NewIterator( rg, nil )

	ud := L.NewUserData()
	ud.Value = it

	L.SetMetatable(ud, L.GetTypeMetatable(lLevelIteratorClass))
	L.Push(ud)

	return 1
}


func itVaild(L *LState) int {

	it := checkIterator(L)

	if it == nil {
		L.Push(LFalse)
	} else {
		L.Push(LBool(it.Valid()))

	}

	return 1
}

func itFirst(L *LState) int {

	it := checkIterator(L)

	if it == nil {
		L.Push(LFalse)
	} else {
		L.Push(LBool(it.First()))

	}

	return 1

}

func itLast(L *LState) int {

	it := checkIterator(L)

	if it == nil {
		L.Push(LFalse)
	} else {
		L.Push(LBool(it.Last()))

	}

	return 1

}


func itSeek(L *LState) int {

	it := checkIterator(L)

	if it == nil {
		L.Push(LFalse)
		return 1
	}

	key := L.Get(2)
	keybs, err := Encode(key)

	if err != nil {
		L.RaiseError("%v", errEncodeKeyError)
	}

	L.Push(LBool(it.Seek(keybs)))
	return 1
}

func itNext(L *LState) int {

	it := checkIterator(L)

	if it == nil {
		L.Push(LFalse)
	} else {
		L.Push(LBool(it.Next()))
	}

	return 1
}


func itPrev(L *LState) int {

	it := checkIterator(L)

	if it == nil {
		L.Push(LFalse)
	} else {
		L.Push(LBool(it.Prev()))
	}

	return 1
}

func itKey(L *LState) int {

	it := checkIterator(L)

	if it == nil {
		L.Push(LNil)
		return 1
	}

	lv, err := L.Decode(it.Key())
	if err != nil {
		L.RaiseError("%v", errDecodeKeyError)
		L.Push(LNil)
	} else {
		L.Push(lv)
	}

	return 1
}

func itValue(L *LState) int {

	it := checkIterator(L)

	if it == nil {
		L.Push(LNil)
		return 1
	}

	lv, err := L.Decode(it.Value())
	if err != nil {
		L.RaiseError("%v", errDecodeValueError)
		L.Push(LNil)
	} else {
		L.Push(lv)
	}

	return 1
}

func itError(L *LState) int {

	it := checkIterator(L)

	if it == nil {

		L.Push( LString("ADB.Iterator expected") )

	} else {

		if it.Error() != nil {
			L.Push( LString(it.Error().Error()) )
		} else {
			L.Push(LNil)
		}

	}

	return 1
}

func itRelease(L *LState) int {

	it := checkIterator(L)

	if it == nil {
		L.Push( LString("ADB.Iterator expected") )
	} else {
		it.Release()
	}

	return 0
}