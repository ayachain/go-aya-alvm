package lua

import (
	"fmt"
	"github.com/ipfs/go-mfs"
	"os"
	"path/filepath"
	"strings"
)

/* load lib {{{ */

//loLoaderLua是从本机文件系统载入资源，在AApp中一律从MFS中载入
//var loLoaders = []LGFunction{loLoaderPreload, loLoaderLua}

var loLoaders = []LGFunction{loLoaderPreload, loLoaderMFS}

func loGetPath(env string, defpath string) string {
	path := os.Getenv(env)
	if len(path) == 0 {
		path = defpath
	}
	path = strings.Replace(path, ";;", ";"+defpath+";", -1)
	if os.PathSeparator != '/' {
		dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
		if err != nil {
			panic(err)
		}
		path = strings.Replace(path, "!", dir, -1)
	}
	return path
}

func loFindFile(L *LState, name, pname string) (string, string) {
	name = strings.Replace(name, ".", string(os.PathSeparator), -1)
	lv := L.GetField(L.GetField(L.Get(EnvironIndex), "package"), pname)
	path, ok := lv.(LString)
	if !ok {
		L.RaiseError("package.%s must be a string", pname)
	}
	messages := []string{}
	for _, pattern := range strings.Split(string(path), ";") {
		luapath := strings.Replace(pattern, "?", name, -1)
		if _, err := os.Stat(luapath); err == nil {
			return luapath, ""
		} else {
			messages = append(messages, err.Error())
		}
	}
	return "", strings.Join(messages, "\n\t")
}

func OpenPackage(L *LState) int {

	packagemod := L.RegisterModule(LoadLibName, loFuncs)

	L.SetField(packagemod, "preload", L.NewTable())

	loaders := L.CreateTable(len(loLoaders), 0)
	for i, loader := range loLoaders {
		L.RawSetInt(loaders, i+1, L.NewFunction(loader))
	}
	L.SetField(packagemod, "loaders", loaders)
	L.SetField(L.Get(RegistryIndex), "_LOADERS", loaders)

	loaded := L.NewTable()
	L.SetField(packagemod, "loaded", loaded)
	L.SetField(L.Get(RegistryIndex), "_LOADED", loaded)

	L.SetField(packagemod, "path", LString(loGetPath(LuaPath, LuaPathDefault)))
	L.SetField(packagemod, "cpath", emptyLString)

	L.Push(packagemod)
	return 1
}

var loFuncs = map[string]LGFunction{
	"loadlib": loLoadLib,
	"seeall":  loSeeAll,
}

func loLoaderPreload(L *LState) int {
	name := L.CheckString(1)
	preload := L.GetField(L.GetField(L.Get(EnvironIndex), "package"), "preload")
	if _, ok := preload.(*LTable); !ok {
		L.RaiseError("package.preload must be a table")
	}
	lv := L.GetField(preload, name)
	if lv == LNil {
		L.Push(LString(fmt.Sprintf("no field package.preload['%s']", name)))
		return 1
	}
	L.Push(lv)
	return 1
}

func loLoaderLua(L *LState) int {
	name := L.CheckString(1)
	path, msg := loFindFile(L, name, "path")
	if len(path) == 0 {
		L.Push(LString(msg))
		return 1
	}
	fn, err1 := L.LoadFile(path)
	if err1 != nil {
		L.RaiseError(err1.Error())
	}
	L.Push(fn)
	return 1
}

func loLoadLib(L *LState) int {
	L.RaiseError("loadlib is not supported")
	return 0
}

func loLoaderMFS(L *LState) int {

	name := L.CheckString(1)

	name = strings.Replace(name, ".", string(os.PathSeparator), -1)
	searchPath := "/Script/?.lua;/Script/?/init.lua;/Script/?/main.lua"
	messages := []string{}

	luafi := &mfs.File{}
	var findError error

	for _, pattern := range strings.Split(string(searchPath), ";") {

		luapath := strings.Replace(pattern, "?", name, -1)

		if luafi, findError = L.MFS_LookupFile(luapath); findError == nil {
			break
		} else {
			messages = append(messages, findError.Error())
		}

	}

	if findError != nil {
		L.Push(LString(strings.Join(messages, "\n\t")))
		return 1
	}

	frd, err := luafi.Open(mfs.Flags{Read:true})
	if err != nil {
		L.Push(LString(err.Error()))
		return 1
	}

	fn, err1 := L.Load(frd, name)
	if err1 != nil {
		L.RaiseError(err1.Error())
	}

	L.Push(fn)
	return 1
}

func loSeeAll(L *LState) int {
	mod := L.CheckTable(1)
	mt := L.GetMetatable(mod)
	if mt == LNil {
		mt = L.CreateTable(0, 1)
		L.SetMetatable(mod, mt)
	}
	L.SetField(mt, "__index", L.Get(GlobalsIndex))
	return 0
}

/* }}} */

//
