package lua

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/ipfs/go-mfs"
	"io"
	"io/ioutil"
	"os"
	gopath "path"
	"strings"
)

var ioFuncs = map[string]LGFunction{
	"open":    ioOpenFile,
	"close":   ioClose,
	"lines":   ioLines,
	"input":   ioInput,
	"output":  ioOutput,
	"read":    ioRead,
	"type":    ioType,
	//"tmpfile": ioTmpFile,
	"write":   ioWrite,
}

const lFileClass = "FILE*"

type lFile struct {
	vfp	   *mfs.File
	name   string
	flag   int
	seek   int64
	writable bool
	readable bool
	closed bool
}

func (lf *lFile) Size() int64 {

	size, err := lf.vfp.Size()

	if err != nil {
		return 0
	} else {
		return size
	}

}

func (lf *lFile) getReader(L *LState) (rd io.Reader, err error) {

	var e error
	var fwt mfs.FileDescriptor
	var flen int64

	defer func() {

		if e != nil && fwt != nil{
			fwt.Close()
		}

	}()

	fwt, e = lf.vfp.Open( mfs.Flags{Read:true, Sync:false} )

	if e != nil {
		return nil, e
	}

	flen, e = fwt.Size()
	if e != nil {
		return nil, e
	}

	if int64(lf.seek) > flen {
		return nil, fmt.Errorf("offset was past end of file (%d > %d)", int64(lf.seek), flen)
	}

	if _, e = fwt.Seek( int64(lf.seek), 0); e != nil {
		return nil, e
	}

	return fwt, nil
}

func (lf *lFile) getWriter(L *LState) (wt io.Writer, err error ) {

	var e error
	var fwt mfs.FileDescriptor
	var flen int64

	defer func() {

		if e != nil && fwt != nil{
			fwt.Close()
		}

	}()

	fwt, e = lf.vfp.Open( mfs.Flags{Write:true, Sync:false} )

	if e != nil {
		return nil, e
	}

	flen, e = fwt.Size()
	if e != nil {
		return nil, e
	}

	if int64(lf.seek) > flen {
		return nil, fmt.Errorf("offset was past end of file (%d > %d)", int64(lf.seek), flen)
	}

	if _, e = fwt.Seek(int64(lf.seek), 0); e != nil {
		return nil, e
	}

	return fwt, nil
}

type lFileType int

const (
	lFileFile lFileType = iota
)

const fileDefOutIndex = 1
const fileDefInIndex = 2

func checkFile(L *LState) *lFile {
	ud := L.CheckUserData(1)
	if file, ok := ud.Value.(*lFile); ok {
		return file
	}
	L.ArgError(1, "file expected")
	return nil
}

func errorIfFileIsClosed(L *LState, file *lFile) {
	if file.closed {
		L.ArgError(1, "file is closed")
	}
}

func newFile(L *LState, vfile *mfs.File, path string, flag int, writable, readable bool) (*LUserData, error) {

	if !strings.HasPrefix(path, "/") {
		path = "/Data/" + path
	} else {
		path = "/Data" + path
	}

	ud := L.NewUserData()
	var err error
	_, name := gopath.Split(path)

	if vfile == nil {

		vfile, err = L.MFS_OpenFile(path, flag)
		if err != nil {
			return nil, err
		}
	}

	lfile := &lFile{
		name : name,
		vfp: vfile,
		flag:flag,
		seek:0,
		writable:writable,
		readable:readable,
		closed: false,
	}

	ud.Value = lfile

	L.SetMetatable(ud, L.GetTypeMetatable(lFileClass))

	return ud, nil
}

func (file *lFile) Type() lFileType {
	return lFileFile
}

func (file *lFile) Name() string {
	return file.name
}

func fileDefOut(L *LState) *LUserData {
	return L.Get(UpvalueIndex(1)).(*LTable).RawGetInt(fileDefOutIndex).(*LUserData)
}

func fileDefIn(L *LState) *LUserData {
	return L.Get(UpvalueIndex(1)).(*LTable).RawGetInt(fileDefInIndex).(*LUserData)
}

func fileIsWritable(L *LState, file *lFile) int {

	if !file.writable {
		L.Push(LNil)
		L.Push(LString(fmt.Sprintf("%s is opened for only reading.", file.Name())))
		L.Push(LNumber(1)) // C-Lua compatibility: Original Lua pushes errno to the stack
		return 3
	}

	return 0
}

func fileIsReadable(L *LState, file *lFile) int {

	if !file.readable {
		L.Push(LNil)
		L.Push(LString(fmt.Sprintf("%s is opened for only writing.", file.Name())))
		L.Push(LNumber(1)) // C-Lua compatibility: Original Lua pushes errno to the stack
		return 3
	}

	return 0
}

func OpenIo(L *LState) int {
	mod := L.RegisterModule(IoLibName, map[string]LGFunction{}).(*LTable)
	mt := L.NewTypeMetatable(lFileClass)
	mt.RawSetString("__index", mt)
	L.SetFuncs(mt, fileMethods)
	mt.RawSetString("lines", L.NewClosure(fileLines, L.NewFunction(fileLinesIter)))

	//for _, finfo := range stdFiles {
	//	file, _ := newFile(L, finfo.file, "", 0, os.FileMode(0), finfo.writable, finfo.readable)
	//	mod.RawSetString(finfo.name, file)
	//}

	uv := L.CreateTable(2, 0)
	uv.RawSetInt(fileDefOutIndex, mod.RawGetString("stdout"))
	uv.RawSetInt(fileDefInIndex, mod.RawGetString("stdin"))
	for name, fn := range ioFuncs {
		mod.RawSetString(name, L.NewClosure(fn, uv))
	}
	mod.RawSetString("lines", L.NewClosure(ioLines, uv, L.NewClosure(ioLinesIter, uv)))
	//mod.RawSetString("lines", L.NewClosure(ioLines, L.NewFunction(ioLinesIter)) )
	//Modifications are being made in-place rather than returned?
	L.Push(mod)
	return 1
}

var fileMethods = map[string]LGFunction{
	"__tostring": fileToString,
	"write":      fileWrite,
	"close":      fileClose,
	"lines":      fileLines,
	"read":       fileRead,
	"seek":       fileSeek,
	//"setvbuf":    fileSetVBuf,
}

func fileToString(L *LState) int {

	file := checkFile(L)

	if file.closed {
		L.Push(LString("file :" + file.Name() + " (closed)"))
	} else {
		L.Push(LString("file :" + file.Name() + " (opened)"))
	}

	return 1
}

func fileWriteAux(L *LState, file *lFile, idx int) int {

	if n := fileIsWritable(L, file); n != 0 {
		return n
	}
	errorIfFileIsClosed(L, file)
	top := L.GetTop()
	
	out, werr := file.getWriter(L)
	defer func() {
		if out != nil {
			out.(mfs.FileDescriptor).Close()
		}
	}()

	if werr != nil {
		L.Push(LNil)
		L.Push(LString(werr.Error()))
		L.Push(LNumber(1)) // C-Lua compatibility: Original Lua pushes errno to the stack
		return 3
	}

	var err error
	for i := idx; i <= top; i++ {

		L.CheckTypes(i, LTNumber, LTString)
		s := LVAsString(L.Get(i))

		if _, err = out.Write(unsafeFastStringToReadOnlyBytes(s)); err != nil {
			goto errreturn
		}

		file.seek += int64(len(s))

	}

	L.Push(LTrue)
	return 1

errreturn:
	L.Push(LNil)
	L.Push(LString(err.Error()))
	L.Push(LNumber(1)) // C-Lua compatibility: Original Lua pushes errno to the stack
	return 3
}

func fileCloseAux(L *LState, file *lFile) int {

	file.closed = true;

	L.Push(LTrue)

	return 1
}

func fileReadAux(L *LState, file *lFile, idx int) int {

	if n := fileIsReadable(L, file); n != 0 {
		return n
	}

	errorIfFileIsClosed(L, file)

	if L.GetTop() == idx-1 {
		L.Push(LString("*l"))
	}

	var rdclose io.Reader
	var err error
	top := L.GetTop()

	rdclose, err = file.getReader(L)
	if err != nil {
		L.Push(LNil)
		L.RaiseError(err.Error())
		return 2
	}
	defer func() {
		if rdclose != nil {
			rdclose.(mfs.FileDescriptor).Close()
		}
	}()

	buffrd := bufio.NewReader(rdclose)

	for i := idx; i <= top; i++ {
		switch lv := L.Get(i).(type) {
		case LNumber:
			size := int64(lv)
			if size == 0 {
				_, err = buffrd.ReadByte()
				if err == io.EOF {
					L.Push(LNil)
					goto normalreturn
				}
				buffrd.UnreadByte()
			}
			var buf []byte
			var iseof bool
			buf, err, iseof = readBufioSize(buffrd, size)
			if iseof {
				file.seek = file.Size()
				L.Push(LNil)
				goto normalreturn
			}
			if err != nil {
				goto errreturn
			}

			file.seek += int64(int(size))
			L.Push(LString(string(buf)))

		case LString:
			options := L.CheckString(i)
			if len(options) > 0 && options[0] != '*' {
				options = "*" + options
				//L.ArgError(2, "invalid options:"+options)
			}
			for _, opt := range options[1:] {
				switch opt {
				case 'n':
					var v LNumber
					_, err = fmt.Fscanf(buffrd, LNumberScanFormat, &v)
					if err == io.EOF {
						file.seek = file.Size()
						L.Push(LNil)
						goto normalreturn
					}
					if err != nil {
						goto errreturn
					}

					file.seek += int64(len(v.String()))
					L.Push(v)

				case 'a':
					var buf []byte
					buf, err = ioutil.ReadAll(buffrd)
					if err == io.EOF {
						file.seek = file.Size()
						L.Push(emptyLString)
						goto normalreturn
					}
					if err != nil {
						goto errreturn
					}

					file.seek = file.Size()
					L.Push(LString(string(buf)))

				case 'l':
					var buf []byte
					var iseof bool
					buf, err, iseof = readBufioLine(buffrd)
					if iseof {
						file.seek = file.Size()
						L.Push(LNil)
						goto normalreturn
					}

					if err != nil {
						goto errreturn
					}

					if !iseof {
						file.seek += int64(len(buf) + 1)
					}
					L.Push(LString(string(buf)))

				default:
					L.ArgError(2, "invalid options:"+string(opt))
				}
			}
		}
	}
normalreturn:
	return L.GetTop() - top

errreturn:
	L.RaiseError(err.Error())
	//L.Push(LNil)
	//L.Push(LString(err.Error()))
	return 2
}

var fileSeekOptions = []string{"set", "cur", "end"}

func fileSeek(L *LState) int {

	file := checkFile(L)
	if file.Type() != lFileFile {
		L.Push(LNil)
		L.Push(LString("can not seek a process."))
		return 2
	}

	top := L.GetTop()
	if top == 1 {
		L.Push(LString("cur"))
		L.Push(LNumber(0))
	} else if top == 2 {
		L.Push(LNumber(0))
	}

	oindex := L.CheckOption(2, fileSeekOptions)

	var spos int64
	var err error

	switch oindex{
	case 0: //start
		spos = 0
	case 1: //cur
		spos = file.seek
	case 2: //end
		spos = file.Size()
	}

	pos := int64( spos + L.CheckInt64(3) )
	if pos >= file.Size() {
		err = fmt.Errorf("offset was past end of file (%d > %d)", pos, file.Size())
		goto errreturn
	}

	file.seek = pos
	L.Push(LNumber(pos))
	return 1

errreturn:
	L.Push(LNil)
	L.Push(LString(err.Error()))
	return 2
}

func fileWrite(L *LState) int {
	return fileWriteAux(L, checkFile(L), 2)
}

func fileClose(L *LState) int {
	return fileCloseAux(L, checkFile(L))
}

func fileLinesIter(L *LState) int {

	var file *lFile

	if ud, ok := L.Get(1).(*LUserData); ok {
		file = ud.Value.(*lFile)
	} else {
		file = L.Get(UpvalueIndex(2)).(*LUserData).Value.(*lFile)
	}

	rd, err := file.getReader(L)
	defer func() {
		if rd != nil {
			rd.(mfs.FileDescriptor).Close()
		}
	}()

	if err != nil {
		L.RaiseError(err.Error())
	}

	bufrd := bufio.NewReader(rd)
	buf, _, err := bufrd.ReadLine()

	if err != nil {

		if err == io.EOF {
			file.seek = file.Size()
			L.Push(LNil)
			return 1
		}

		L.RaiseError(err.Error())
	}

	file.seek += int64(len(buf) + 1)
	L.Push(LString(string(buf)))

	return 1
}

func fileLines(L *LState) int {
	file := checkFile(L)
	ud := L.CheckUserData(1)
	if n := fileIsReadable(L, file); n != 0 {
		return 0
	}
	L.Push(L.NewClosure(fileLinesIter, L.Get(UpvalueIndex(1)), ud))
	return 1
}

func ioLines(L *LState) int {
	path := L.CheckString(1)
	ud, err := newFile(L, nil, path, os.O_RDONLY, false, true)
	if err != nil {
		return 0
	}
	L.Push(L.NewClosure(ioLinesIter, L.Get(UpvalueIndex(1)), ud))
	return 1
}


func fileRead(L *LState) int {
	return fileReadAux(L, checkFile(L), 2)
}

var filebufOptions = []string{"no", "full"}

func ioInput(L *LState) int {
	if L.GetTop() == 0 {
		L.Push(fileDefIn(L))
		return 1
	}
	switch lv := L.Get(1).(type) {
	case LString:
		file, err := newFile(L, nil, string(lv), os.O_RDONLY,false, true)
		if err != nil {
			L.RaiseError(err.Error())
		}
		L.Get(UpvalueIndex(1)).(*LTable).RawSetInt(fileDefInIndex, file)
		L.Push(file)
		return 1
	case *LUserData:
		if _, ok := lv.Value.(*lFile); ok {
			L.Get(UpvalueIndex(1)).(*LTable).RawSetInt(fileDefInIndex, lv)
			L.Push(lv)
			return 1
		}
	}

	L.ArgError(1, "string or file expedted, but got "+L.Get(1).Type().String())
	return 0
}

func ioClose(L *LState) int {
	if L.GetTop() == 0 {
		return fileCloseAux(L, fileDefOut(L).Value.(*lFile))
	}
	return fileClose(L)
}

func ioLinesIter(L *LState) int {

	var file *lFile
	toclose := false

	if ud, ok := L.Get(1).(*LUserData); ok {
		file = ud.Value.(*lFile)
	} else {
		file = L.Get(UpvalueIndex(2)).(*LUserData).Value.(*lFile)
		toclose = true
	}

	rd, err := file.getReader(L)
	defer func() {
		if rd != nil {
			rd.(mfs.FileDescriptor).Close()
		}
	}()

	if err != nil {
		L.RaiseError(err.Error())
	}

	bufrd := bufio.NewReader(rd)
	buf, _, err := bufrd.ReadLine()

	if err != nil {

		if err == io.EOF {

			file.seek = file.Size()

			if toclose {
				fileCloseAux(L, file)
			}

			L.Push(LNil)
			return 1
		}

		L.RaiseError(err.Error())
	}

	file.seek += int64(len(buf) + 1)
	L.Push(LString(string(buf)))
	return 1
}

var ioOpenOpions = []string{"r", "rb", "w", "wb", "a", "ab", "r+", "rb+", "w+", "wb+", "a+", "ab+"}

func ioOpenFile(L *LState) int {

	path := L.CheckString(1)
	if L.GetTop() == 1 {
		L.Push(LString("r"))
	}

	mode := os.O_RDONLY
	writable := true
	readable := true

	switch ioOpenOpions[L.CheckOption(2, ioOpenOpions)] {
	case "r", "rb":
		mode = os.O_RDONLY
		writable = false
	case "w", "wb":
		mode = os.O_WRONLY | os.O_CREATE
		readable = false
	case "a", "ab":
		mode = os.O_WRONLY | os.O_APPEND | os.O_CREATE
	case "r+", "rb+":
		mode = os.O_RDWR
	case "w+", "wb+":
		mode = os.O_RDWR | os.O_TRUNC | os.O_CREATE
	case "a+", "ab+":
		mode = os.O_APPEND | os.O_RDWR | os.O_CREATE
	}

	file, err := newFile(L, nil, path, mode, writable, readable)
	if err != nil {
		L.Push(LNil)
		L.Push(LString(err.Error()))
		L.Push(LNumber(1)) // C-Lua compatibility: Original Lua pushes errno to the stack
		return 3
	}
	L.Push(file)
	return 1

}

func ioRead(L *LState) int {
	return fileReadAux(L, fileDefIn(L).Value.(*lFile), 1)
}

func ioType(L *LState) int {
	ud, udok := L.Get(1).(*LUserData)
	if !udok {
		L.Push(LNil)
		return 1
	}
	file, ok := ud.Value.(*lFile)
	if !ok {
		L.Push(LNil)
		return 1
	}
	if file.closed {
		L.Push(LString("closed file"))
		return 1
	}
	L.Push(LString("file"))
	return 1
}

func ioOutput(L *LState) int {
	if L.GetTop() == 0 {
		L.Push(fileDefOut(L))
		return 1
	}
	switch lv := L.Get(1).(type) {
	case LString:
		file, err := newFile(L, nil, string(lv), os.O_WRONLY|os.O_CREATE,true, false)
		if err != nil {
			L.RaiseError(err.Error())
		}
		L.Get(UpvalueIndex(1)).(*LTable).RawSetInt(fileDefOutIndex, file)
		L.Push(file)
		return 1
	case *LUserData:
		if _, ok := lv.Value.(*lFile); ok {
			L.Get(UpvalueIndex(1)).(*LTable).RawSetInt(fileDefOutIndex, lv)
			L.Push(lv)
			return 1
		}

	}
	L.ArgError(1, "string or file expedted, but got "+L.Get(1).Type().String())
	return 0
}

func ioWrite(L *LState) int {
	return fileWriteAux(L, fileDefOut(L).Value.(*lFile), 1)
}


//目录到读取必须从根目录开始不支持相对路径
func checkBaseRootPath( path string ) error {

	if strings.HasPrefix(path, "/") {
		return nil
	} else {
		return errors.New("paths must start with a leading slash")
	}

}

//虚拟目录下除了Data目录由AApp逻辑控制读写外，其他目录均为只读路径
func isReadOnlyPath( path string ) bool {
	return !strings.HasPrefix(path, "/Data")
}

//
