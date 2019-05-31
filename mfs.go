package lua

import (
	"context"
	"errors"
	"fmt"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-mfs"
	ft "github.com/ipfs/go-unixfs"
	"io"
	"io/ioutil"
	"os"
	gopath "path"
	"strings"
)

func checkPath( path string ) error {
	if strings.HasPrefix(path, "/") {
		return nil
	} else {
		return errors.New("paths must start with a leading slash")
	}
}

/// Lookup
func ( l *LState ) MFS_Lookup ( path string ) (mfs.FSNode, error) {

	if err := checkPath(path); err != nil {
		return nil, err
	}

	return mfs.Lookup(l.mfsRoot, path)
}

func ( l *LState ) MFS_LookupFile ( path string ) (*mfs.File, error) {

	if err := checkPath(path); err != nil {
		return nil, err
	}

	if fsn, err := mfs.Lookup( l.mfsRoot, path); err != nil {

		return nil, fmt.Errorf("%v not search file or directory", path)

	} else {

		fi, ok := fsn.(*mfs.File)
		if !ok {
			return nil, fmt.Errorf("%v was not a file", path)
		}

		return fi, nil

	}

}

func ( l *LState ) MFS_LookupDir ( path string ) (*mfs.Directory, error) {

	if err := checkPath(path); err != nil {
		return nil, err
	}

	if fsn, err := mfs.Lookup( l.mfsRoot, path); err != nil {

		return nil, fmt.Errorf("%v not search file or directory", path)

	} else {

		fd, ok := fsn.(*mfs.Directory)
		if !ok {
			return nil, fmt.Errorf("%v was not a directory", path)
		}

		return fd, nil

	}
}

func ( l *LState ) MFS_ReadAll( file *mfs.File, offset int64 ) ([]byte, error) {

	rfd, err := file.Open( mfs.Flags{Read:true} )
	if err != nil {
		return nil, err
	}
	defer rfd.Close()


	if offset < 0 {
		return nil, fmt.Errorf("cannot specify negative offset")
	}

	filen, err := rfd.Size()
	if err != nil {
		return nil, err
	}

	if offset > filen {
		return nil, fmt.Errorf("offset was past end of file (%d > %d)", offset, filen)
	}

	_, err = rfd.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, err
	}

	if bs, err := ioutil.ReadAll(rfd); err != nil {
		return nil, err
	} else {
		return bs, nil
	}
}

func ( l *LState ) MFS_ReadOffset( file *mfs.File, offset int64, size int ) ([]byte, error) {

	rfd, err := file.Open( mfs.Flags{Read:true} )
	if err != nil {
		return nil, err
	}
	defer rfd.Close()


	if offset < 0 {
		return nil, fmt.Errorf("cannot specify negative offset")
	}

	filen, err := rfd.Size()
	if err != nil {
		return nil, err
	}

	if offset > filen {
		return nil, fmt.Errorf("offset was past end of file (%d > %d)", offset, filen)
	}

	if offset + int64(size) > filen {
		return nil, fmt.Errorf("offset + size was past end of file (%d > %d)", offset + int64(size), filen)
	}

	_, err = rfd.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, err
	}

	r := io.LimitReader( rfd, int64(size) )

	if bs, err := ioutil.ReadAll(r); err != nil {
		return nil, err
	} else {
		return bs, nil
	}

}


/// Dir
func ( l *LState ) MFS_DirLS( path string ) ([]mfs.NodeListing, error) {

	if err := checkPath(path); err != nil {
		return nil, err
	}

	dir, err := l.MFS_LookupDir(path)
	if err != nil {
		return nil, err
	}

	names, err := dir.ListNames( context.Background() )
	if err != nil {
		return nil, err
	}

	var output []mfs.NodeListing
	for _, v := range names {
		output = append(output, mfs.NodeListing{Name:v})
	}

	return output, nil
}

func ( l *LState ) MFS_Mkdir( path string, parent bool ) error {

	if err := checkPath(path); err != nil {
		return err
	}

	return mfs.Mkdir( l.mfsRoot, path, mfs.MkdirOpts {
		Mkparents:  parent,
		Flush:      false,
	})

}


/// Other
func ( l *LState ) MFS_Rm( path string, recursive, force bool) error {

	if err := checkPath(path); err != nil {
		return err
	}

	if path == "/" {
		return fmt.Errorf("cannot delete root")
	}

	// 'rm a/b/c/' will fail unless we trim the slash at the end
	if path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}

	dir, name := gopath.Split(path)
	parent, err := mfs.Lookup(l.mfsRoot, dir)
	if err != nil {
		return fmt.Errorf("parent lookup: %s", err)
	}

	pdir, ok := parent.(*mfs.Directory)
	if !ok {
		return fmt.Errorf("no such file or directory: %s", path)
	}

	// get child node by name, when the node is corrupted and nonexistent,
	// it will return specific error.
	child, err := pdir.Child(name)
	if err != nil {
		return err
	}

	switch child.(type) {
	case *mfs.Directory:
		if !recursive {
			return fmt.Errorf("%s is a directory, use -r to remove directories", path)
		}
	}

	err = pdir.Unlink(name)
	if err != nil {
		return err
	}

	return pdir.Flush()

}

func ( l *LState ) MFS_Cp( path string, nd ipld.Node ) error {

	if err := checkPath(path); err != nil {
		return err
	}

	path = strings.TrimRight(path, "/")

	return mfs.PutNode(l.mfsRoot, path, nd)
}

func ( l *LState ) MFS_MV( src, dist string) error {

	if err := checkPath(src); err != nil {
		return err
	}

	if err := checkPath(dist); err != nil {
		return err
	}

	return mfs.Mv(l.mfsRoot, src, dist)
}

func ( l *LState ) MFS_Stat( path string ) ( *statOutput, error ) {

	fsn, err := l.MFS_Lookup(path)
	if err != nil {
		return nil, err
	}

	nd, err := fsn.GetNode()
	if err != nil {
		return nil, err
	}

	return statNode(nd)
}

func ( l *LState ) MFS_Flush( path string ) (cid.Cid, error) {

	if err := checkPath(path); err != nil {
		return cid.Cid{}, err
	}

	//n, err := mfs.FlushPath(context.Background(), l.mfsRoot, path)

	ctx, cancel := context.WithCancel( context.Background() )
	defer cancel()

	n, err := mfs.FlushPath(ctx, l.mfsRoot, path)
	if err != nil {
		return cid.Cid{}, err
	} else {
		return n.Cid(), nil
	}

}


/// File
/*
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
*/

func ( l *LState ) MFS_OpenFile ( path string, flag int ) (*mfs.File, error) {

	create := flag & os.O_CREATE == os.O_CREATE

	fli, err := getFileHandle(l.mfsRoot, path, create, l.ProtoNode.CidBuilder())
	if err != nil {
		return nil, err
	}

	return fli, nil
}

func ( l *LState ) MFS_OpenWriter ( fli *mfs.File, flag int) (io.Writer, error) {

	fwt, err := fli.Open(mfs.Flags{Write: true, Sync: false})
	if err != nil {
		return nil, err
	}

	if flag & os.O_APPEND == os.O_APPEND {

		if flen, err := fwt.Size(); err != nil {

			return nil, err

		} else {

			if _, err := fwt.Seek(flen, 0); err != nil {
				return nil, err
			}

		}

	} else if flag & os.O_TRUNC == os.O_TRUNC {

		err = fwt.Truncate(0)
		if err != nil {
			return nil, err
		}

	}

	return fwt, nil
}

func ( l *LState ) MFS_OpenReader ( fli *mfs.File, flag int) (io.Reader, error) {

	frd, err := fli.Open(mfs.Flags{Read: true, Sync: false})
	if err != nil {
		return nil, err
	}

	return frd, nil
}

type statOutput struct {
	Hash           string
	Size           uint64
	CumulativeSize uint64
	Blocks         int
	Type           string
	WithLocality   bool   `json:",omitempty"`
	Local          bool   `json:",omitempty"`
	SizeLocal      uint64 `json:",omitempty"`
}

func statNode( nd ipld.Node ) ( *statOutput, error) {

	c := nd.Cid()

	cumulsize, err := nd.Size()
	if err != nil {
		return nil, err
	}

	switch n := nd.(type) {
	case *dag.ProtoNode:
		d, err := ft.FSNodeFromBytes(n.Data())
		if err != nil {
			return nil, err
		}

		var ndtype string
		switch d.Type() {
		case ft.TDirectory, ft.THAMTShard:
			ndtype = "directory"
		case ft.TFile, ft.TMetadata, ft.TRaw:
			ndtype = "file"
		default:
			return nil, fmt.Errorf("unrecognized node type: %s", d.Type())
		}

		return &statOutput{
			Hash:           c.String(),
			Blocks:         len(nd.Links()),
			Size:           d.FileSize(),
			CumulativeSize: cumulsize,
			Type:           ndtype,
		}, nil
	case *dag.RawNode:
		return &statOutput{
			Hash:           c.String(),
			Blocks:         0,
			Size:           cumulsize,
			CumulativeSize: cumulsize,
			Type:           "file",
		}, nil
	default:
		return nil, fmt.Errorf("not unixfs node (proto or raw)")
	}
}

func getFileHandle(r *mfs.Root, path string, create bool, builder cid.Builder) (*mfs.File, error) {
	target, err := mfs.Lookup(r, path)
	switch err {
	case nil:
		fi, ok := target.(*mfs.File)
		if !ok {
			return nil, fmt.Errorf("%s was not a file", path)
		}
		return fi, nil

	case os.ErrNotExist:
		if !create {
			return nil, err
		}

		// if create is specified and the file doesnt exist, we create the file
		dirname, fname := gopath.Split(path)
		pdiri, err := mfs.Lookup(r, dirname)
		if err != nil {
			return nil, err
		}
		pdir, ok := pdiri.(*mfs.Directory)
		if !ok {
			return nil, fmt.Errorf("%s was not a directory", dirname)
		}
		if builder == nil {
			builder = pdir.GetCidBuilder()
		}

		nd := dag.NodeWithData(ft.FilePBData(nil, 0))
		nd.SetCidBuilder(builder)
		err = pdir.AddChild(fname, nd)
		if err != nil {
			return nil, err
		}

		fsn, err := pdir.Child(fname)
		if err != nil {
			return nil, err
		}

		fi, ok := fsn.(*mfs.File)
		if !ok {
			return nil, errors.New("expected *mfs.File, didnt get it. This is likely a race condition")
		}
		return fi, nil

	default:
		return nil, err
	}
}