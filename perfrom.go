package lua

import (
	"bytes"
)

func ( l *LState ) PerfromGlobal ( global string, arg ...string ) ( []byte, error ) {

	lfn := l.GetGlobal(global)

	switch lfn.Type(){
	case LTFunction:
		break

	default:
		return Encode(lfn)
	}

	var params []LValue

	for _, av := range arg {
		params = append( params,  l.DecodeValue(av) )
	}


	if err := l.CallByParam( P {
		Fn: lfn,
		NRet: MultRet,
		Protect: true,
	}, params...); err != nil {
		return nil, err
	}

	var bsarr [][]byte

	for l.GetTop() > 0 {

		ret := l.Get(-1)

		if bs, err := Encode(ret); err != nil {
			return nil, err
		} else {
			bsarr = append(bsarr, bs)
		}

		l.Pop(1)

	}

	return bytes.Join(bsarr, []byte(",")), nil
}