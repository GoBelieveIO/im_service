package main
import "bytes"
import log "github.com/golang/glog"
import "strconv"

type OfflineComparer struct{}

//appid, uid, msgid
func (oc OfflineComparer) Split(a []byte) ([]byte, []byte, []byte) {
	index1 := bytes.IndexByte(a, '_')
	if index1 == -1 || index1 + 1 >= len(a) {
		return nil, nil, nil
	}
	index2 := bytes.IndexByte(a[index1+1:], '_')
	if index2 == -1 || index2 + 1 >= len(a) {
		return nil, nil, nil
	}
	
	return a[:index1], a[index1+1:index1+1+index2], a[index1+1+index2+1:]
}

func (oc OfflineComparer) Compare(a, b []byte) int {
	p1, p2, p3 := oc.Split(a)
	p4, p5, p6 := oc.Split(b)

	if p1 == nil || p4 == nil {
		log.Infof("can't find seperate, a:%s b:%s compare bytes...\n", string(a), string(b))
		return bytes.Compare(a, b)
	}

	r1 := bytes.Compare(p1, p4)
	if r1 != 0 {
		return r1
	}

	r2 := bytes.Compare(p2, p5)
	if r2 != 0 {
		return r2
	}

	v1, err1 := strconv.ParseInt(string(p3), 10, 64)
	v2, err2 := strconv.ParseInt(string(p6), 10, 64)
	if err1 != nil || err2 != nil {
		log.Infof("parse int err, a:%s b:%s compare bytes...\n", string(a), string(b))
		return bytes.Compare(p3, p6)
	}

	if v1 < v2 {
		return -1
	} else if v1 == v2 {
		return 0
	} else {
		return 1
	}
}

func (OfflineComparer) Name() string {
	return "im.OfflineComparator"
}

func (OfflineComparer) Separator(dst, a, b []byte) []byte {
	return nil
}

func (OfflineComparer) Successor(dst, b []byte) []byte {

	return nil
}

type GroupOfflineComparer struct{}

//appid, gid, uid, msgid
func (oc GroupOfflineComparer) Split(a []byte) ([]byte, []byte, []byte, []byte) {
	index1 := bytes.IndexByte(a, '_')
	if index1 == -1 || index1 + 1 >= len(a) {
		return nil, nil, nil, nil
	}
	index2 := bytes.IndexByte(a[index1+1:], '_')
	if index2 == -1 || index2 + 1 >= len(a) {
		return nil, nil, nil, nil
	}
	index2 += index1 + 1
	index3 := bytes.IndexByte(a[index2+1:], '_')
	if index3 == -1 || index3 + 1 >= len(a) {
		return nil, nil, nil, nil
	}
	index3 += index2 + 1

	return a[:index1], a[index1+1:index2], a[index2+1:index3], a[index3+1:]
}

func (oc GroupOfflineComparer) Compare(a, b []byte) int {
	p1, p2, p3, p4 := oc.Split(a)
	p5, p6, p7, p8 := oc.Split(b)

	if p1 == nil || p5 == nil {
		log.Infof("can't find seperate, a:%s b:%s compare bytes...\n", string(a), string(b))
		return bytes.Compare(a, b)
	}

	r1 := bytes.Compare(p1, p5)
	if r1 != 0 {
		return r1
	}

	r2 := bytes.Compare(p2, p6)
	if r2 != 0 {
		return r2
	}

	r3 := bytes.Compare(p3, p7)
	if r3 != 0 {
		return r3
	}

	v1, err1 := strconv.ParseInt(string(p4), 10, 64)
	v2, err2 := strconv.ParseInt(string(p8), 10, 64)
	if err1 != nil || err2 != nil {
		log.Infof("parse int err, a:%s b:%s compare bytes...\n", string(a), string(b))
		return bytes.Compare(p3, p6)
	}

	if v1 < v2 {
		return -1
	} else if v1 == v2 {
		return 0
	} else {
		return 1
	}
}

func (GroupOfflineComparer) Name() string {
	return "im.GroupOfflineComparator"
}

func (GroupOfflineComparer) Separator(dst, a, b []byte) []byte {
	return nil
}

func (GroupOfflineComparer) Successor(dst, b []byte) []byte {

	return nil
}
