package main
import "sync"
import "log"

type Group struct {
	gid int64
	mutex sync.Mutex
	members map[int64]struct{}
}

func NewGroup(gid int64) *Group {
    group := new(Group)
    group.gid = gid
    group.members = make(map[int64]struct{})
    return group
}

func (group *Group) Members() chan int64{
    c := make(chan int64)
    go func() {
        group.mutex.Lock()
        defer group.mutex.Unlock()
        for gid, _ := range group.members {
            c <- gid
        }
        close(c)
    }()
    return c
}

func (group *Group) AddMember(uid int64) {
    group.mutex.Lock()
    defer group.mutex.Unlock()
    if _, ok := group.members[uid]; ok {
        log.Printf("group member:%d exists\n", uid)
    } else {
        group.members[uid] = struct{}{}
    }
}

func (group *Group) RemoveMember(uid int64) {
    group.mutex.Lock()
    defer group.mutex.Unlock()
    if _, ok := group.members[uid]; ok {
        delete(group.members, uid)
    } else {
        log.Println("group no member:", uid)
    }
}

func CreateGroup(master int64, name string) int64 {
    return 10
}

func DeleteGroup(group_id int64) bool {
    return true
}

func AddGroupMember(group_id int64, uid int64) bool {
    return true
}

func RemoveGroupMember(group_id int64, uid int64) bool {
    return true
}

func LoadAllGroup() []*Group {
    return nil
}

func LoadGroup(group_id int64) *Group {
    return nil
}
