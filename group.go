package main
import "sync"
import "log"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"

type Group struct {
	gid int64
	mutex sync.Mutex
	members IntSet
}

func NewGroup(gid int64, members []int64) *Group {
    group := new(Group)
    group.gid = gid
    group.members = NewIntSet()
    for _, m := range members {
        group.members.Add(m)
    }
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
    group.members.Add(uid)
}

func (group *Group) RemoveMember(uid int64) {
    group.mutex.Lock()
    defer group.mutex.Unlock()
    group.members.Remove(uid)
}

func CreateGroup(db *sql.DB, master int64, name string) int64 {
    stmtIns, err := db.Prepare("INSERT INTO im_group(master, name) VALUES( ?, ? )")
    if err != nil {
        log.Println("error:", err)
        return 0
    }
    defer stmtIns.Close() 
    result, err := stmtIns.Exec(master, name)
    if err != nil {
        log.Println("error:", err)
        return 0
    }
    gid, err := result.LastInsertId()
    if err != nil {
        log.Println("error:", err)
        return 0
    }
    return gid
}

func DeleteGroup(db *sql.DB, group_id int64) bool {
    var stmt1, stmt2 *sql.Stmt

    tx, err := db.Begin()
    if err != nil {
        log.Println("error:", err)
        return false
    }

    stmt1, err = tx.Prepare("DELETE FROM im_group WHERE id=?")
    if err != nil {
        log.Println("error:", err)
        goto ROLLBACK
    }
    defer stmt1.Close() 
    _, err = stmt1.Exec(group_id)
    if err != nil {
        log.Println("error:", err)
        goto ROLLBACK
    }    

    stmt2, err = tx.Prepare("DELETE FROM group_member WHERE group_id=?")
    if err != nil {
        log.Println("error:", err)
        goto ROLLBACK
    }
    defer stmt2.Close() 
    _, err = stmt2.Exec(group_id)
    if err != nil {
        log.Println("error:", err)
        goto ROLLBACK
    }    

    tx.Commit()
    return true

ROLLBACK:
    tx.Rollback()
    return false
}

func AddGroupMember(db *sql.DB, group_id int64, uid int64) bool {
    stmtIns, err := db.Prepare("INSERT INTO group_member(group_id, uid) VALUES( ?, ? )")
    if err != nil {
        log.Println("error:", err)
        return false
    }
    defer stmtIns.Close() 
    _, err = stmtIns.Exec(group_id, uid)
    if err != nil {
        log.Println("error:", err)
        return false
    }
    return true
}

func RemoveGroupMember(db *sql.DB, group_id int64, uid int64) bool {
    stmtIns, err := db.Prepare("DELETE FROM group_member WHERE group_id=? AND uid=?")
    if err != nil {
        log.Println("error:", err)
        return false
    }
    defer stmtIns.Close() 
    _, err = stmtIns.Exec(group_id, uid)
    if err != nil {
        log.Println("error:", err)
        return false
    }
    return true
}

func LoadAllGroup(db *sql.DB) (map[int64]*Group, error) {
    stmtIns, err := db.Prepare("SELECT id FROM im_group")
    if err != nil {
        log.Println("error:", err)
        return nil, nil
    }

    defer stmtIns.Close() 
    groups := make(map[int64]*Group)
    rows, err := stmtIns.Query()
    for rows.Next() {
        var id int64
        rows.Scan(&id)
        members, err := LoadGroupMember(db, id)
        if err != nil {
            log.Println("error:", err)
            continue
        }
        group := NewGroup(id, members)
        groups[group.gid] = group
    }
    return groups, nil
}

func LoadGroupMember(db *sql.DB, group_id int64) ([]int64, error) {
    stmtIns, err := db.Prepare("SELECT uid FROM group_member WHERE group_id=?")
    if err != nil {
        log.Println("error:", err)
        return nil, err
    }

    defer stmtIns.Close() 
    members := make([]int64, 0, 4)
    rows, err := stmtIns.Query(group_id)
    for rows.Next() {
        var uid int64
        rows.Scan(&uid)
        members = append(members, uid)
    }
    return members, nil
}
