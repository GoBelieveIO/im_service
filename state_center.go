package main
import "sync"
import "log"

type StateCenter struct {
    mutex sync.Mutex
    subscribers map[int64]IntSet
}

func NewStateCenter() *StateCenter {
    center := new(StateCenter)
    center.subscribers = make(map[int64]IntSet)
    return center
}

func (center *StateCenter) Subscribe(uid int64, targets IntSet) {
    center.mutex.Lock()
    defer center.mutex.Unlock()
    log.Println("targets:", targets)
    for target, _ := range targets {
        log.Println("target:", target)
        if s, ok := center.subscribers[target]; ok {
            s.Add(uid)
        } else {
            s = NewIntSet()
            s.Add(uid)
            center.subscribers[target] = s
        }
    }
}

func (center *StateCenter) Unsubscribe(uid int64, targets IntSet) {
    center.mutex.Lock()
    defer center.mutex.Unlock()

    for target, _ := range targets {
        if s, ok := center.subscribers[target]; ok {
            s.Remove(uid)
        }
    }
}

func (center *StateCenter) FindSubsriber(uid int64) []int64 {
    if _, ok := center.subscribers[uid]; !ok {
        return nil
    }
    set := center.subscribers[uid]
    s := make([]int64, len(set))
    i := 0
    for k, _ := range set {
        s[i] = k
        i++
    }
    return s
}


//func main() {
//    center := NewStateCenter()
//    set := NewIntSet()
//    set.Add(1)
//    log.Println(set.IsMember(1))
//    set.Remove(1)
//    log.Println(set.IsMember(1))
//    set.Add(13635273143)
//    center.Subscribe(13635273142, set)
//    subs := center.FindSubsriber(13635273143)
//    for _, sub := range subs {
//        log.Println("sub:", sub)
//    }
//    log.Println("sss:", center.subscribers)
//}
