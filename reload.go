package main

import (
    "fmt"
    "log"
    "net"
    "os"
    "os/exec"
    "os/signal"
    "syscall"
    "sync"
    "net/http"
    "strconv"
    "reflect"
    "strings"
)

const (
    Graceful = "graceful"
)

// Test whether an error is equivalent to net.errClosing as returned by
// Accept during a graceful exit.
func IsErrClosing(err error) bool {
    if opErr, ok := err.(*net.OpError); ok {
        err = opErr.Err
    }
    return "use of closed network connection" == err.Error()
}

// Allows for us to notice when the connection is closed.
type Conn struct {
    net.Conn
    wg      *sync.WaitGroup
    isClose bool
    lock    sync.Mutex
}

func (c *Conn) Close() error {
    log.Printf("close %s", c.RemoteAddr())
    c.lock.Lock()
    defer c.lock.Unlock()
    err := c.Conn.Close()
    if !c.isClose && err == nil {
        c.wg.Done()
        c.isClose = true
    }
    return err
}

type stoppableListener struct {
    net.Listener
    wg      sync.WaitGroup
}

// restart cmd
var cmd *exec.Cmd

// listener lock
var lock sync.Mutex

// listener wait group
var listenerWaitGroup sync.WaitGroup

// listener object
var listeners map[uintptr]net.Listener

func init() {
    listeners = make(map[uintptr]net.Listener)
    path, err := exec.LookPath(os.Args[0])
    if nil != err {
        log.Fatalf("gracefulRestart: Failed to launch, error: %v", err)
    }
    cmd = exec.Command(path, os.Args[1:]...)
    cmd.Stdin = os.Stdin
    cmd.Stdout = os.Stdout
    cmd.Stderr = os.Stderr
}

func newStoppable(l net.Listener) (sl *stoppableListener) {
    lock.Lock()
    defer lock.Unlock()

    sl = &stoppableListener{Listener: l}
    v := reflect.ValueOf(l).Elem().FieldByName("fd").Elem()
    fd := uintptr(v.FieldByName("sysfd").Int())
    listeners[fd] = l
    return
}

func (sl *stoppableListener) Accept() (c net.Conn, err error) {
    c, err = sl.Listener.Accept()
    if err != nil {
        return
    }
    sl.wg.Add(1)
    // Wrap the returned connection, so that we can observe when
    // it is closed.
    c = &Conn{Conn: c, wg: &sl.wg}
    return
}

func (sl *stoppableListener) Close() error {
    log.Printf("close listener: %s", sl.Addr())
    return sl.Listener.Close()
}

// wait signal and restart service, then close listener, finally wait
func Wait() {
    waitSignal()
    lock.Lock()
    for _, listener := range (listeners) {
        listener.Close()
    }
    lock.Unlock()
    listenerWaitGroup.Wait()
    log.Println("close main process")
}

func waitSignal() error {
    ch := make(chan os.Signal, 1)
    signal.Notify(ch, syscall.SIGTERM, syscall.SIGHUP)
    for {
        sig := <-ch
        log.Println(sig.String())
        switch sig {

            case syscall.SIGTERM:
            return nil
            case syscall.SIGHUP:
            restart(sig)
            return nil
        }
    }
    return nil // It'll never get here.
}

func restart(s os.Signal) {
    lock.Lock()
    defer lock.Unlock()
    os.Setenv(Graceful, fmt.Sprintf("%d", s))
    i := 3
    for fd, listener := range (listeners) {
        // get listener fd
        os.Setenv(listener.Addr().String(), fmt.Sprintf("%d", i))
        // entry i becomes file descriptor 3+i
        cmd.ExtraFiles = append(cmd.ExtraFiles, os.NewFile(
        fd,
        listener.Addr().String(),
        ))
        i++
    }

    err := cmd.Start()
    if err != nil {
        log.Fatalf("gracefulRestart: Failed to launch, error: %v", err)
    }
}

func getFormattedAddr(addr string) string {
    // If host has colons or a percent sign, have to bracket it.
    result := strings.IndexByte(addr, ':')
    if result == 0 {
        return "0.0.0.0" + addr
    }
    return addr
}

func getInitListener(laddr string) (net.Listener, error) {
    var l net.Listener
    var err error
    listenerWaitGroup.Add(1)
    // format addr
    laddr = getFormattedAddr(laddr)

    graceful := os.Getenv(Graceful)
    if graceful != "" {
        // get current file descriptor
        currFdStr := os.Getenv(laddr)
        currFd, err := strconv.Atoi(currFdStr)
        if err != nil {
            log.Printf("%s get fd fail: %v", laddr, err)
        }
        log.Printf("main: %s Listening to existing file descriptor %v.", laddr, currFd)
        f := os.NewFile(uintptr(currFd), "")
        // file listener dup fd
        l, err = net.FileListener(f)
        // close current file descriptor
        f.Close()
    } else {
        log.Printf("listen to %s.", laddr)
        l, err = net.Listen("tcp4", laddr)
    }
    return l, err
}

// socket service
func Serve(laddr string, handler func(net.Conn)) {
    l, err := getInitListener(laddr)
    if err != nil {
        log.Fatalf("start fail: %v", err)
    }
    theStoppable := newStoppable(l)
    serve(theStoppable, handler)
    log.Printf("%s wait all connection close...", laddr)
    theStoppable.wg.Wait()
    listenerWaitGroup.Done()
    log.Printf("close socket %s", laddr)
}

func serve(l net.Listener, handle func(net.Conn)) {
    defer l.Close()
    for {
        c, err := l.Accept()
        if nil != err {
            if IsErrClosing(err) {
                log.Println("error closing")
                return
            }
            log.Fatalln(err)
        }
        log.Println("handle client", c.RemoteAddr())
        handle(c)
    }
}

// HTTP service
func ListenAndServe(laddr string, handler http.Handler) {
    var err error
    var l net.Listener
    l, err = getInitListener(laddr)
    if err != nil {
        log.Fatalf("start fail: %v", err)
    }
    theStoppable := newStoppable(l)
    log.Printf("Serving on http://%s/", laddr)
    server := &http.Server{Handler: handler}
    err = server.Serve(theStoppable)
    if err != nil {
        log.Println("ListenAndServe: ", err)
    }
    log.Printf("%s wait all connection close...", laddr)
    theStoppable.wg.Wait()
    listenerWaitGroup.Done()
    log.Printf("close http %s", laddr)
}

// socket service
func SocketService(laddr string, handler func(net.Conn)) {
    go func() {
        Serve(laddr, handler)
    }()
}

// HTTP service
func HTTPService(laddr string, handler http.Handler) {
    go func() {
        ListenAndServe(laddr, handler)
    }()
}

// single HTTP service
func SingleHTTPService(laddr string, handler http.Handler) {
    HTTPService(laddr, handler)
    Wait()
}

// single socket service
func SingleSocketService(laddr string, handler func(net.Conn)) {
    SocketService(laddr, handler)
    Wait()
}