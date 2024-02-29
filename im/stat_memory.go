/**
 * Copyright (c) 2014-2015, GoBelieve
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package main

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

func formatStdOut(stdout []byte, userfulIndex int) []string {
	eol := "\n"
	infoArr := strings.Split(string(stdout), eol)[userfulIndex]
	ret := strings.Fields(infoArr)
	return ret
}

func ReadRSSDarwin(pid int) int64 {
	args := "-o rss -p"
	stdout, _ := exec.Command("ps", args, strconv.Itoa(pid)).Output()
	ret := formatStdOut(stdout, 1)
	if len(ret) == 0 {
		log.Warning("can't find process")
		return 0
	}

	rss, _ := strconv.ParseInt(ret[0], 10, 64)
	return rss * 1024
}

func ReadRSSLinux(pid int, pagesize int) int64 {
	//http://man7.org/linux/man-pages/man5/proc.5.html
	procStatFileBytes, err := ioutil.ReadFile(path.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		log.Warning("read file err:", err)
		return 0
	}

	splitAfter := strings.SplitAfter(string(procStatFileBytes), ")")

	if len(splitAfter) == 0 || len(splitAfter) == 1 {
		log.Warning("Can't find process ")
		return 0
	}

	infos := strings.Split(splitAfter[1], " ")
	if len(infos) < 23 {
		//impossible
		return 0
	}

	rss, _ := strconv.ParseInt(infos[22], 10, 64)
	return rss * int64(pagesize)
}

func ReadRSS(platform string, pid int, pagesize int) int64 {
	if platform == "linux" {
		return ReadRSSLinux(pid, pagesize)
	} else if platform == "darwin" {
		return ReadRSSDarwin(pid)
	} else {
		return 0
	}
}

func MemStatService(low_memory *int32, config *Config) {
	platform := runtime.GOOS
	pagesize := os.Getpagesize()
	pid := os.Getpid()
	//3 min
	ticker := time.NewTicker(time.Second * 60 * 3)
	for range ticker.C {
		rss := ReadRSS(platform, pid, pagesize)
		if rss > config.memory_limit {
			atomic.StoreInt32(low_memory, 1)
		} else {
			atomic.StoreInt32(low_memory, 0)
		}
		log.Infof("process rss:%dk low memory:%d", rss/1024, *low_memory)
	}
}
