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

type IntSet map[int64]struct{}

func NewIntSet() IntSet {
	return make(map[int64]struct{})
}

func (set IntSet) Add(v int64) {
	if _, ok := set[v]; ok {
		return
	}
	set[v] = struct{}{}
}

func (set IntSet) IsMember(v int64) bool {
	if _, ok := set[v]; ok {
		return true
	}
	return false
}

func (set IntSet) Remove(v int64) {
	if _, ok := set[v]; !ok {
		return
	}
	delete(set, v)
}

func (set IntSet) Clone() IntSet {
	n := make(map[int64]struct{})
	for k, v := range set {
		n[k] = v
	}
	return n
}
