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

package set

type Set[T comparable] map[T]struct{}

func NewSet[T comparable]() Set[T] {
	return make(map[T]struct{})
}

func (set Set[T]) Add(v T) {
	if _, ok := set[v]; ok {
		return
	}
	set[v] = struct{}{}
}

func (set Set[T]) IsMember(v T) bool {
	if _, ok := set[v]; ok {
		return true
	}
	return false
}

func (set Set[T]) Remove(v T) {
	if _, ok := set[v]; !ok {
		return
	}
	delete(set, v)
}

func (set Set[T]) Clone() Set[T] {
	n := make(map[T]struct{})
	for k, v := range set {
		n[k] = v
	}
	return n
}

func (set Set[T]) Count() int {
	return len(set)
}

type IntSet = Set[int64]

func NewIntSet() IntSet {
	return NewSet[int64]()
}
