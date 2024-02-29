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

import "net/http"

type Handler[T1 any] struct {
	handler func(http.ResponseWriter, *http.Request, T1)
	arg1    T1
}

func (handler *Handler[T1]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler.handler(w, r, handler.arg1)
}

type Handler2[T1 any, T2 any] struct {
	handler func(http.ResponseWriter, *http.Request, T1, T2)
	arg1    T1
	arg2    T2
}

func (handler *Handler2[T1, T2]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler.handler(w, r, handler.arg1, handler.arg2)
}

type Handler3[T1 any, T2 any, T3 any] struct {
	handler func(http.ResponseWriter, *http.Request, T1, T2, T3)
	arg1    T1
	arg2    T2
	arg3    T3
}

func (handler *Handler3[T1, T2, T3]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler.handler(w, r, handler.arg1, handler.arg2, handler.arg3)
}

func handle_http[T any](pattern string, handler func(http.ResponseWriter, *http.Request, T), arg T) {
	http.Handle(pattern, &Handler[T]{handler, arg})
}

func handle_http2[T1 any, T2 any](pattern string, handler func(http.ResponseWriter, *http.Request, T1, T2), arg1 T1, arg2 T2) {
	http.Handle(pattern, &Handler2[T1, T2]{handler, arg1, arg2})
}

func handle_http3[T1 any, T2 any, T3 any](pattern string, handler func(http.ResponseWriter, *http.Request, T1, T2, T3), arg1 T1, arg2 T2, arg3 T3) {
	http.Handle(pattern, &Handler3[T1, T2, T3]{handler, arg1, arg2, arg3})
}
