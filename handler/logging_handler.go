package handler

import (
	"net/http"

	log "github.com/sirupsen/logrus"
)

type LoggingHandler struct {
	Handler http.Handler
}

func (h LoggingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Infof("http request:%s %s %s", r.RemoteAddr, r.Method, r.URL)
	h.Handler.ServeHTTP(w, r)
}
