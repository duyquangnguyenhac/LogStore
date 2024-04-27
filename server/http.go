package server

import (
	"encoding/json"
	"fmt"
	"log_struct"
	"net/http"

	"github.com/gorilla/mux"
)

type logStore struct {
	Log *log_struct.Log
}

func (server *logStore) handleHello(wrt http.ResponseWriter, req *http.Request) {
	fmt.Println("Hello")
	json.NewEncoder(wrt).Encode("Hello")
}

func (server *logStore) handlePost(wrt http.ResponseWriter, req *http.Request) {
	var request PostLogRequest
	err := json.NewDecoder(req.Body).Decode(&request)
	if err != nil {
		http.Error(wrt, err.Error(), http.StatusBadRequest)
		return
	}
	offset, append_err := server.Log.Append(request.Record)
	if append_err != nil {
		http.Error(wrt, append_err.Error(), http.StatusInternalServerError)
		return
	}
	res := PostLogResponse{
		Offset: offset,
	}
	encode_err := json.NewEncoder(wrt).Encode(res)
	if encode_err != nil {
		http.Error(wrt, encode_err.Error(), http.StatusInternalServerError)
		return
	}
}

func (server *logStore) handleGet(wrt http.ResponseWriter, req *http.Request) {
	var request GetLogRequest
	err := json.NewDecoder(req.Body).Decode(&request)
	if err != nil {
		http.Error(wrt, err.Error(), http.StatusBadRequest)
		return
	}
	record, read_err := server.Log.Read(request.Offset)
	if read_err != nil {
		http.Error(wrt, read_err.Error(), http.StatusInternalServerError)
		return
	}
	res := GetLogResponse{
		Record: record,
	}

	encode_err := json.NewEncoder(wrt).Encode(res)
	if encode_err != nil {
		http.Error(wrt, err.Error(), http.StatusInternalServerError)
		return
	}
}

type PostLogRequest struct {
	Record log_struct.Record `json:"record"`
}

type PostLogResponse struct {
	Offset uint64 `json:"offset"`
}

type GetLogRequest struct {
	Offset uint64 `json:"offset"`
}

type GetLogResponse struct {
	Record log_struct.Record `json:"record"`
}

func createLogStore() *logStore {
	return &logStore{
		Log: &log_struct.Log{},
	}
}

func HttpServer(addr string) *http.Server {
	var server = createLogStore()
	var router = mux.NewRouter()
	router.HandleFunc("/", server.handlePost).Methods("POST")
	router.HandleFunc("/", server.handleGet).Methods("GET")
	router.HandleFunc("/hello", server.handleHello).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: router,
	}
}
