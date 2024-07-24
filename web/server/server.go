package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/xia-Sang/bitcask"
)

var engine *bitcask.Db

func init() {
	newOptions := bitcask.NewOptions("./server")
	engine = bitcask.NewDb(newOptions)
}
func main() {
	parseCommand()
}
func parseCommand() {
	address := flag.String("address", ":8081", "The address to listen on, e.g., ':8081'")

	flag.Parse()
	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/put", handlePut)
	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/del", handleDelete)
	http.HandleFunc("/list", handleListKv)

	fmt.Printf("Starting server at %s\n", *address)

	if err := http.ListenAndServe(*address, nil); err != nil {
		fmt.Printf("Error starting server: %s\n", err)
	}
}
func handleIndex(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, you've requested: %s\n", r.URL.Path)
}

// put请求处理
func handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var data map[string]string
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range data {
		err := engine.Put([]byte(key), []byte(value))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			log.Printf("failed to put kv in db: %v\n", err)
			return
		}
	}
	w.Write([]byte("OK!"))
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	val, ok := engine.Get([]byte(key))
	if !ok {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(string(val))
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	err := engine.Delete([]byte(key))
	if err != nil {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
}
func handleListKv(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	listKvs := "["
	engine.Fold(func(key, value []byte) bool {
		if listKvs != "[" {
			listKvs += ", "
		}
		listKvs += fmt.Sprintf("(%s:%s)", key, value)
		return true
	})
	listKvs += "]"
	_ = json.NewEncoder(w).Encode(listKvs)
}
