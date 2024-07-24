package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

var serverAddr string

func init() {
	flag.StringVar(&serverAddr, "addr", "http://localhost:8081", "Server address")
	flag.Parse()
}

func printPrompt() {
	fmt.Print("bitcask >>")
}
func main() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("Enter command (put/get/delete/list/exit):\n")
	for {
		printPrompt()
		scanner.Scan()
		command := scanner.Text()

		switch {
		case command == "exit":
			fmt.Println("Exiting...")
			return
		case strings.HasPrefix(command, "put"):
			handlePut(strings.TrimPrefix(command, "put"))
		case strings.HasPrefix(command, "get"):
			handleGet(strings.TrimPrefix(command, "get"))
		case strings.HasPrefix(command, "delete"):
			handleDelete(strings.TrimPrefix(command, "delete"))
		case command == "list":
			handleList()
		default:
			fmt.Println("Unknown command. Use put, get, delete, list, or exit.")
		}
	}
}

func handlePut(input string) {
	// 去除前后空白
	input = strings.TrimSpace(input)
	// 分割输入为 key 和 value
	parts := strings.SplitN(input, " ", 2)
	if len(parts) != 2 {
		fmt.Println("Usage: put <key> <value>")
		return
	}
	key, value := parts[0], parts[1]
	fmt.Printf("key: %s, value: %s\n", key, value)

	// 创建请求体
	sendBody := strings.NewReader(fmt.Sprintf(`{"%s": "%s"}`, key, value))

	// 发起 POST 请求
	resp, err := http.Post(fmt.Sprintf("%s/put", serverAddr), "application/json", sendBody)
	if err != nil {
		fmt.Printf("Error making PUT request: %v\n", err)
		return
	}
	defer resp.Body.Close()

	// 读取响应体
	recvBody, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error reading PUT response: %v\n", err)
		return
	}

	// 打印响应内容
	fmt.Println("Response:", string(recvBody))
}

func handleGet(key string) {
	key = strings.TrimSpace(key)
	resp, err := http.Get(fmt.Sprintf("%s/get?key=%s", serverAddr, key))
	if err != nil {
		fmt.Printf("Error making GET request: %v\n", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println("Response:", string(body))
}

func handleDelete(key string) {
	key = strings.TrimSpace(key)
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/del?key=%s", serverAddr, key), nil)
	if err != nil {
		fmt.Printf("Error creating DELETE request: %v\n", err)
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("Error making DELETE request: %v\n", err)
		return
	}
	defer resp.Body.Close()
	fmt.Println("Response Status:", resp.Status)
}

func handleList() {

	resp, err := http.Get(fmt.Sprintf("%s/list", serverAddr))
	if err != nil {
		fmt.Printf("Error making LIST request: %v\n", err)
		return
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println("Response:", string(body))
}
