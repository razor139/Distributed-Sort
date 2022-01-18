package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

type Client struct {
	clientConn net.Conn
	record     []byte
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func read(file string) [][]byte {
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	buf := make([]byte, 100)

	var records [][]byte

	for {
		_, err := f.Read(buf)
		var buf1 []byte
		buf1 = append(buf1, buf...)
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}

		records = append(records, buf1)
	}
	return records
}
func write(sorted_records [][]byte, file string) {
	f, err := os.Create(file)
	defer f.Close()

	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(sorted_records); i++ {
		_, err1 := f.Write(sorted_records[i])
		if err1 != nil {
			log.Fatal(err1)
		}
	}
}

type example_records [][]byte

func (r example_records) Len() int {
	return len(r)
}

func (r example_records) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r example_records) Less(i, j int) bool {

	if bytes.Compare(r[i][0:10], r[j][0:10]) < 0 {
		return true
	}
	return false
}

func sendClientData(dataSend [][]byte, Host string, Port string) {
	var clientConn net.Conn
	var err error
	//log.Println("Amount of records to be sent to:", Host, "is:", len(dataSend))
	for {
		clientConn, err = net.Dial("tcp", Host+":"+Port)
		if err != nil {
			time.Sleep(time.Millisecond * 8)
			//log.Println(err)
		} else {
			log.Println("Connected with Host:", Host)
			break
		}

	}
	defer clientConn.Close()

	for i := 0; i < len(dataSend); i++ {
		_, err := clientConn.Write(dataSend[i])
		if err != nil {
			log.Println("Send Failed")
		}
	}
	//log.Printf("All data sent\n")
	//clientConn.Write([]byte("Done"))
}

func handleConn(conn net.Conn, ch chan<- Client) {
	buff := make([]byte, 100)
	var res []byte
	defer conn.Close()
	for {
		bytes, err := conn.Read(buff)
		if err != nil {
			if err != io.EOF {
				log.Printf("Receive failed: %s\n", err)
				continue
			} else {
				//log.Println("Data tx terminated by node")
				break
			}
		}
		// if string(buff[0:bytes]) == "Done" {
		// 	break
		// }
		res = append(res, buff[0:bytes]...)
	}
	//log.Printf("Received total data size: %d\n", len(res))
	newRecord := Client{conn, res}
	ch <- newRecord
}

func acceptConn(ln net.Listener, ch chan<- Client, serverNum int) {
	log.Printf("Accepting connections now\n")
	var count int = 0
	defer func() {
		log.Println("Closing listener")
		ln.Close()
	}()
	for {
		if count == serverNum-1 {
			break
		}
		conn, err1 := ln.Accept()
		if err1 != nil {
			//log.Println("Connection failed")
			continue
		}
		count++
		go handleConn(conn, ch)

	}
	//log.Printf("Ending accept conn\n")
}

func consolData(ch <-chan Client, serverNum int) [][]byte {
	var recData [][]byte
	numOfClientsComp := 0
	for {
		if numOfClientsComp == serverNum-1 {
			break
		}
		recEntry := <-ch
		numOfClientsComp++
		for i := 0; i < len(recEntry.record)/100; i++ {
			recData = append(recData, recEntry.record[i*100:(i+1)*100])
		}
	}
	return recData
}

func createServer(servers ServerConfigs, serverId int, ch chan<- Client, dataSend map[int][][]byte) {
	ln, err := net.Listen("tcp", servers.Servers[serverId].Host+":"+servers.Servers[serverId].Port)
	log.Printf("Server created at ID: %d with address: %s\n", serverId, ln.Addr().String())
	if err != nil {
		log.Fatalf(err.Error())
	}

	//defer ln.Close()
	time.Sleep(2 * time.Second)
	for i := 0; i < len(servers.Servers); i++ {

		if i != serverId {
			//log.Printf("Creating client connections with : %d\n", i)
			go sendClientData(dataSend[i], servers.Servers[i].Host, servers.Servers[i].Port)
		}
	}

	go acceptConn(ln, ch, len(servers.Servers))
	//log.Println("Closing all connections at Server:", serverId)
}

func partitionData(records [][]byte, numServers int) map[int][][]byte {
	dataSend := make(map[int][][]byte)
	var serverTosend int
	for i := 0; i < len(records); i++ {
		var record []byte
		record = append(record, records[i]...)
		serverTosend = int(records[i][0] >> (8 - numServers))
		dataSend[serverTosend] = append(dataSend[serverTosend], record)
	}
	return dataSend
}

func main() {
	var all_records, recData [][]byte
	var partData map[int][][]byte
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	/*
		Implement Distributed Sort
	*/
	ch := make(chan Client, 100)
	all_records = read(os.Args[2])
	fmt.Println("Amount of data in server:", serverId, "is :", len(all_records))
	partData = partitionData(all_records, int(math.Log2(float64(len(scs.Servers)))))
	fmt.Println("Data has been partitioned")

	go createServer(scs, serverId, ch, partData)
	recData = consolData(ch, len(scs.Servers))
	fmt.Println("Waiting for communication to complete")
	for i := 0; i < len(partData[serverId]); i++ {
		recData = append(recData, partData[serverId][i])
	}
	fmt.Println("starting the sort")
	sort.Sort(example_records(recData))
	fmt.Println("Writing sorted data")
	write(recData, os.Args[3])

}
