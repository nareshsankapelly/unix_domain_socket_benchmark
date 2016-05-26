package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

func createByteArray(size int) []byte {
	var buffer = make([]byte, size)
	for i := range buffer {
		buffer[i] = 0
	}
	return buffer
}

func writeToUdsSocket(c net.Conn, wg *sync.WaitGroup, numberOfMessages int, dataSize int, latencies []int64, index int) {
	for i := 0; i < numberOfMessages; i++ {
		t0 := time.Now()
		_, err := c.Write(createByteArray(dataSize))
		if err != nil {
			log.Fatal("write error:", err)
			break
		}
		t1 := time.Now()
		latencies[index*numberOfMessages+i] = t1.Sub(t0).Nanoseconds() / 1000
	}
	wg.Done()
}

func writeToUdsSocketWithNewConn(wg *sync.WaitGroup, numberOfMessages int, dataSize int) {
	retryCount := 5
	var err error
	var c net.Conn
	for i := 0; i < retryCount; i++ {
		c, err = net.Dial("unix", "/tmp/test.sock")

		if err == nil {
			break
		}
		time.Sleep(time.Duration(math.Exp2(float64(retryCount))) * time.Millisecond * 10)
	}

	if err != nil {
		panic(err)
	}

	for i := 0; i < numberOfMessages; i++ {
		_, err := c.Write(createByteArray(dataSize))
		if err != nil {
			log.Fatal("write error:", err)
			break
		}
	}
	wg.Done()

	c.Close()
}

func createConnectionPool(size int) []net.Conn {
	var connectionPool = make([]net.Conn, size)
	var err error
	for i := range connectionPool {
		retryCount := 5
		for j := 0; j < retryCount; j++ {
			connectionPool[i], err = net.Dial("unix", "/tmp/test.sock")

			if err == nil {
				break
			}
			time.Sleep(time.Duration(math.Exp2(float64(retryCount))) * time.Millisecond * 10)
		}
		if err != nil {
			panic(err)
		}
	}
	return connectionPool
}

func main() {
	args := os.Args[1:]
	if len(args) != 4 {
		fmt.Printf("Usage: ./uds_client <max concurrency> <max size in bytes> <Connection pool size>")
	}
	maxConcurrency, err := strconv.Atoi(args[0])
	maxSize, err := strconv.Atoi(args[1])
	connectionPoolSize, err := strconv.Atoi(args[2])

	file, err := os.Create("result.csv")
	if err != nil {
		panic(err)
	}

	defer file.Close()

	writer := csv.NewWriter(file)

	header := []string{"connections", "concurrency", "number of messages", "size in bytes", "Average Latency in us"}

	writer.Write(header)

	defer writer.Flush()

	connectionPool := createConnectionPool(connectionPoolSize)
	numberOfMessages := 256
	for numOfThreads := 16; numOfThreads <= maxConcurrency; numOfThreads = numOfThreads * 2 {
		for dataSize := 128; dataSize <= maxSize; dataSize = dataSize * 4 {
			var wg sync.WaitGroup
			t0 := time.Now()
			var latencies = make([]int64, numOfThreads*numberOfMessages)
			for i := 0; i < numOfThreads; i++ {
				wg.Add(1)
				go writeToUdsSocket(connectionPool[rand.Intn(len(connectionPool))], &wg, numberOfMessages, dataSize, latencies, i)
			}
			wg.Wait()
			t1 := time.Now()

			numberOfWrites := (numOfThreads * numberOfMessages)
			var timeTakenInMilliSecs int64 = t1.Sub(t0).Nanoseconds() / (1000 * 1000)
			var throughput int64 = (int64(numberOfWrites*1000) / timeTakenInMilliSecs)
			record := []string{strconv.Itoa(connectionPoolSize), strconv.Itoa(numOfThreads), strconv.Itoa(numberOfWrites), strconv.Itoa(dataSize), strconv.FormatFloat(float64(throughput), 'f', 2, 64)}
			writer.Write(record)
			fmt.Printf("conns:%d, Conc:%d, noOfMsgs:%d, size:%d bytes,  throughput:%s\n", connectionPoolSize, numOfThreads, numberOfWrites, dataSize, strconv.FormatFloat(float64(throughput), 'f', 2, 64))
		}
	}
}
