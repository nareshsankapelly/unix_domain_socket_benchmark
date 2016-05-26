package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"sort"
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

func writeToUdsSocket(c net.Conn, wg *sync.WaitGroup, numberOfMessages int, dataSize int, latencies []int, index int) {
	for i := 0; i < numberOfMessages; i++ {
		t0 := time.Now()
		_, err := c.Write(createByteArray(dataSize))
		if err != nil {
			log.Fatal("write error:", err)
			break
		}
		t1 := time.Now()
		latencies[index*numberOfMessages+i] = int(t1.Sub(t0).Nanoseconds() / 1000)
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
	if len(args) != 3 {
		fmt.Printf("Usage: ./uds_client <max concurrency (greater than 8)> <max size in bytes (greater than 64)> <Max Connection pool size (greater than 8)>")
	}
	maxConcurrency, err := strconv.Atoi(args[0])
	maxSize, err := strconv.Atoi(args[1])
	maxConnectionPoolSize, err := strconv.Atoi(args[2])

	file, err := os.Create("result.csv")
	if err != nil {
		panic(err)
	}

	defer file.Close()

	writer := csv.NewWriter(file)

	header := []string{"connections", "concurrency", "size in bytes", "Average Latency in us", "99thPercentileLatency in us", "95thPercentileLatency in us", "throughput"}

	writer.Write(header)

	defer writer.Flush()

	numberOfMessages := 10000
	for connectionPoolSize := 8; connectionPoolSize <= maxConnectionPoolSize; connectionPoolSize = connectionPoolSize * 2 {
		connectionPool := createConnectionPool(connectionPoolSize)
		for numOfThreads := 8; numOfThreads <= maxConcurrency; numOfThreads = numOfThreads * 2 {
			for dataSize := 64; dataSize <= maxSize; dataSize = dataSize * 4 {
				var wg sync.WaitGroup
				t0 := time.Now()
				var latencies = make([]int, numOfThreads*numberOfMessages)
				for i := 0; i < numOfThreads; i++ {
					wg.Add(1)
					go writeToUdsSocket(connectionPool[rand.Intn(len(connectionPool))], &wg, numberOfMessages, dataSize, latencies, i)
				}
				wg.Wait()
				t1 := time.Now()
				var sumOfLatencies int = 0
				var maxLatency int = latencies[0]
				var minLatency int = latencies[0]
				for _, value := range latencies {
					if value > maxLatency {
						maxLatency = value
					}

					if value < minLatency {
						minLatency = value
					}

					sumOfLatencies = sumOfLatencies + value
				}
				sort.Ints(latencies)
				numberOfWrites := (numOfThreads * numberOfMessages)
				averageLatency := float64(sumOfLatencies / numberOfWrites)
				var timeTakenInMilliSecs int64 = t1.Sub(t0).Nanoseconds() / (1000 * 1000)
				var throughput int64 = (int64(numberOfWrites*1000) / timeTakenInMilliSecs)
				var nintyNinthPercentileIndex int = int((99 * numberOfWrites) / 100)
				var nintyFifthPercentileIndex int = int((95 * numberOfWrites) / 100)

				var nintyNinthPercentileLatency int = latencies[nintyNinthPercentileIndex]
				var nintyFifthPercentileLatency int = latencies[nintyFifthPercentileIndex]
				record := []string{strconv.Itoa(connectionPoolSize),
					strconv.Itoa(numOfThreads),
					strconv.Itoa(dataSize),
					strconv.FormatFloat(float64(averageLatency), 'f', 2, 64),
					strconv.FormatFloat(float64(nintyNinthPercentileLatency), 'f', 2, 64),
					strconv.FormatFloat(float64(nintyFifthPercentileLatency), 'f', 2, 64),
					strconv.FormatFloat(float64(throughput), 'f', 2, 64)}

				writer.Write(record)

				fmt.Printf("conns:%d, Conc:%d, size:%d bytes,  throughput:%s, averageLatency:%s us, 99thPercentileLatency:%s us, 95thPercentileLatency: %s us\n",
					connectionPoolSize, numOfThreads, dataSize, strconv.FormatFloat(float64(throughput), 'f', 2, 64),
					strconv.FormatFloat(float64(averageLatency), 'f', 2, 64),
					strconv.FormatFloat(float64(nintyNinthPercentileLatency), 'f', 2, 64),
					strconv.FormatFloat(float64(nintyFifthPercentileLatency), 'f', 2, 64))
			}
		}
	}
}
