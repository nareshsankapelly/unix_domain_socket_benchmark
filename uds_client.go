package main

import (
    "log"
    "net"
    "time"
    "fmt"
    "sync"
    "math"
    "encoding/csv"
    "os"
    "strconv"
    "math/rand"
)

func createByteArray(size int) []byte {
    var buffer = make([]byte, size)
    for i := range buffer {
        buffer[i] = 0
    }
    return buffer
}

func writeToUdsSocket(c net.Conn, wg *sync.WaitGroup, numberOfMessages int, dataSize int) {
    for i := 0; i < numberOfMessages; i++ {
        _, err := c.Write(createByteArray(dataSize))
        if err != nil {
            log.Fatal("write error:", err)
            break
        } 
   }
   wg.Done()
}

func writeToNewUDSSocket(wg *sync.WaitGroup, numberOfMessages int, dataSize int) {
    retryCount := 5
    var err error
    var c net.Conn
    for i:=0; i < retryCount; i++ {
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

func createConnectionPool(size int) []net.Conn{
    var connectionPool = make([]net.Conn, size)
    var err error
    for i := range connectionPool {
        retryCount := 5
        for j:=0; j < retryCount; j++ {
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
        fmt.Printf("Usage: ./uds_client <max concurrency> <max number of messages> <max size in bytes>")
    }
    maxConcurrency, err := strconv.Atoi(args[0])
    maxNumberOfMessages, err  := strconv.Atoi(args[1])
    maxSize, err := strconv.Atoi(args[2])

    file, err := os.Create("result.csv")
    if err != nil {
        panic(err)
    }

    defer file.Close()

    writer := csv.NewWriter(file)

    header := [] string {"connections", "concurrency", "number of messages", "size in bytes", "Average Latency in us"}

    writer.Write(header)

    defer writer.Flush()

    c, err := net.Dial("unix", "/tmp/test.sock")
    if err != nil {
        panic(err)
    }
    //defer c.Close()

    for numberOfMessages:= 1; numberOfMessages <= maxNumberOfMessages; numberOfMessages = numberOfMessages * 2 {
        for numOfThreads:= 1; numOfThreads <= maxConcurrency; numOfThreads = numOfThreads * 2 {
            for dataSize := 2; dataSize <= maxSize; dataSize = dataSize * 2 {
                var wg sync.WaitGroup
                t0 := time.Now()
                for i := 0; i < numOfThreads; i++ {
                    wg.Add(1)
                    go writeToUdsSocket(c, &wg, numberOfMessages, dataSize)	
                }
                wg.Wait()
                t1 := time.Now()
                numberOfWrites := (numOfThreads * numberOfMessages)
                record := [] string {"Single", strconv.Itoa(numOfThreads), strconv.Itoa(numberOfMessages), strconv.Itoa(dataSize), strconv.FormatInt(t1.Sub(t0).Nanoseconds() / (int64(numberOfWrites) * 1000), 10)}
                writer.Write(record)
                fmt.Printf("SC - Conc:%d, noOfMsgs:%d, size:%d bytes,  AvgLat:%d us.\n", numOfThreads, numberOfMessages, dataSize, t1.Sub(t0).Nanoseconds() / (int64(numberOfWrites) * 1000))
            }
        }
    }
    c.Close()

    connectionPool := createConnectionPool(32)

    for numberOfMessages:= 1; numberOfMessages <= maxNumberOfMessages; numberOfMessages = numberOfMessages * 2 {
        for numOfThreads:= 1; numOfThreads <= maxConcurrency; numOfThreads = numOfThreads * 2 {
            for dataSize := 2; dataSize <= maxSize; dataSize = dataSize * 2 {
                var wg sync.WaitGroup
                t0 := time.Now()
                for i := 0; i < numOfThreads; i++ {
                    wg.Add(1)
                    go writeToUdsSocket(connectionPool[rand.Intn(len(connectionPool))], &wg, numberOfMessages, dataSize)
                }
                wg.Wait()
                t1 := time.Now()
                numberOfWrites := (numOfThreads * numberOfMessages)
                record := [] string {"multiple", strconv.Itoa(numOfThreads), strconv.Itoa(numberOfMessages), strconv.Itoa(dataSize), strconv.FormatInt(t1.Sub(t0).Nanoseconds() / (int64(numberOfWrites) * 1000), 10)}
                writer.Write(record)
                fmt.Printf("MC - Conc:%d, noOfMsgs:%d, size:%d bytes,  AvgLat:%d us.\n", numOfThreads, numberOfMessages, dataSize, t1.Sub(t0).Nanoseconds() / (int64(numberOfWrites) * 1000))
            }
        }
    }
}

