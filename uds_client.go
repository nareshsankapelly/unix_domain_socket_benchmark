package main

import (
    "log"
    "net"
    "time"
    "fmt"
    "sync"
)

func createByteArray(size int) []byte {
    var buffer = make([]byte, size)
    for i := range buffer {
        buffer[i] = 0
    }
    return buffer
}

func writeToUdsSocket(c net.Conn, wg *sync.WaitGroup, iterations int, dataSize int) {
    for i := 0; i < iterations; i++ {
        _, err := c.Write(createByteArray(dataSize))
        if err != nil {
            log.Fatal("write error:", err)
            break
        } 
   }
   wg.Done()
}


func main() {
    c, err := net.Dial("unix", "/tmp/test.sock")
    if err != nil {
        panic(err)
    }
    //defer c.Close()
    var wg sync.WaitGroup
    t0 := time.Now()
    iterations := 10
    for numOfThreads:= 1; numOfThreads <= 1024; numOfThreads = numOfThreads * 2 {
        for dataSize := 2; dataSize <= 65536; dataSize = dataSize * 2 {
            for i := 0; i < numOfThreads; i++ {
                wg.Add(1)
                go writeToUdsSocket(c, &wg, iterations, dataSize)	
            }
            wg.Wait()
            t1 := time.Now()
            numberOfWrites := (numOfThreads * iterations)
            fmt.Printf("Average latency with %d concurrency and %d bytes datasize: %d micro seconds.\n", numOfThreads, dataSize, t1.Sub(t0).Nanoseconds() / (int64(numberOfWrites) * 1000))
        }
    }
    c.Close()
}

