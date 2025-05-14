package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

func ExecutePipeline(jobs ...job) {
	wg := &sync.WaitGroup{}
	in := make(chan interface{})
	close(in)

	for i := 0; i < len(jobs); i++ {
		wg.Add(1)
		out := make(chan interface{})
		go worker(jobs[i], in, out, wg)
		in = out
	}

	wg.Wait()
}

func worker(j job, in, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(out)
	j(in, out)
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	md5Mutex := &sync.Mutex{}
	
	for data := range in {
		wg.Add(1)
		go func(data interface{}) {
			defer wg.Done()
			dataStr := strconv.Itoa(data.(int))
			
			crc32Chan := make(chan string)
			md5Crc32Chan := make(chan string)

			go func() {
				crc32Chan <- DataSignerCrc32(dataStr)
			}()
			
			go func() {
				md5Mutex.Lock()
				md5Hash := DataSignerMd5(dataStr)
				md5Mutex.Unlock()
				md5Crc32Chan <- DataSignerCrc32(md5Hash)
			}()

			crc32Result := <-crc32Chan
			md5Crc32Result := <-md5Crc32Chan
			result := crc32Result + "~" + md5Crc32Result
			out <- result
		}(data)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for data := range in {
		wg.Add(1)
		go func(data interface{}) {
			defer wg.Done()
			
			dataStr := data.(string)
			results := make([]string, 6)
			innerWg := &sync.WaitGroup{}
			for th := 0; th < 6; th++ {
				innerWg.Add(1)
				go func(th int) {
					defer innerWg.Done()
					thStr := strconv.Itoa(th)
					hash := DataSignerCrc32(thStr + dataStr)
					results[th] = hash
				}(th)
			}
			
			innerWg.Wait()
			result := strings.Join(results, "")
			out <- result
		}(data)
	}
	
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var results []string
	
	for data := range in {
		results = append(results, data.(string))
	}

	sort.Strings(results)
	result := strings.Join(results, "_")
	out <- result
}
