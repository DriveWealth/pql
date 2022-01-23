package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/panjf2000/ants/v2"
	"io"
	"log"
	"os"
	"pql/creds"
	"pql/util"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const (
	VERSION        = "0.1b"
	MAX_BATCH_SIZE = 25
	AWS_KEY_ENV    = "AWS_ACCESS_KEY_ID"
	AWS_SECRET_ENV = "AWS_SECRET_ACCESS_KEY"
	AWS_REGION_ENV = "AWS_REGION"
)

var (
	poolSize   int
	statsFreq  int
	maxRetries int
	profile    string
	inFiles    []string
	pool       *ants.Pool
	freq       time.Duration

	totalLines int
	okFiles    int

	dbAwsKeyId     string
	dbAwsSecretKey string
	dbAwsRegion    string

	rowsFailed      = new(int32)
	batchesFailed   = new(int32)
	executed        = new(int32)
	retries         = new(int32)
	inFlight        = new(int32)
	capUsed         = new(int64)
	executedBatches = new(int32)

	dbClient *dynamodb.Client

	ONE       = int32(1)
	MINUS_ONE = int32(-1)
)

func init() {

	cores := runtime.NumCPU()
	runtime.GOMAXPROCS(cores)
	flag.IntVar(&poolSize, "pool", cores*10, "The size of the thread pool for executing PartiQL batches")
	flag.IntVar(&statsFreq, "stats", 10, "The period on which stats are printed in seconds")
	flag.StringVar(&profile, "profile", "", "The optional AWS shared config credential profile name")
	flag.IntVar(&maxRetries, "maxretries", -1, "The maximum number of retries for a failed batch write (-1 for infinite)")

	usage := flag.Usage
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "pql: v%s\n", VERSION)
		fmt.Fprintf(os.Stderr, "Parameters: pql [options] [file1 file2 .... fileN]\n")
		usage()
	}
	flag.Parse()
	freq = time.Duration(statsFreq) * time.Second
	log.Printf("Stats Frequency: %s\n", freq.String())
	inFiles = flag.Args()

	if len(inFiles) == 0 {
		fmt.Fprintf(os.Stderr, "ERROR: No input files specified\n")
		os.Exit(-9)
	}

	if profile != "" {
		pcfg, err := creds.GetProfileCreds(profile)
		if err != nil {
			fmt.Printf("ERROR: Failed to load credentials for profile [%s]: %s\n", profile, err.Error())
			os.Exit(-10)
		}
		//fmt.Printf("Loaded Profile: source=%s\n", pcfg[0])
		dbAwsKeyId = pcfg[1]
		dbAwsSecretKey = pcfg[2]
		dbAwsRegion = pcfg[3]
	} else {
		dbAwsKeyId = util.Env("", AWS_KEY_ENV)
		dbAwsSecretKey = util.Env("", AWS_SECRET_ENV)
		dbAwsRegion = util.Env("us-east-1", AWS_REGION_ENV)
	}

	pool, _ = ants.NewPool(poolSize, ants.WithPreAlloc(true))
}

func main() {
	//defer ants.Release()
	totalLines, okFiles = evalFiles(inFiles)
	if okFiles < 1 {
		fmt.Fprintf(os.Stderr, "ERROR: No valid input files specified\n")
		os.Exit(-9)
	}
	log.Printf("Input Files: count=%d, totalLines=%d\n", okFiles, totalLines)
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(dbAwsRegion),
		config.WithCredentialsProvider(creds.NewChainedCredentialProvider(
			credentials.NewStaticCredentialsProvider(dbAwsKeyId, dbAwsSecretKey, ""),
			ec2rolecreds.New(),
		)),
	)

	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	go func() {
		for {
			time.Sleep(10 * time.Second)
			reportStats(false)
		}
	}()

	// Using the Config value, create the DynamoDB client
	dbClient = dynamodb.NewFromConfig(cfg)

	var globalWg sync.WaitGroup
	globalWg.Add(okFiles)
	startTime := time.Now()
	for _, fileName := range inFiles {
		fname := fileName
		pool.Submit(func() { // FIXME
			processFile(fname, &globalWg)
		})
	}
	log.Printf("All files in process\n")
	globalWg.Wait()
	pool.Release()
	reportStats(true)
	log.Printf("Done. Elapsed=%s\n", time.Since(startTime))

	os.Exit(0)

}

func processFile(fileName string, globalWg *sync.WaitGroup) {
	defer globalWg.Done()
	file, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer func() {
		closeFile(file)
	}()
	scanner := bufio.NewScanner(file)
	arr := make([]types.BatchStatementRequest, 0, MAX_BATCH_SIZE)
	currentBatchSize := 0
	var fileWg sync.WaitGroup
	for scanner.Scan() {
		st := strings.TrimSpace(scanner.Text())
		if len(st) == 0 {
			continue
		}
		req := types.BatchStatementRequest{
			Statement: aws.String(st),
		}
		arr = append(arr, req)
		currentBatchSize++
		if len(arr) == MAX_BATCH_SIZE {
			currentBatchSize = 0
			arrCopy := arr
			arr = make([]types.BatchStatementRequest, 0, MAX_BATCH_SIZE)
			fileWg.Add(1)
			pool.Submit(func() { // FIXME: handle possible pool failure
				defer fileWg.Done()
				submitBatch(arrCopy)
			})
		}
	}
	if currentBatchSize > 0 {
		fileWg.Add(1)
		pool.Submit(func() { // FIXME: handle possible pool failure
			defer fileWg.Done()
			submitBatch(arr)
		})
	}
	fileWg.Wait()
	log.Printf("File Processing Complete: %s\n", fileName)
}

func submitBatch(arrCopy []types.BatchStatementRequest) {
	retryCount := 0
	atomic.AddInt32(inFlight, ONE)
	defer func() {
		atomic.AddInt32(inFlight, MINUS_ONE)
		if retryCount > 0 {
			atomic.AddInt32(retries, int32(retryCount))
		}
	}()

	for {
		failedCommands, err := executeBatch(dbClient, arrCopy)
		if err != nil {
			// Whole batch failed, not cap related
			atomic.AddInt32(batchesFailed, int32(len(arrCopy)))
			break
		} else {
			if failedCommands != nil && len(failedCommands) > 0 {
				atomic.AddInt32(executed, int32(len(arrCopy))-int32(len(failedCommands)))
				retryCount++
				if maxRetries > 0 {
					if retryCount > maxRetries {
						// Retries Exhausted, fail all rows
						atomic.AddInt32(rowsFailed, int32(len(arrCopy)))
						break
					} else {
						// Retries not exhausted yet
						arrCopy = failedCommands
						continue
					}
				} else {
					// Retrying indefinitely
					arrCopy = failedCommands
					continue
				}
			} else {
				// All rows successful
				atomic.AddInt32(executed, int32(len(arrCopy)))
				break
			}
		}
	}
	atomic.AddInt32(executedBatches, ONE)
	return
}

func executeBatch(client *dynamodb.Client, commands []types.BatchStatementRequest) (capFailedCommands []types.BatchStatementRequest, err error) {
	failedArr := make([]types.BatchStatementRequest, 0)

	var totalCap = int64(0)
	if out, batchErr := client.BatchExecuteStatement(context.TODO(), &dynamodb.BatchExecuteStatementInput{
		Statements:             commands,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	}); batchErr != nil {
		//log.Fatalf("Batch Write Failed: error=%s\n", batchErr.Error())
		return nil, batchErr
	} else {
		if len(out.ConsumedCapacity) > 0 {
			for _, cc := range out.ConsumedCapacity {
				if cc.CapacityUnits != nil {
					//atomic.AddInt64(_capUsed, int64(*out.ConsumedCapacity[0].CapacityUnits))
					totalCap += int64(*cc.CapacityUnits)
				}
			}
		}
		for idx, rez := range out.Responses {
			if rez.Error != nil {
				if rez.Error.Code == "ThrottlingError" {
					failedArr = append(failedArr, commands[idx])
				}
			}
		}

	}
	atomic.AddInt64(capUsed, totalCap)
	return failedArr, nil
}

func closeFile(file *os.File) {
	if file != nil {
		err := file.Close()
		if err != nil {
			log.Printf("WARNING: Failed to close file: file=%s, error=%s\n", file.Name(), err.Error())
		}
	}
}

func reportStats(final bool) {
	if final {
		log.Printf("Final Status: rowsprocessed=%d, batches=%d, failed=%d, retries=%d, cap=%d, poolbusy=%d, inflight=%d\n",
			atomic.LoadInt32(executed), atomic.LoadInt32(executedBatches), atomic.LoadInt32(rowsFailed), atomic.LoadInt32(retries), atomic.LoadInt64(capUsed), pool.Running(), atomic.LoadInt32(inFlight),
		)
	} else {
		log.Printf("Progress: rowsprocessed=%d, batches=%d, failed=%d, retries=%d, cap=%d, poolbusy=%d, inflight=%d\n",
			atomic.LoadInt32(executed), atomic.LoadInt32(executedBatches), atomic.LoadInt32(rowsFailed), atomic.LoadInt32(retries), atomic.LoadInt64(capUsed), pool.Running(), atomic.LoadInt32(inFlight),
		)
	}
}

func evalFiles(names []string) (int, int) {
	lines := 0
	ok := 0
	for _, name := range names {
		if l, err := lineCounter(name); err == nil {
			lines += l
			ok++
		} else {
			log.Printf("WARNING: Failed to open file: name=%s, error=%s\n", name, err.Error())
		}
	}
	return lines, ok
}

func lineCounter(fileName string) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}
	f, ferr := os.Open(fileName)
	if ferr != nil {
		return -1, ferr
	}
	defer func() {
		closeFile(f)
	}()
	for {
		c, err := f.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}
