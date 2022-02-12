package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"log"
	"os"
	"pql/creds"
	"pql/util"
	"pql/version"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	AWS_KEY_ENV    = "AWS_ACCESS_KEY_ID"
	AWS_SECRET_ENV = "AWS_SECRET_ACCESS_KEY"
	AWS_REGION_ENV = "AWS_REGION"

	MAX_BATCH_SIZE = 25
	ONE            = int32(1)
	MINUS_ONE      = int32(-1)
)

var (
	maxRetries int
	profile    string
	table      string
	readers    int

	dbAwsKeyId     string
	dbAwsSecretKey string
	dbAwsRegion    string

	rowsRetrieved = new(int32)
	rowsDeleted   = new(int32)
	retries       = new(int32)
	resubs        = new(int32)
	workers       = new(int32)
	getCapUsed    = new(int64)
	deleteCapUsed = new(int64)

	dbClient *dynamodb.Client

	indexes []TableIndex
)

func reportStats(final bool) {
	status := ""
	if final {
		status = "Truncate " + table + " Complete"
	} else {
		status = "Truncate " + table + " Running"
	}
	log.Printf("%s Stats: keys=%d, deleted=%d, resubs=%d, retries=%d, getcap=%d, delcap=%d, workers=%d\n",
		status,
		atomic.LoadInt32(rowsRetrieved),
		atomic.LoadInt32(rowsDeleted),
		atomic.LoadInt32(resubs),
		atomic.LoadInt32(retries),
		atomic.LoadInt64(getCapUsed),
		atomic.LoadInt64(deleteCapUsed),
		atomic.LoadInt32(workers),
	)
}

type TableIndex struct {
	columnName string
	columnType types.ScalarAttributeType
}

func init() {

	flag.StringVar(&profile, "profile", "", "The optional AWS shared config credential profile name")
	flag.StringVar(&table, "table", "", "The table to truncate")
	flag.IntVar(&maxRetries, "maxretries", -1, "The maximum number of retries for a capacity failure (-1 for infinite)")
	flag.IntVar(&readers, "readers", 64, "The number of reader routines to parallel scan and batch delete with")

	usage := flag.Usage
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "truncate: v%s\n", version.VERSION)
		usage()
	}
	flag.Parse()

	if table == "" {
		fmt.Fprintf(os.Stderr, "ERROR: No table specified\n")
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
}

func main() {

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
	dbClient = dynamodb.NewFromConfig(cfg)
	indexes = GetTableIndexes()
	attrNames := make([]string, 0)
	for _, index := range indexes {
		attrNames = append(attrNames, index.columnName)
	}

	log.Printf("Starting table truncation: table=%s, keys=%s\n", table, attrNames)
	go func() {
		for {
			time.Sleep(5 * time.Second)
			reportStats(false)
		}
	}()
	startTime := time.Now()
	StartScan()
	reportStats(true)
	log.Printf("Elapsed: %s\n", time.Since(startTime).String())
	os.Exit(0)
}

func StartScan() {
	var scanWg sync.WaitGroup
	scans := BuildScanRequests()
	for _, scan := range scans {
		scanWg.Add(1)
		go doSegment(scan, &scanWg)
	}
	log.Printf("All readers running: %d\n", readers)
	scanWg.Wait()
	log.Printf("Total Rows: %d\n", atomic.LoadInt32(rowsRetrieved))
}

func doSegment(input *dynamodb.ScanInput, scanWg *sync.WaitGroup) {
	atomic.AddInt32(workers, ONE)
	defer func() {
		scanWg.Done()
		atomic.AddInt32(workers, MINUS_ONE)
	}()
	rows := 0
	deleted := 0
	for {
		if out, err := dbClient.Scan(context.Background(), input); err != nil {
			var oe *smithy.OperationError
			if errors.As(err, &oe) {
				log.Fatalf("Scan OE Error: %s\n", oe.Error())
			} else {
				log.Fatalf("Scan Error: %s\n", err.Error())
			}
		} else {
			atomic.AddInt64(getCapUsed, int64(*out.ConsumedCapacity.CapacityUnits))
			rowCount := len(out.Items)
			rows += rowCount
			atomic.AddInt32(rowsRetrieved, int32(rowCount))
			dels := doDeletes(out.Items, 0)
			deleted += dels
			if out.LastEvaluatedKey != nil {
				input.ExclusiveStartKey = out.LastEvaluatedKey
			} else {
				//log.Printf("Block Complete: segment=%d, rowsFetched=%d, rowsDeleted=%d\n", *input.Segment, rows, deleted)
				break
			}
		}
	}
	//log.Printf("Scan Complete: segment=%d, rowsFetched=%d, rowsDeleted=%d\n", *input.Segment, rows, deleted)
}

func partitionItems(items []map[string]types.AttributeValue) [][]map[string]types.AttributeValue {
	size := len(items)

	batchCount := size / MAX_BATCH_SIZE // + size%MAX_BATCH_SIZE
	if size%MAX_BATCH_SIZE != 0 {
		batchCount++
	}
	batches := make([][]map[string]types.AttributeValue, 0, batchCount)
	x := 0
	batch := make([]map[string]types.AttributeValue, 0, MAX_BATCH_SIZE)
	for idx := 0; idx < size; idx++ {
		batch = append(batch, items[idx])
		x++
		if x == MAX_BATCH_SIZE {
			batches = append(batches, batch)
			x = 0
			batch = make([]map[string]types.AttributeValue, 0, MAX_BATCH_SIZE)
		}
	}
	if len(batch) > 0 {
		batches = append(batches, batch)
	}
	return batches
}

func buildDeleteOp(items []map[string]types.AttributeValue) map[string][]types.WriteRequest {
	size := len(items)
	arr := make([]*dynamodb.BatchWriteItemInput, size, size)
	// RequestItems map[string][]types.WriteRequest
	deletes := make([]types.WriteRequest, size)
	for idx := 0; idx < size; idx++ {
		deletes[idx] = types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: items[idx],
			},
		}
	}
	tDeletes := map[string][]types.WriteRequest{
		table: deletes,
	}
	for idx := 0; idx < size; idx++ {
		arr[idx] = &dynamodb.BatchWriteItemInput{
			RequestItems:           tDeletes,
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		}
	}
	m := map[string][]types.WriteRequest{
		table: deletes,
	}
	return m
}

//func delete(items []map[string]types.AttributeValue) (int, int, []map[string]types.AttributeValue) {
//
//}

func doDeletes(items []map[string]types.AttributeValue, nested int) int {
	_nested := nested
	rowsDel := 0
	batches := partitionItems(items)
	for q, batch := range batches {
		if q == -1 {
			break
		}
		for {
			deleteBatch := buildDeleteOp(batch)
			originalBatchSize := len(deleteBatch[table])
			batchWrite := &dynamodb.BatchWriteItemInput{RequestItems: deleteBatch, ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal}
			if out, err := dbClient.BatchWriteItem(context.Background(), batchWrite); err != nil {
				var oe *smithy.OperationError
				// retry quota exceeded
				if errors.As(err, &oe) {
					if _, ok := oe.Err.(*retry.MaxAttemptsError); ok || strings.Contains(oe.Error(), "retry quota exceeded") {
						total := atomic.AddInt32(retries, ONE)
						if maxRetries > -1 {
							if total > int32(maxRetries) {
								log.Fatalf("Max Retries Exceeded: %d\n", total)
								os.Exit(-1)
							}
						}
						continue
					}
					log.Fatalf("Delete OE Error: %s\n", oe.Error())
					os.Exit(-1)
				} else {
					log.Fatalf("Delete Error: %s\n", err.Error())
					os.Exit(-1)
				}
			} else {
				atomic.AddInt64(deleteCapUsed, int64(*out.ConsumedCapacity[0].CapacityUnits))
				// Increment rowsDeleted
				if len(out.UnprocessedItems) == 0 {
					rowsDel += originalBatchSize
					atomic.AddInt32(rowsDeleted, int32(originalBatchSize))
					break
				} else {
					if unproc, ok := out.UnprocessedItems[table]; ok {
						processed := originalBatchSize - len(unproc)
						rowsDel += processed
						atomic.AddInt32(rowsDeleted, int32(processed))
						rereq := make([]map[string]types.AttributeValue, 0)
						rets := 0
						for _, d := range unproc {
							if d.DeleteRequest != nil {
								rereq = append(rereq, d.DeleteRequest.Key)
								rets++
							}
						}
						atomic.AddInt32(resubs, int32(rets))
						_nested++
						d := doDeletes(rereq, _nested)
						rowsDel += d
					}
					break
				}
			}
		}
	}
	//log.Printf("Delete Batches: nest=%d, deleted=%d\n", _nested, rowsDel)
	return rowsDel
}

func BuildScanRequests() []*dynamodb.ScanInput {
	attrNames := make([]string, 0)
	for _, index := range indexes {
		attrNames = append(attrNames, index.columnName)
	}
	arr := make([]*dynamodb.ScanInput, readers, readers)
	totalSegments := int32(readers)
	for idx := 0; idx < readers; idx++ {
		segment := int32(idx)
		arr[idx] = &dynamodb.ScanInput{
			TableName:              &table,
			AttributesToGet:        attrNames,
			ExclusiveStartKey:      nil,
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
			Segment:                &segment,
			TotalSegments:          &totalSegments,
		}
	}
	return arr
}

func GetTableIndexes() []TableIndex {
	if desc, err := dbClient.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: &table}); err != nil {
		panic(err)
	} else {
		indexes := make([]TableIndex, 0)
		attrTypes := make(map[string]types.ScalarAttributeType, 3)
		for _, k := range desc.Table.AttributeDefinitions {
			attrTypes[*k.AttributeName] = k.AttributeType
		}
		for _, k := range desc.Table.KeySchema {
			indexes = append(indexes, TableIndex{
				columnName: *k.AttributeName,
				columnType: attrTypes[*k.AttributeName],
			})
		}
		return indexes
	}
}
