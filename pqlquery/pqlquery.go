package main

import (
	"context"
	"encoding/json"
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
	"pql/ddb"
	"pql/util"
	"pql/version"
	"strings"
	"sync/atomic"
	"time"
)

const (
	AWS_KEY_ENV    = "AWS_ACCESS_KEY_ID"
	AWS_SECRET_ENV = "AWS_SECRET_ACCESS_KEY"
	AWS_REGION_ENV = "AWS_REGION"
)

var (
	maxRetries int
	profile    string
	query      string
	consistent bool
	minify     bool
	nout       bool

	dbAwsKeyId     string
	dbAwsSecretKey string
	dbAwsRegion    string

	rowsRetrieved = new(int32)
	retries       = new(int32)
	capUsed       = new(int64)

	dbClient *dynamodb.Client

	ONE       = int32(1)
	MINUS_ONE = int32(-1)
)

func init() {

	flag.StringVar(&profile, "profile", "", "The optional AWS shared config credential profile name")
	flag.StringVar(&query, "query", "", "The PartiSQL statement to execute")
	flag.BoolVar(&consistent, "consistent", false, "Specify for consistent reads")
	flag.BoolVar(&minify, "minify", false, "Specify for minified JSON instead of DynamoDB JSON")
	flag.BoolVar(&nout, "nout", false, "Specify to suppress completion message")
	flag.IntVar(&maxRetries, "maxretries", -1, "The maximum number of retries for a capacity failure (-1 for infinite)")

	usage := flag.Usage
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "pqlquery: v%s\n", version.VERSION)
		usage()
	}
	flag.Parse()

	if query == "" {
		fmt.Fprintf(os.Stderr, "ERROR: No query specified\n")
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
	//fmt.Fprintf(os.Stderr, "Output: %s\n", stdOutFileName())
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
	retries := 0
	loops := 0
	var nextToken *string = nil
	startTime := time.Now()
	for {

		if out, err := dbClient.ExecuteStatement(context.TODO(), &dynamodb.ExecuteStatementInput{
			Statement:              &query,
			ConsistentRead:         &consistent,
			NextToken:              nextToken,
			Parameters:             nil,
			ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		}); err != nil {
			if serr, ok := err.(*smithy.OperationError); ok {
				if rerr, ok := serr.Err.(*retry.MaxAttemptsError); ok {
					retries++
					if maxRetries != -1 && retries > maxRetries {
						log.Fatalf("Statement Failure: error=%s\n", rerr.Error())
					} else {
						continue
					}
				}
				if strings.Contains(serr.Error(), "quota") || strings.Contains(serr.Err.Error(), "quota") {
					retries++
					if maxRetries != -1 && retries > maxRetries {
						log.Fatalf("Statement Failure: error=%s\n", serr.Err.Error())
					} else {
						continue
					}
				}
			}
			log.Fatalf("Statement Failure: error=%s\n", err.Error())
		} else {
			loops++
			if loops%10 == 0 {
				fmt.Fprintf(os.Stderr, "In Process: rows=%d, retries=%d, executions=%d, capacity=%d, elapsed=%s\n",
					atomic.LoadInt32(rowsRetrieved),
					retries,
					loops,
					atomic.LoadInt64(capUsed),
					time.Since(startTime).String())
			}
			retries = 0
			if out.ConsumedCapacity != nil && out.ConsumedCapacity.CapacityUnits != nil {
				atomic.AddInt64(capUsed, int64(*out.ConsumedCapacity.CapacityUnits))
			}
			if out.Items != nil {
				for _, item := range out.Items {
					atomic.AddInt32(rowsRetrieved, ONE)
					if minify {
						minied := ddb.ExtractItem(item)
						if b, err := json.Marshal(minied); err == nil {
							fmt.Printf("%s\n", string(b))
						}

					} else {
						if b, err := json.Marshal(item); err == nil {
							fmt.Printf("%s\n", string(b))
						}
					}
				}
			}

			if out.NextToken == nil {
				if !nout {
					fmt.Fprintf(os.Stderr, "Complete: rows=%d, retries=%d, executions=%d, capacity=%d, elapsed=%s\n",
						atomic.LoadInt32(rowsRetrieved),
						retries,
						loops,
						atomic.LoadInt64(capUsed),
						time.Since(startTime).String())
				}
				break
			} else {
				nextToken = out.NextToken
			}
		}
	}
}

func stdOutFileName() string {
	stat, _ := os.Stdout.Stat()
	return stat.Name()
}
