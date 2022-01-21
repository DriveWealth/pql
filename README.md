# pql
## High Speed DynamoDB PartiQL Executor

pql is a command line utility to execute DynamoDB [PartiQL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.html) statements from a provided file.

PartiQL is a SQL-compatible query language, to select, insert, update, and delete data in Amazon DynamoDB. 

### Usage

```
Parameters: pql [options] [file1 file2 .... fileN]
Usage of pql:
  -maxretries int
        The maximum number of retries for a failed batch write (-1 for infinite) (default -1)
  -pool int
        The size of the thread pool for executing PartiQL batches (default 160)
  -profile string
        The optional AWS shared config credential profile name
  -stats int
        The period on which stats are printed in seconds (default 10)
```

### Example Executions
##### Specifying All Options
```pql -maxretries 20 -pool 200 -profile QA -stats 5 accountUpdates.pql userUpdates.pql```
##### Minimal Options
```pql accountUpdates.pql userUpdates.pql```

If a `profile` is not specified, credentials will default to either:
* Local IAM profile if running on EC2
* The environment variables **AWS_ACCESS_KEY_ID**, **AWS_SECRET_ACCESS_KEY** and **AWS_REGION**. If these are specified, they will override the EC2 IAM profile.

### Example Output
```
➜  ~ pql -profile PER /home/nwhitehead/pql/bo.accounts.3.pql /home/nwhitehead/pql/bo.accounts.4.pql
2022/01/21 16:12:43 Stats Frequency: 10s
Loaded Profile: source=SharedConfigCredentials: /home/nwhitehead/.aws/credentials
2022/01/21 16:12:43 Input Files: count=2, totalLines=20000
2022/01/21 16:12:43 All files in process
2022/01/21 16:12:53 Progress: rowsprocessed=14851, batches=553, failed=0, retries=12787, cap=14851, poolbusy=160, inflight=158
2022/01/21 16:12:56 File Processing Complete: /home/nwhitehead/pql/bo.accounts.3.pql
2022/01/21 16:12:56 File Processing Complete: /home/nwhitehead/pql/bo.accounts.4.pql
2022/01/21 16:12:56 Final Status: rowsprocessed=20000, batches=800, failed=0, retries=18348, cap=20000, poolbusy=121, inflight=0
2022/01/21 16:12:56 Done. Elapsed=12.486873469s
```

### Example PQL File

```
UPDATE "bo.accounts" SET accountMgmtType = 3 WHERE userID = '2a8c1a61-a919-4144-badb-12db3a3004a0' AND accountID = '2a8c1a61-a919-4144-badb-12db3a3004a0.1576613479364';
UPDATE "bo.accounts" SET accountMgmtType = 3 WHERE userID = '9ed1ba94-45ab-47c6-af3c-0ed5745ca829' AND accountID = '9ed1ba94-45ab-47c6-af3c-0ed5745ca829.1576613728307';
```

### PartiQL/pql Caveats, Provisos and Stipulatons

* PartiQL supports C-R-U-D operations, but pql is only useful for writes, so operations should be limited to **UPDATE**, **INSERT** and **DELETE** operations.
* Any pql input file should be limited to only one type of operation (**UPDATE**, **INSERT** or **DELETE**), but will support operations against multiple tables.
* Tables containing a dot (.) need to be wrapped in double quotes (as seen in the Example PQL File above)



