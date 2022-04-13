# PartiQL Command Line Tools for DynamoDB

## pql: High Speed DynamoDB PartiQL Executor

pql is a command line utility to execute DynamoDB [PartiQL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.html) statements from a provided file.

PartiQL is a SQL-compatible query language, to select, insert, update, and delete data in Amazon DynamoDB. 

### Downloads
Binaries can be downloaded [here](https://github.com/DriveWealth/pql/releases/tag/v0.2a).

### Usage

```
pql: v0.5a
Parameters: pql [options] [file1 file2 .... fileN]
Usage of pql:
  -faker
    	Specify to enable faker test data generation and token substitution
  -maxretries int
    	The maximum number of retries for a failed batch write (-1 for infinite) (default -1)
  -noexec
    	Specify to disable statement execution, but just output the statements as a dry run
  -pool int
    	The size of the thread pool for executing PartiQL batches (default 160)
  -profile string
    	The optional AWS shared config credential profile name
  -stats int
    	The period on which stats are printed in seconds (default 10)
```

 

### Example Executions
##### Specifying All Options
```pql -maxretries 20 -pool 200 -profile QA -stats 5 -faker accountUpdates.pql userUpdates.pql```
##### Minimal Options
```pql accountUpdates.pql userUpdates.pql```
##### Use StdIn instead of specifying a file
```cat queries.txt | pql -profile QA```

### Authentication
If a `profile` is not specified, credentials will default to either:
* Local IAM profile if running on EC2
* The environment variables **AWS_ACCESS_KEY_ID**, **AWS_SECRET_ACCESS_KEY** and **AWS_REGION**. If these are specified, they will override the EC2 IAM profile.

### Faker
When faker is enabled, faker symbols in the submitted queries will be dynamically substituted with the symbol's resolved values.
e.g. For a query like `UPDATE "bo.users"  SET addressLine1 = '##streetaddress##', addressLine2 = '' WHERE userID = 'f3b5a3d9-99a9-40bb-8755-e2c4cc862adf';`
the faker symbol `##streetaddress##` will be replaced with a random street address:
`UPDATE "bo.users"  SET addressLine1 = '8 Helena Knoll', addressLine2 = '' WHERE userID = 'f3b5a3d9-99a9-40bb-8755-e2c4cc862adf';`

All supported faker symbols are listed in [Appendix A](https://github.com/DriveWealth/pql#Appendix-A) below.

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
* For PartiQL **SELECT** queries, see [pqlquery](https://github.com/DriveWealth/pql#pqlquery).
* Any pql input file should be limited to only one type of operation (**UPDATE**, **INSERT** or **DELETE**), but will support operations against multiple tables.
* Tables containing a dot (.) need to be wrapped in double quotes (as seen in the Example PQL File above)

## pqlquery: PartiQL SQL Queries for DynamoDB
pqlquery is a command line utility for executing PartiQL queries against DynamoDB. Results are returned as one line of JSON per row returned.

### Usage
```
pqlquery: v0.5a
Usage of pqlquery:
  -consistent
    	Specify for consistent reads
  -count
    	Specify to retrieve count of matching rows only
  -maxretries int
    	The maximum number of retries for a capacity failure (-1 for infinite) (default -1)
  -maxrows int
    	The maximum number of rows to retrieve (-1 for infinite) (default -1)
  -minify
    	Specify for minified JSON instead of DynamoDB JSON
  -nout
    	Specify to suppress completion message
  -profile string
    	The optional AWS shared config credential profile name
  -query string
    	The PartiSQL statement to execute
  -template string
    	The name of a query template file to generate pql statements with, or just the content
```

#### Examples

##### Query
```
pqlquery -profile UAT -query "select jobStart, jobName, jobSpecificData, itemCount, successCount from \"sys.jobStatus\" where subSystem = 'INTELICLEAR' and createdWhen > '2019-' and jobName = 'MOD_FINTRN' and itemCount > 0 ORDER BY createdWhen desc"
```

##### Output

```
➜  queries pqlquery -profile UAT -query "select jobStart, jobName, jobSpecificData, itemCount, successCount from \"sys.jobStatus\" where subSystem = 'INTELICLEAR' and createdWhen > '2019-' and jobName = 'MOD_FINTRN' and itemCount > 0 ORDER BY createdWhen desc" | more
{"itemCount":{"Value":"17722"},"jobName":{"Value":"MOD_FINTRN"},"jobStart":{"Value":"2022-01-21T18:32:46.972Z"},"successCount":{"Value":"770"}}
{"itemCount":{"Value":"30843"},"jobName":{"Value":"MOD_FINTRN"},"jobStart":{"Value":"2022-01-20T18:32:00.031Z"},"successCount":{"Value":"527"}}
{"itemCount":{"Value":"1010771"},"jobName":{"Value":"MOD_FINTRN"},"jobStart":{"Value":"2022-01-19T18:32:02.659Z"},"successCount":{"Value":"973"}}
{"itemCount":{"Value":"894984"},"jobName":{"Value":"MOD_FINTRN"},"jobStart":{"Value":"2022-01-18T18:31:53.042Z"},"successCount":{"Value":"934"}}
{"itemCount":{"Value":"23835"},"jobName":{"Value":"MOD_FINTRN"},"jobStart":{"Value":"2022-01-14T18:33:12.865Z"},"successCount":{"Value":"1025"}}
{"itemCount":{"Value":"18800"},"jobName":{"Value":"MOD_FINTRN"},"jobStart":{"Value":"2022-01-13T18:32:00.495Z"},"successCount":{"Value":"1856"}}
{"itemCount":{"Value":"602872"},"jobName":{"Value":"MOD_FINTRN"},"jobStart":{"Value":"2022-01-12T18:32:41.914Z"},"successCount":{"Value":"818"}}
<SNIP>
```

#### Minified JSON Output

The `-minify` option for pqlquery will make a best effort to clean up the JSON output and generate a more standardized document structure.

##### DynamoDB Default JSON

```json
{
  "itemCount": {
    "Value": "14983925"
  },
  "jobName": {
    "Value": "MOD_ACCOUNTS"
  },
  "jobStart": {
    "Value": "2022-01-03T18:33:28.346Z"
  },
  "successCount": {
    "Value": "0"
  }
}
```
##### Minified JSON

```json
{
  "itemCount": 14983925,
  "jobName": "MOD_ACCOUNTS",
  "jobStart": "2022-01-03T18:33:28.346Z",
  "successCount": 0
}
```

#### Generating `pql` Input from `pqlQuery`

`pqlquery` output can be transformed using your favorite command line tools and then redirected to `pql` for execution.

For example, this `pqlquery` generates PartiQL inserts using `jq` and `awk`:

```
pqlquery -profile UAT -nout -minify -query "select * from \"ref.sequences\" where begins_with(sequenceName, 'accountNo_');" | jq -r '[.sequenceName, .nextNo] | @csv' | sed  's/"//g' | awk '{split($0,a,","); printf("INSERT INTO \"ref.sequences\" value {'\''sequenceName'\'' : '\''%s'\'', '\''nextNo'\'' : %s);\n", a[1], a[2])}'
```

The output is:

```
INSERT INTO "ref.sequences" value {'sequenceName' : 'accountNo_GQUF', 'nextNo' : 16);
INSERT INTO "ref.sequences" value {'sequenceName' : 'accountNo_UNMH', 'nextNo' : 10);
INSERT INTO "ref.sequences" value {'sequenceName' : 'accountNo_RLRY', 'nextNo' : 82);
INSERT INTO "ref.sequences" value {'sequenceName' : 'accountNo_VTDD', 'nextNo' : 4);
<SNIP>
```
This output can be captured into a file, but can also be redirected to `pql` for execution:

```
pqlquery -profile UAT -nout -minify -query "select * from \"ref.sequences\" where begins_with(sequenceName, 'accountNo_');" | jq -r '[.sequenceName, .nextNo] | @csv' | sed  's/"//g' | awk '{split($0,a,","); printf("INSERT INTO \"ref.sequences\" value {'\''sequenceName'\'' : '\''%s'\'', '\''nextNo'\'' : %s);\n", a[1], a[2])}' | pql -profile PER
```

### Templates

Templates provide data driven transformation of each row returned from a query execution.
Consider a pql query like this: `select userID from \"bo.users\"`
and a template file like this:
`UPDATE "bo.users"  SET addressLine1 = '3249 Bay Street', addressLine2 = '' WHERE userID = '{{.userID}}';`

The template variable `{{.userID}}` will be replaced by the actual **userID** returned in each row of the query.

Templates can also be used with **pql** redirects and fakers to generate database load. Consider the same query as above and this template (in the file `t.tmp`):
`UPDATE "bo.users"  SET addressLine1 = '##streetaddress##', addressLine2 = '' WHERE userID = '{{.userID}}';`

Executing the query `pqlquery -profile DEV -query "select userID from \"bo.users\"" -minify -template t.temp -maxrows 3` would produce the following output:

```
UPDATE "bo.users"  SET addressLine1 = '##streetaddress##', addressLine2 = '' WHERE userID = 'f3b5a3d9-99a9-40bb-8755-e2c4cc862adf';
UPDATE "bo.users"  SET addressLine1 = '##streetaddress##', addressLine2 = '' WHERE userID = '2ae341cb-8381-4b31-9063-d508137bd5e6';
UPDATE "bo.users"  SET addressLine1 = '##streetaddress##', addressLine2 = '' WHERE userID = '2091da29-f516-4625-a3cc-d47a0621eddb';
```

Now the output can be redirected to **pql** to execute the update statements:

```
➜  pql pqlquery -profile DEV -query "select userID from \"bo.users\"" -minify -template t.temp -maxrows 3 | pql -profile DEV -faker
2022/04/13 10:50:30 Stats Frequency: 10s
Complete: rows=3, retries=0, executions=1, capacity=126, elapsed=330.953699ms
2022/04/13 10:50:30 Input Files: count=1, totalLines=3
2022/04/13 10:50:31 Loaded 92 Wlps
2022/04/13 10:50:31 Instruments Loaded: count=4335, elapsed=237.942176ms
2022/04/13 10:50:31 All files in process
UPDATE "bo.users"  SET addressLine1 = '72 Shayna Springs Apt. 536', addressLine2 = '' WHERE userID = 'f3b5a3d9-99a9-40bb-8755-e2c4cc862adf';
UPDATE "bo.users"  SET addressLine1 = '171672 Daniel View', addressLine2 = '' WHERE userID = '2ae341cb-8381-4b31-9063-d508137bd5e6';
UPDATE "bo.users"  SET addressLine1 = '12168 Orland Skyway', addressLine2 = '' WHERE userID = '2091da29-f516-4625-a3cc-d47a0621eddb';
2022/04/13 10:50:31 File Processing Complete: /tmp/pql-tmp087320653
2022/04/13 10:50:31 Final Status: rowsprocessed=3, batches=1, failed=0, retries=0, cap=24, poolbusy=2, inflight=0
2022/04/13 10:50:31 Done. Elapsed=38.64447ms
```

In order to test this without actually executing the updates, you can use the **-noexec** option.

```
➜  pql pqlquery -profile DEV -query "select userID from \"bo.users\"" -minify -template t.temp -maxrows 3 | pql -profile DEV -faker -noexec
2022/04/13 10:54:21 Stats Frequency: 10s
Complete: rows=3, retries=0, executions=1, capacity=126, elapsed=247.764893ms
2022/04/13 10:54:21 Input Files: count=1, totalLines=3
2022/04/13 10:54:21 Loaded 92 Wlps
2022/04/13 10:54:21 Instruments Loaded: count=4335, elapsed=255.494018ms
2022/04/13 10:54:21 All files in process
UPDATE "bo.users"  SET addressLine1 = '657876 Lemke Port Suite 900', addressLine2 = '' WHERE userID = 'f3b5a3d9-99a9-40bb-8755-e2c4cc862adf';
UPDATE "bo.users"  SET addressLine1 = '3482 Flatley Rue Suite 685', addressLine2 = '' WHERE userID = '2ae341cb-8381-4b31-9063-d508137bd5e6';
UPDATE "bo.users"  SET addressLine1 = '812 Marilie Cape', addressLine2 = '' WHERE userID = '2091da29-f516-4625-a3cc-d47a0621eddb';
2022/04/13 10:54:21 File Processing Complete: /tmp/pql-tmp498925686
2022/04/13 10:54:21 No statement executed (-noexec was enabled)
2022/04/13 10:54:21 Done. Elapsed=124.763µs
```

- [Templates Cheat Sheet](https://docs.google.com/document/d/1OCgrDgrSEcF6QYEQHOMvyoVYXZljx7qfY9F1Eiv_8AA/edit?usp=sharing)

## ddbtruncate: Fast Table Truncation for DynamoDB

When you have a DynamoDB table you want to truncate (delete all the records), it might be easiest to just drop the table and recreate it. 
However, depending on the capacity settings and the table's secondary indexes, it might be quicker to use **ddbtruncate**.

### ddbtruncate Usage

```
truncate: v0.5a
Usage of ddbtruncate:
  -maxretries int
    	The maximum number of retries for a capacity failure (-1 for infinite) (default -1)
  -profile string
    	The optional AWS shared config credential profile name
  -readers int
    	The number of reader routines to parallel scan and batch delete with (default 64)
  -table string
    	The table to truncate
```

#### Example

```ddbtruncate -profile PER -table aod.streamAudit```

##### Output

```
2022/01/27 20:02:01 Starting table truncation: table=aod.streamAudit, keys=[eventID]
2022/01/27 20:02:01 All readers running: 64
2022/01/27 20:02:06 Truncate aod.streamAudit Running Stats: keys=72863, deleted=62811, resubs=1493, retries=0, getcap=28899, delcap=508082, workers=64
2022/01/27 20:02:11 Truncate aod.streamAudit Running Stats: keys=99122, deleted=97815, resubs=2200, retries=0, getcap=39322, delcap=791234, workers=11
2022/01/27 20:02:16 Truncate aod.streamAudit Running Stats: keys=101602, deleted=101589, resubs=2225, retries=0, getcap=40305, delcap=821748, workers=1
2022/01/27 20:02:16 Total Rows: 101602
2022/01/27 20:02:16 Truncate aod.streamAudit Complete Stats: keys=101602, deleted=101602, resubs=2225, retries=0, getcap=40305, delcap=821852, workers=0
2022/01/27 20:02:16 Elapsed: 15.01848681s
```

### Appendix-A: Faker Symbols

- **##yearcode##** : The current DriveWealth year code
- **##monthcode##** : The current DriveWealth month code
- **##instrumentID##** : A random Instrument ID
- **##firstname##** : A random first name
- **##streetaddress##** : A random street address
- **##float##** : A random float between 0 and 99999999 with a maximum of 8 decimals
- **##bool##** : A random boolean
- **##isotime##** : A random offset off the current date/time in ISO format
- **##int##** : A random 32 bit int
- **##userid##** : A random `bo.users` userID
- **##lastname##** : A random last name
- **##state##** : A random state (2 chars)
- **##zip##** : A random zip code
- **##title##** : A random title
- **##side##** : A random trade side (B or S)
- **##company##** : A random company name
- **##phone##** : A random phone number
- **##uuid##** : A random UUID
- **##orderID##** : A random orderID (<Current YearCode><Current MonthCode>).<UUID>)
- **##accountID##** : A random accountID (<UUID>.<Random 64 bit int>)
- **##fintranID##** : A random finTranID (<Current YearCode><Current MonthCode>).<UUID>)
- **##symbol##** : A random Instrument symbol
- **##city##** : A random city name
- **##long##** : A random 64 bit int
- **##gender##** : A random gender
- **##email##** : A random email




