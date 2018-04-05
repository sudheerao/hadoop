<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# S3 Select

**Experimental Feature**

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->

S3 Select is a feature for Amazon S3 introduced in April 2018. It allows for
SQL-like SELECT expressions to be applied to CSV and JSON files.
By performing the SELECT operation in S3 itself, the bandwidth requirements
between S3 and the hosts making the request may be reduced.
Along with latency, this bandwidth is often the limiting factor in processing
data from S3, especially with larger CSV datasets.

Hadoop's S3A Client has experimental support for this feature, with the
following warnings:

* This is experimental, treat with caution.
* It is unlikely to be supported by third party S3 implementations.
* The performance characteristics of this feature are untested.
* Performance appears best when the selection restricts the number of fields,
and projected columns: the less data returned, the faster the response.
* High-level support in tools such as Apache Hive and Spark will also be
evolving. Nobody has ever written CSV connectors with predicate pushdown before.
* The CSV filtering is being done in S3. If the CSV Files cannot be parsed,
that's not something which can be fixed in Hadoop or layers above.

The S3A client's I/O support has currently been optimized for high-performance
I/O with columnar data formats such as ORC and Parquet.
These provide robust and efficient storage for many gigabytes of data, along with
stable schema mechanisms capable of describing complex data structures.
For data processing within the Hadoop family of tools, these are currently the
best tested and benchmarked structures,


## Currently Implemented Features

* CSV input with/without compression.
* CSV output.

## Not supported (yet)

* JSON files.


## Enabling/Disabling S3 Select

Set/clear the option `fs.s3a.select.enabled`. This is true by default, as
Amazon S3 supports it across all regions, for all accounts.

To probe to see if a FileSystem instance implements it, 
`S3AFileSystem.hasCapability("s3a:s3-select")` will be true iff
the version of S3A is aware of S3 Select, and the destination store
has it enabled. 

## Using S3 Select in applications

There is an unstable proof-of-concept API call in the S3A Filesystem client,
`select(Path, expression)`. This allows a select call to be made of a file.

* This is unstable and will be deleted once an extensible mechanism for
configuring the `FileSystem.open()` command has been added.

* The `S3AFileSystem.open(Path)` method invokes `select(path, expression)`
when a file is opened and the configuration option `fs.s3a.select.expression`
is non-empty. This is clearly unsafe in a multithreaded environment, and exists
purely to allow PoC applications to make select calls without needing a new
API. Again, it will be deleted as soon as a stable API is available.

## S3 Select configuration options.

Consult the javadocs for `org.apache.hadoop.fs.s3a.select.SelectConstants`.

## Security

SQL Injection attacks are the classic attack on data. 
Because this is a read-only API, the classic ["Bobby Tables"](https://xkcd.com/327/)
attack to gain write access isn't going to work. Even so: sanitize your inputs.

CSV does have security issues of its own, specifically:

*Excel and other spreadsheets may interpret some fields beginning with special
characters as formula, and execute them*

S3 Select does not appear vulnerable to this, but in workflows where untrusted
data eventually ends up in a spreadsheet (including Google Document spreadsheet),
the data should be sanitized/audited first. There is no support for
such sanitization in S3 Select or in the S3A connector.


Links

* [CVE-2014-3524](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2014-3524).
* [The Absurdly Underestimated Dangers of CSV Injection](http://georgemauer.net/2017/10/07/csv-injection.html).
* [Comma Separated Vulnerabilities](https://www.contextis.com/blog/comma-separated-vulnerabilities).

## Using


### SQL Syntax

The SQL Syntax directly supported by the AWS S3 Select API is documented by
Amazon themselves.

* Use single quotes for all constants, not double quotes.
* All CSV column values are strings unless cast to a type
* Simple SELECT calls, no JOIN.

### CSV formats

"CSV" is less a format, more "a term meaning the data is in some nonstandard
line-by-line" test file, and there are even "multiline CSV files".

S3 Select only supports a subset of the loose "CSV" concept, as covered in
the AWS documentation.

The specific quotation character, field and record delimiters, comments and escape
characters can be configured in the Hadoop configuration.

### API

Hadoop does not currently offer a way to provide a set of options to an `open(path)`
command; this has been discussed and now, with, S3 Select support, likely in
the near future. 

Until then, the ways of invoking the S3 Select call are nonstandard and
unstable.

#### Prototype `S3AFileSystem.select()` method.

There is an unstable PoC API entry point in the `S3AFileSystem` class, 
`select(path, expression, options)`.

```java
DataInputStream select(Path source,
      String expression,
      Configuration conf) throws IOException 
```

#### Standard `FileSystem.open()` command + Hadoop configuration options

It is also possible to use the standard `FileSystem.open()` command, 
by setting the property `fs.s3a.select.expression` property when creating
the filesystem instance.

The option can be changed/cleared in the configuration returned in `FileSystem.getConf()`,
so changing the behavior of the next `open()` command.


This is dangerously brittle, but does let you invoke the API without needing
any explicit calls on the client.


### Probing for support of S3 Select.


S3AFileSystem implements `org.apache.hadoop.fs.StreamCapabilities.StreamCapability`;
the `hasCapability()` call can be used to probe for it being supported on
the specific Hadoop version and S3 filesystem instance.

1. If the FileSystem cannot be case to `StreamCapability`, the API is not supported.
1. If the call `hasCapability("s3a:s3-select")` on the cast object returns
true.

If either of these conditions is not met, the filesystem does not support
the feature.

In Java, this can be tested as follows:

```java
boolean isSelectAvailable(final FileSystem filesystem) {
  return filesystem instanceof StreamCapabilities
      && ((StreamCapabilities) filesystem).hasCapability("s3a:s3-select");
}
```

Do not rely on the ability to cast a `FileSystem` instance to
use the `S3AFileSystem.select()` as evidence the store supports S3 Select,
as any store which does not support the feature 

### CLI: `hadoop s3guard select`

The `s3guard select` command allows direct select statements to be made
of a path

Usage:

```
hadoop s3guard select [OPTIONS] \
 [-limit lines] \
 [-header (use|none|ignore)] \
 [-out file] \
 [-compression (gzip|none)] \
  <PATH> <SELECT QUERY>
```

The output is printed, followed by some summary statistics, unless the `-out`
option is used to declare a destination file. In this mode 
status will be logged to the console, but the output of the query will be
saved directly to the output file.

Example: read the first 100 rows of the landsat dataset where cloud cover is zero

```
hadoop s3guard select -header use -compression gzip -limit 100  \
  s3a://landsat-pds/scene_list.gz \
  "SELECT s.entityId FROM S3OBJECT s WHERE s.cloudCover = '0.0'"
```


Return the entityId column for all rows in the dataset where the cloud
cover was 0.0., and safe it to the file `output.csv` 

```
hadoop s3guard select -header use -out output.csv \
  s3a://landsat-pds/scene_list.gz \
  "SELECT s.entityId from S3OBJECT s WHERE s.cloudCover = '0.0'"
```

This file will:

1. Be UTF-8 encoded.
1. Have quotes on all columns returned.
1. Use commas as a separator.
1. Not have any header.

### Compression Formats

Set `fs.s3a.select.compression` to `GZIP`

## Troubleshooting

The exceptions here are all based on the experience during writing tests;
more may surface with broader use.

The failures listed here are all considered unrecoverable and will not be
reattempted.



### AWSBadRequestException `InvalidColumnIndex`


Your SQL is wrong.

```
org.apache.hadoop.fs.s3a.AWSBadRequestException:
  Select: SELECT * FROM S3OBJECT WHERE odd = true on test/testSelectOddLines.csv:
  com.amazonaws.services.s3.model.AmazonS3Exception:
  The column index at line 1, column 30 is invalid.
  Please check the service documentation and try again.
  (Service: Amazon S3; Status Code: 400; Error Code: InvalidColumnIndex;
```


Here it's the first line of the query, column 30. Paste the query
into an editor and position yourself on the line and column at fault.

```sql
SELECT * FROM S3OBJECT WHERE odd = true
                             ^ HERE
```

Another example is where the headers are being ignored, the synthetic name
is generated and somehow

```
org.apache.hadoop.fs.s3a.AWSBadRequestException: Select:
SELECT * FROM S3OBJECT s WHERE s._1 = "true" on test/testSelectOddLines.csv:
  com.amazonaws.services.s3.model.AmazonS3Exception:
  The column index at line 1, column 39 is invalid.
  Please check the service documentation and try again.
  (Service: Amazon S3; Status Code: 400;
  Error Code: InvalidColumnIndex;
```

Here it is because strings must be single quoted, not double quoted. 

```sql
SELECT * FROM S3OBJECT s WHERE s._1 = "true"
                                      ^ HERE
```

S3S select uses double quotes to wrap column names, interprets the string
as column "true", and fails.

### AWSBadRequestException `Invalid Path component`

Your SQL is wrong.

```
org.apache.hadoop.fs.s3a.AWSBadRequestException:
Select: SELECT * FROM S3OBJECT s WHERE s.'odd' is "true" on test/testSelectOddLines.csv
: com.amazonaws.services.s3.model.AmazonS3Exception: Invalid Path component,
  expecting either an IDENTIFIER or STAR, got: LITERAL,at line 1, column 34.
  (Service: Amazon S3; Status Code: 400; Error Code: ParseInvalidPathComponent;

```

```
SELECT * FROM S3OBJECT s WHERE s.'odd' is "true" on test/testSelectOddLines.csv
                                 ^ HERE
```


### AWSBadRequestException  `ParseExpectedTypeName`

Your SQL is still wrong.

```

org.apache.hadoop.fs.s3a.AWSBadRequestException:
 Select: SELECT * FROM S3OBJECT s WHERE s.odd = "true"
on test/testSelectOddLines.csv:
com.amazonaws.services.s3.model.AmazonS3Exception
: Expected type name, found QUOTED_IDENTIFIER:'true' at line 1, column 41.
(Service: Amazon S3; Status Code: 400; Error Code: ParseExpectedTypeName;
```

### `ParseUnexpectedToken`

SQL error

```
org.apache.hadoop.fs.s3a.AWSBadRequestException:
Select: SELECT * FROM S3OBJECT s WHERE s.5 = `true` on test/testSelectOddLines.csv:
com.amazonaws.services.s3.model.AmazonS3Exception:
Unexpected token found LITERAL:5d-1 at line 1, column 33.
(Service: Amazon S3; Status Code: 400; Error Code: ParseUnexpectedToken;
```


### `MissingHeaders`


```
org.apache.hadoop.fs.s3a.AWSBadRequestException:
Select: SELECT * FROM S3OBJECT s WHERE s."odd" = `true` on test/testSelectOddLines.csv:
com.amazonaws.services.s3.model.AmazonS3Exception:
Some headers in the query are missing from the file.
Please check the file and try again.
(Service: Amazon S3; Status Code: 400; Error Code: MissingHeaders;
```

1. There's a header used in the query which doesn't match any in the document
itself.
1. The header option for the select query is set to "none" or "ignore", and
you are trying to use a header named there.

This can happen if you are trying to use double quotes for constants in the
SQL expression.  

```
SELECT * FROM S3OBJECT s WHERE s."odd" = "true" on test/testSelectOddLines.csv:
                                         ^ HERE   
```

Double quotes (") may only be used when naming columns; for constants 
single quotes are required.


### Method not allowed


```
org.apache.hadoop.fs.s3a.AWSS3IOException: Select on test/testSelectWholeFile:
com.amazonaws.services.s3.model.AmazonS3Exception: The specified method is not
allowed against this resource. (Service: Amazon S3; Status Code: 405;
rror Code: MethodNotAllowed;

at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:265)
at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:111)
at org.apache.hadoop.fs.s3a.Invoker.lambda$retry$3(Invoker.java:260)
at org.apache.hadoop.fs.s3a.Invoker.retryUntranslated(Invoker.java:317)
at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:256)
at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:231)
at org.apache.hadoop.fs.s3a.S3AFileSystem.select(S3AFileSystem.java:3400)
at org.apache.hadoop.fs.s3a.S3AFileSystem.select(S3AFileSystem.java:768)
at org.apache.hadoop.fs.s3a.select.ITestSelect.testSelectWholeFile(ITestSelect.java:168)
at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
at java.lang.reflect.Method.invoke(Method.java:498)
at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:47)
at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:44)
at org.junit.internal.runners.statements.InvokeMethod.evaluate(InvokeMethod.java:17)
at org.junit.internal.runners.statements.RunBefores.evaluate(RunBefores.java:26)
at org.junit.internal.runners.statements.RunAfters.evaluate(RunAfters.java:27)
at org.junit.rules.TestWatcher$1.evaluate(TestWatcher.java:55)
at org.junit.internal.runners.statements.FailOnTimeout$StatementThread.run(FailOnTimeout.java:74)
```


Happens when you are trying to read data which for some reason you can't
(e.g encrypted and encryption is not supported).

### AWSBadRequestException "InvalidTextEncoding"

The file couldn't be parsed. This can happen if you try to read a `.gz` file
and forget to set the compression in the select request.

That can be done through the `fs.s3a.select.compression` option.

```
org.apache.hadoop.fs.s3a.AWSBadRequestException: 
  Select: '" SELECT * FROM S3OBJECT s WHERE endstation_name = 'Bayswater Road: Hyde Park' "
  on s3a://example/dataset.csv.gz:
  com.amazonaws.services.s3.model.AmazonS3Exception:
   UTF-8 encoding is required. The text encoding error was found near byte 8,192.
    (Service: Amazon S3; Status Code: 400; Error Code: InvalidTextEncoding
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:212)
  at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:111)
  at org.apache.hadoop.fs.s3a.Invoker.lambda$retry$3(Invoker.java:260)
  at org.apache.hadoop.fs.s3a.Invoker.retryUntranslated(Invoker.java:317)
  at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:256)
  at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:231)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.select(S3AFileSystem.java:3420)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.select(S3AFileSystem.java:3381)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.open(S3AFileSystem.java:709)
  at org.apache.hadoop.fs.FileSystem.open(FileSystem.java:950)
  at org.apache.hadoop.fs.s3a.select.SelectTool.run(SelectTool.java:153)
  at org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.run(S3GuardTool.java:351)
  at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:76)
  at org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.run(S3GuardTool.java:1483)
  at org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.main(S3GuardTool.java:1492)
```

### AWSBadRequestException  `InvalidCompressionFormat` "GZIP is not applicable to the queried object"

A SELECT call has been made using a compression which doesn't match that of the 
source object, such as it being a plain text file.

```
org.apache.hadoop.fs.s3a.AWSBadRequestException: Select:
 '" SELECT * FROM S3OBJECT s WHERE endstation_name = 'Bayswater Road: Hyde Park' "
  on s3a://example/dataset.csv:
   com.amazonaws.services.s3.model.AmazonS3Exception:
    GZIP is not applicable to the queried object. Please correct the request and try again.
     (Service: Amazon S3; Status Code: 400; Error Code: InvalidCompressionFormat;
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:212)
  at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:111)
  at org.apache.hadoop.fs.s3a.Invoker.lambda$retry$3(Invoker.java:260)
  at org.apache.hadoop.fs.s3a.Invoker.retryUntranslated(Invoker.java:317)
  at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:256)
  at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:231)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.select(S3AFileSystem.java:3421)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.select(S3AFileSystem.java:3382)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.open(S3AFileSystem.java:709)
  at org.apache.hadoop.fs.FileSystem.open(FileSystem.java:950)
  at org.apache.hadoop.fs.s3a.select.SelectTool.run(SelectTool.java:167)
  at org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.run(S3GuardTool.java:351)
  at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:76)
  at org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.run(S3GuardTool.java:1483)
  at org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.main(S3GuardTool.java:1492)
Caused by: com.amazonaws.services.s3.model.AmazonS3Exception: GZIP is not applicable to the queried object.
 Please correct the request and try again.
  Service: Amazon S3; Status Code: 400; Error Code: InvalidCompressionFormat; 
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1639)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1304)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1056)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:743)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:717)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:699)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:667)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:649)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:513)
  at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:4280)
  at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:4227)
  at com.amazonaws.services.s3.AmazonS3Client.selectObjectContent(AmazonS3Client.java:779)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.lambda$select$17(S3AFileSystem.java:3425)
  at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:109)
```
