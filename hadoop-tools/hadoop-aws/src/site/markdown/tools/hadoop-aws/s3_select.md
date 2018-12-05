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
data from S3, especially with larger CSV and JSON datasets.

Apache Hadoop's S3A Client has experimental support for this feature, with the
following warnings:

* The filtering is being done in S3 itself. If the source files cannot be parsed,
that's not something which can be fixed in Hadoop or layers above.
* It is unlikely to be supported by third party S3 implementations.
* The performance characteristics of this feature are untested.
* Performance *appears* best when the selection restricts the number of fields,
and projected columns: the less data returned, the faster the response.
* High-level support in tools such as Apache Hive and Spark will also be
evolving. Nobody has ever written CSV connectors with predicate pushdown before.


## Currently Implemented Features

* CSV input with/without compression.
* CSV output.

## Currently Unsupported

* JSON files.
* Structured file formats like Apache Parquet. Use Apache Spark and Apache
Hive for high-performance query engines using the latest ASF-supported
artifacts.


## Enabling/Disabling S3 Select

S3 Select is enabled by default

```xml
<property>
  <name>fs.s3a.select.enabled</name>
  <value>true</value>
  <description>Is S3 Select enabled?</description>
</property>
```

To disable it: clear the option `fs.s3a.select.enabled`.

To probe to see if a FileSystem instance implements it,
`S3AFileSystem.hasCapability("s3a:s3-select")` will be true iff
the version of S3A is aware of S3 Select, and it is enabled.

## Using S3 Select in applications

Use the new `FileSystem.openFile(path)` command to get a builder, and
set the mandatory s3 select options though `must()` operations.


```java
FileSystem.FSDataInputStreamBuilder builder =
    filesystem.openFile("s3a://bucket/path-to-file.csv")
        .must("s3a:select.sql",
            "SELECT * FROM S3OBJECT s WHERE s.\"odd\" = `TRUE`")
        .must("s3a:fs.s3a.select.compression", "none")
        .must("s3a:fs.s3a.select.input.csv.header", "use");
CompletableFuture<FSDataInputStream> future = builder.build();
try (FSDataInputStream select = future.get()) {
  // process the output
  stream.read();
}
```

When the Builder's `build()` call is made, if the filesystem
does not recognize these options it will fail. The S3A connector
does recognize them, and will generate a select request.

The `build()` call returns a `CompletableFuture<FSDataInputStream>`.
This future retrieves the result of the select call, which is executed
asynchronously in the S3A FileSystem instance's executor pool.

Errors in the SQL, missing file, permission failures and suchlike
will surface when the future is evaluated, *not the build call*.

In the returned stream, `seek()` calls, other than to the current position
that is, stream.seek(stream.getPos())`, will fail, as will positioned
read operations when the offset of the read is not the current position.

*It is not possible to seek within a stream returned by a SELECT query*.


## S3 Select configuration options.

Consult the javadocs for `org.apache.hadoop.fs.s3a.select.SelectConstants`.

The listed options can be set in `core-site.xml`, supported by s3a per-bucket
configuration, and can be set programmatically on the `Configuration` object
use to configure a new filesystem instance.

Any of these options can be set in the builder returned by the `openFile()` call
—simply prefix them with `s3a:`.

```xml

<property>
  <name>fs.s3a.select.input.csv.comment.marker</name>
  <value>#</value>
  <description>In S3 Select queries: the marker for comment lines in CSV files</description>
</property>

<property>
  <name>fs.s3a.select.input.csv.record.delimiter</name>
  <value>\n</value>
  <description>In S3 Select queries over CSV files: the record delimiter.
    \t is remapped to the TAB character, \r to CR \n to newline. \\ to \
    and \" to "
  </description>
</property>

<property>
  <name>fs.s3a.select.input.csv.field.delimiter</name>
  <value>,</value>
  <description>In S3 Select queries over CSV files: the field delimiter.
    \t is remapped to the TAB character, \r to CR \n to newline. \\ to \
    and \" to "
  </description>
</property>

<property>
  <name>fs.s3a.select.input.csv.quote.character</name>
  <value>"</value>
  <description>In S3 Select queries over CSV files: quote character.
    \t is remapped to the TAB character, \r to CR \n to newline. \\ to \
    and \" to "
  </description>
</property>

<property>
  <name>fs.s3a.select.input.csv.quote.escape.character</name>
  <value></value>
  <description>In S3 Select queries over CSV files: quote escape character.
    \t is remapped to the TAB character, \r to CR \n to newline. \\ to \
    and \" to "
  </description>
</property>

<property>
  <name>fs.s3a.select.input.csv.header</name>
  <value>none</value>
  <description>In S3 Select queries over CSV files: what is the role of the header? One of "none", "ignore" and "use"</description>
</property>

<property>
  <name>fs.s3a.select.input.compression</name>
  <value></value>
  <description>In S3 Select queries, the source compression
    algorithm. One of: "none" and "gzip"</description>
</property>
```

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

### Probing for support of S3 Select.


S3AFileSystem implements `org.apache.hadoop.fs.StreamCapabilities.StreamCapability`;
the `hasCapability()` call can be used to probe for it being supported on
the specific Hadoop version and S3 filesystem instance.

1. If the FileSystem cannot be cast to `StreamCapability`, the API is not supported.
1. The call `hasCapability("s3a:s3-select")` on the cast object must returns
true.

If either of these conditions is not met, the filesystem does not support
the feature: the `build()` call will raise an exception.

In Java, this can be tested as follows:

```java
boolean isSelectAvailable(final FileSystem filesystem) {
  return filesystem instanceof StreamCapabilities
      && ((StreamCapabilities) filesystem).hasCapability("s3a:select.sql");
}
```

### Consistency, Concurrency and Error handling

**Consistency**

* Assume the usual S3 create consistency model applies: a recently updated
file may not be visible

* When enabled, S3Guard's DynamoDB table will declare whether or not
a newly deleted file is visible: if it is marked as deleted, the
select request will be rejected with a `FileNotFoundException`.

* When an existing S3-hosted object is changed, the S3 select operation
may return the results of a SELECT call as applied to either the old
or new version.

* We don't know whether you can get partially consistent reads, or whether
an extended read ever picks up a later value.

* The AWS S3 load balancers are known to (briefly) cache 404/Not-Found entries
from a failed HEAD/GET request against a nonexistent file; this cached
entry can briefly create create inconsistency, despite the
AWS "Create is consistent" model. There is no attempt to detect or recover from
this.

**Concurrency**

The outcome of what happens when source file is overwritten while the result of
a select call is overwritten is undefined.

The input stream returned by the operation is *NOT THREAD SAFE*.

**Error Handling**

If an attempt to issue an S3 select call fails, the S3A connector will
reissue the request if and only if it believes a retry may succeed.
That is: it considers the operation to be idempotent and if the failure is
considered to be a recoverable connectivity problem or a server-side rejection
which can be retried (500, 503).

If an attempt to read data from an S3 select stream (`org.apache.hadoop.fs.s3a.select.SelectInputStream)` fails partway through the read, *no attempt is made to retry the operation*

In contrast, normal S3 stream reads try to recover from (possibly transient)
failures.


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

Example: read the first 100 rows of the landsat dataset where cloud cover is zero:

```
hadoop s3guard select -header use -compression gzip -limit 100  \
  s3a://landsat-pds/scene_list.gz \
  "SELECT s.entityId FROM S3OBJECT s WHERE s.cloudCover = '0.0'"
```

Return the `entityId` column for all rows in the dataset where the cloud
cover was 0.0., and save it to the file `output.csv`:

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

Set `s3a:fs.s3a.select.input.compression` to `gzip`, rather than `none`.

## Performance

The Select operation is best when the least amount of data is returned by
the query, as this reduces the amount of data downloaded.

* Limit the number of columns projected to only those needed.
* Use `LIMIT` to set an upper limit on the rows read, rather than implementing
a row counter in application code and closing the stream when reached.
This avoids having to abort the HTTPS connection and negotiate a new one
on the next S3 request.

The select call itself can be slow, especially when the source is a multi-MB
compressed file with aggressive filtering in the `WHERE` clause.
Assumption: the select query starts at row 1 and scans through each row,
and does not return data until it has matched one or more rows.

If the asynchronous nature of the `openFile().build().get()` sequence
can be taken advantage of, by performing other work before or in parallel
to the `get()` call: do it.

## Troubleshooting

The exceptions here are all based on the experience during writing tests;
more may surface with broader use.

The failures listed here are all considered unrecoverable and will not be
reattempted.

As parse-time errors always state the line and column of an error, you can
simplify debugging by breaking a SQL statement across lines, e.g.

```java
String sql = "SELECT\n"
    + "s.entityId \n"
    + "FROM " + "S3OBJECT s WHERE\n"
    + "s.\"cloudCover\" = '100.0'\n"
    + " LIMIT 100";
```
Now if the error is declared as "line 4", it will be on the select conditions;
the column offset will begin from the first character on that row.


### AWSBadRequestException `InvalidColumnIndex`


Your SQL is wrong and the element at fault is considered an unknown column
name.

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

Another example:

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

S3 select uses double quotes to wrap column names, interprets the string
as column "true", and fails with a non-intuitive message.

*Tip*: look for the element at fault and treat the `InvalidColumnIndex`
message as a parse-time message, rather than the definitive root
cause of the problem.

### AWSBadRequestException `ParseInvalidPathComponent`

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

Your SQL is broken.

```
org.apache.hadoop.fs.s3a.AWSBadRequestException:
Select: SELECT * FROM S3OBJECT s WHERE s.5 = `true` on test/testSelectOddLines.csv:
com.amazonaws.services.s3.model.AmazonS3Exception:
Unexpected token found LITERAL:5d-1 at line 1, column 33.
(Service: Amazon S3; Status Code: 400; Error Code: ParseUnexpectedToken;
```
### `ParseUnexpectedOperator`

Your SQL is broken.

```
com.amazonaws.services.s3.model.AmazonS3Exception: Unexpected operator OPERATOR:'%' at line 1, column 45.
(Service: Amazon S3; Status Code: 400;
Error Code: ParseUnexpectedOperator; Request ID: E87F30C57436B459;
S3 Extended Request ID: UBFOIgkQxBBL+bcBFPaZaPBsjdnd8NRz3NFWAgcctqm3n6f7ib9FMOpR+Eu1Cy6cNMYHCpJbYEY
 =:ParseUnexpectedOperator: Unexpected operator OPERATOR:'%' at line 1, column 45.
at java.util.concurrent.CompletableFuture.reportGet(CompletableFuture.java:357)
at java.util.concurrent.CompletableFuture.get(CompletableFuture.java:1895)
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

```

Happens when you are trying to use S3 Select to read data which for some reason
you are not allowed to.

### AWSBadRequestException `InvalidTextEncoding`

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
...
Caused by: com.amazonaws.services.s3.model.AmazonS3Exception: GZIP is not applicable to the queried object.
 Please correct the request and try again.
  Service: Amazon S3; Status Code: 400; Error Code: InvalidCompressionFormat;
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse
  ...
```


### PathIOException: seek() not supported

The input stream returned by the select call does not support seeking
to any location other than the current one.
Similarly, `PositionedReadable` operations which attempt to read at any offset
other than that of `getPos()` will be rejected.

```
org.apache.hadoop.fs.PathIOException: `s3a://landsat-pds/landsat.csv.gz': seek() not supported

  at org.apache.hadoop.fs.s3a.select.SelectInputStream.unsupported(SelectInputStream.java:254)
  at org.apache.hadoop.fs.s3a.select.SelectInputStream.seek(SelectInputStream.java:243)
  at org.apache.hadoop.fs.FSDataInputStream.seek(FSDataInputStream.java:66)
```

There is no fix for this. You can move forward in a file using `skip(offset)`;
bear in mind that the return value indicates what offset was skipped -it
may be less than expected.

### `IllegalArgumentException`: "Unknown mandatory key s3a:select.sql"

The file options to tune an S3 select call are only valid when a SQL expression
is set in the "s3a:select.sql". If not, any such option added as a `must()` value
will fail.

```
java.lang.IllegalArgumentException: Unknown mandatory key s3a:fs.s3a.select.csv.header
at com.google.common.base.Preconditions.checkArgument(Preconditions.java:88)
at org.apache.hadoop.fs.AbstractFSBuilder.lambda$rejectUnknownMandatoryKeys$0(AbstractFSBuilder.java:331)
at java.lang.Iterable.forEach(Iterable.java:75)
at java.util.Collections$UnmodifiableCollection.forEach(Collections.java:1080)
at org.apache.hadoop.fs.AbstractFSBuilder.rejectUnknownMandatoryKeys(AbstractFSBuilder.java:330)
at org.apache.hadoop.fs.s3a.S3AFileSystem.openFileWithOptions(S3AFileSystem.java:3541)
at org.apache.hadoop.fs.FileSystem$FSDataInputStreamBuilder.build(FileSystem.java:4442)
```

Requiring these options without providing a SQL query is invariably an error.
Fix: add the SQL statement, or use `opt()` calls to set the option.

### `IllegalArgumentException`: "Unknown mandatory key "s3a:select.sql"

The filesystem is not an S3A filesystem, and the s3a select option is not recognized.

```
java.lang.IllegalArgumentException: Unknown mandatory key "s3a:select.sql"
at com.google.common.base.Preconditions.checkArgument(Preconditions.java:88)
at org.apache.hadoop.fs.AbstractFSBuilder.lambda$rejectUnknownMandatoryKeys$0(AbstractFSBuilder.java:331)
at java.lang.Iterable.forEach(Iterable.java:75)
at java.util.Collections$UnmodifiableCollection.forEach(Collections.java:1080)
at org.apache.hadoop.fs.AbstractFSBuilder.rejectUnknownMandatoryKeys(AbstractFSBuilder.java:330)
at org.apache.hadoop.fs.filesystem.openFileWithOptions(FileSystem.java:3541)
at org.apache.hadoop.fs.FileSystem$FSDataInputStreamBuilder.build(FileSystem.java:4442)
```

* Verify that the URL has an "s3a:" prefix.
* If it does, there may be a non-standard S3A implementation, or some
a filtering/relaying class has been placed in front of the S3AFilesystem.

### `IllegalArgumentException`: Unknown mandatory key "s3a:fs.s3a.select.csv.header"

The file options to tune an S3 select call are only valid when a SQL expression
is set in the "s3a:select.sql". If not, any such option added as a `must()` value
will fail.

```
java.lang.IllegalArgumentException: Unknown mandatory key "s3a:fs.s3a.select.csv.header"
at com.google.common.base.Preconditions.checkArgument(Preconditions.java:88)
at org.apache.hadoop.fs.AbstractFSBuilder.lambda$rejectUnknownMandatoryKeys$0(AbstractFSBuilder.java:331)
at java.lang.Iterable.forEach(Iterable.java:75)
at java.util.Collections$UnmodifiableCollection.forEach(Collections.java:1080)
at org.apache.hadoop.fs.AbstractFSBuilder.rejectUnknownMandatoryKeys(AbstractFSBuilder.java:330)
at org.apache.hadoop.fs.s3a.S3AFileSystem.openFileWithOptions(S3AFileSystem.java:3541)
at org.apache.hadoop.fs.FileSystem$FSDataInputStreamBuilder.build(FileSystem.java:4442)
```

Requiring these options without providing a SQL query is invariably an error.
Fix: add the SQL statement, or use `opt()` calls to set the option.

If the "s3a:select.sql" option is set, and still a key is rejected, then
either the spelling of the key is wrong, it has leading or trailing spaces,
or it is an option not supported in that specific release of Hadoop.
