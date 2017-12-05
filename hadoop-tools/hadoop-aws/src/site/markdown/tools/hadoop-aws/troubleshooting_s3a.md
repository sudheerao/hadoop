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

# Troubleshooting S3A

Here are some lower level details and hints on troubleshooting and tuning
the S3A client.

## Logging at lower levels

The AWS SDK and the Apache HTTP components can be configured to log at
more detail, as can S3A itself.

```properties
log4j.logger.org.apache.hadoop.fs.s3a=DEBUG
log4j.logger.com.amazonaws.request=DEBUG
log4j.logger.org.apache.http=DEBUG
log4j.logger.org.apache.http.wire=ERROR
```

Be aware that logging HTTP headers may leak sensitive AWS account information,
so should not be shared.

## Advanced: network performance

An example of this is covered in [HADOOP-13871](https://issues.apache.org/jira/browse/HADOOP-13871).

1. For public data, use `curl`:

        curl -O https://landsat-pds.s3.amazonaws.com/scene_list.gz
1. Use `nettop` to monitor a processes connections.

Consider reducing the connection timeout of the s3a connection.

```xml
<property>
  <name>fs.s3a.connection.timeout</name>
  <value>15000</value>
</property>
```
<<<<<<< HEAD
This *may* cause the client to react faster to network pauses.
=======
This *may* cause the client to react faster to network pauses, so display
stack traces fast. At the same time, it may be less resilient to
connectivity problems.


## Other Issues

### <a name="logging"></a> Enabling low-level logging

The AWS SDK and the Apache S3 components can be configured to log at
more detail, as can S3A itself.

```properties
log4j.logger.org.apache.hadoop.fs.s3a=DEBUG
log4j.logger.com.amazonaws.request=DEBUG
log4j.logger.com.amazonaws.thirdparty.apache.http=DEBUG
```

If using the "unshaded" JAR, then the Apache HttpClient can be directly configured:

```properties
log4j.logger.org.apache.http=DEBUG
```


This produces a log such as this, wich is for a V4-authenticated PUT of a 0-byte file used
as an empty directory marker

```
execchain.MainClientExec (MainClientExec.java:execute(255)) - Executing request PUT /test/ HTTP/1.1
execchain.MainClientExec (MainClientExec.java:execute(266)) - Proxy auth state: UNCHALLENGED
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(135)) - http-outgoing-0 >> PUT /test/ HTTP/1.1
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> Host: ireland-new.s3-eu-west-1.amazonaws.com
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> x-amz-content-sha256: UNSIGNED-PAYLOAD
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> Authorization: AWS4-HMAC-SHA256 Credential=AKIAIYZ5JEEEER/20170904/eu-west-1/s3/aws4_request,  ...
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> X-Amz-Date: 20170904T172929Z
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> User-Agent: Hadoop 3.0.0-beta-1, aws-sdk-java/1.11.134 ...
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> amz-sdk-invocation-id: 75b530f8-ad31-1ad3-13db-9bd53666b30d
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> amz-sdk-retry: 0/0/500
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> Content-Type: application/octet-stream
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> Content-Length: 0
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> Connection: Keep-Alive
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "PUT /test/ HTTP/1.1[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "Host: ireland-new.s3-eu-west-1.amazonaws.com[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "x-amz-content-sha256: UNSIGNED-PAYLOAD[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "Authorization: AWS4-HMAC-SHA256 Credential=AKIAIYZ5JEEEER/20170904/eu-west-1/s3/aws4_request, ,,,
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "X-Amz-Date: 20170904T172929Z[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "User-Agent: 3.0.0-beta-1, aws-sdk-java/1.11.134  ...
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "amz-sdk-invocation-id: 75b530f8-ad31-1ad3-13db-9bd53666b30d[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "amz-sdk-retry: 0/0/500[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "Content-Type: application/octet-stream[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "Content-Length: 0[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "Connection: Keep-Alive[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 << "HTTP/1.1 200 OK[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 << "x-amz-id-2: mad9GqKztzlL0cdnCKAj9GJOAs+DUjbSC5jRkO7W1E7Nk2BUmFvt81bhSNPGdZmyyKqQI9i/B/A=[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 << "x-amz-request-id: C953D2FE4ABF5C51[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 << "Date: Mon, 04 Sep 2017 17:29:30 GMT[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 << "ETag: "d41d8cd98f00b204e9800998ecf8427e"[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 << "Content-Length: 0[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 << "Server: AmazonS3[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 << "[\r][\n]"
http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(124)) - http-outgoing-0 << HTTP/1.1 200 OK
http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(127)) - http-outgoing-0 << x-amz-id-2: mad9GqKztzlL0cdnCKAj9GJOAs+DUjbSC5jRkO7W1E7Nk2BUmFvt81bhSNPGdZmyyKqQI9i/B/A=
http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(127)) - http-outgoing-0 << x-amz-request-id: C953D2FE4ABF5C51
http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(127)) - http-outgoing-0 << Date: Mon, 04 Sep 2017 17:29:30 GMT
http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(127)) - http-outgoing-0 << ETag: "d41d8cd98f00b204e9800998ecf8427e"
http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(127)) - http-outgoing-0 << Content-Length: 0
http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(127)) - http-outgoing-0 << Server: AmazonS3
execchain.MainClientExec (MainClientExec.java:execute(284)) - Connection can be kept alive for 60000 MILLISECONDS
```


## <a name="retries"></a>  Reducing failures by configuring retry policy

The S3A client can ba configured to rety those operations which are considered
retriable. That can be because they are idempotent, or
because there failure happened before the request was processed by S3.

The number of retries and interval between each retry can be configured:

```xml
<property>
  <name>fs.s3a.attempts.maximum</name>
  <value>20</value>
  <description>How many times we should retry commands on transient errors,
  excluding throttling errors.</description>
</property>

<property>
  <name>fs.s3a.retry.interval</name>
  <value>500ms</value>
  <description>
    Interval between retry attempts.
  </description>
</property>
```

Not all failures are retried. Specifically excluded are those considered
unrecoverable:

* Low-level networking: `UnknownHostException`, `NoRouteToHostException`.
* 302 redirects
* Missing resources, 404/`FileNotFoundException`
* HTTP 416 response/`EOFException`. This can surface if the length of a file changes
  while another client is reading it.
* Failures during execution or result processing of non-idempotent operations where
it is considered likely that the operation has already taken place.

In future, others may be added to this list.

When one of these failures arises in the S3/S3A client, the retry mechanism
is bypassed and the operation will fail.

*Warning*: the S3A client considers DELETE, PUT and COPY operations to
be idempotent, and will retry them on failure. These are only really idempotent
if no other client is attempting to manipulate the same objects, such as:
renaming() the directory tree or uploading files to the same location.
Please don't do that. Given that the emulated directory rename and delete operations
aren't atomic, even without retries, multiple S3 clients working with the same
paths can interfere with each other

#### <a name="retries"></a> Throttling

When many requests are made of a specific S3 bucket (or shard inside it),
S3 will respond with a 503 "throttled" response.
Throttling can be recovered from, provided overall load decreases.
Furthermore, because it is sent before any changes are made to the object store,
is inherently idempotent. For this reason, the client will always attempt to
retry throttled requests.

The limit of the number of times a throttled request can be retried,
and the exponential interval increase between attempts, can be configured
independently of the other retry limits.

```xml
<property>
  <name>fs.s3a.retry.throttle.limit</name>
  <value>20</value>
  <description>
    Number of times to retry any throttled request.
  </description>
</property>

<property>
  <name>fs.s3a.retry.throttle.interval</name>
  <value>500ms</value>
  <description>
    Interval between retry attempts on throttled requests.
  </description>
</property>
```

If a client is failing due to `AWSServiceThrottledException` failures,
increasing the interval and limit *may* address this. However, it
it is a sign of AWS services being overloaded by the sheer number of clients
and rate of requests. Spreading data across different buckets, and/or using
a more balanced directory structure may be beneficial.
Consult [the AWS documentation](http://docs.aws.amazon.com/AmazonS3/latest/dev/request-rate-perf-considerations.html).

Reading or writing data encrypted with SSE-KMS forces S3 to make calls of
the AWS KMS Key Management Service, which comes with its own
[Request Rate Limits](http://docs.aws.amazon.com/kms/latest/developerguide/limits.html).
These default to 1200/second for an account, across all keys and all uses of
them, which, for S3 means: across all buckets with data encrypted with SSE-KMS.

###### Tips to Keep Throttling down

* If you are seeing a lot of throttling responses on a large scale
operation like a `distcp` copy, *reduce* the number of processes trying
to work with the bucket (for distcp: reduce the number of mappers with the
`-m` option).

* If you are reading or writing lists of files, if you can randomize
the list so they are not processed in a simple sorted order, you may
reduce load on a specific shard of S3 data, so potentially increase throughput.

* An S3 Bucket is throttled by requests coming from all
simultaneous clients. Different applications and jobs may interfere with
each other: consider that when troubleshooting.
Partitioning data into different buckets may help isolate load here.

* If you are using data encrypted with SSE-KMS, then the
will also apply: these are stricter than the S3 numbers.
If you believe that you are reaching these limits, you may be able to
get them increased.
Consult [the KMS Rate Limit documentation](http://docs.aws.amazon.com/kms/latest/developerguide/limits.html).

* S3Guard uses DynamoDB for directory and file lookups;
it is rate limited to the amount of (guaranteed) IO purchased for a
table. If significant throttling events/rate is observed here, the preallocated
IOPs can be increased with the `s3guard set-capacity` command, or
through the AWS Console. Throttling events in S3Guard are noted in logs, and
also in the S3A metrics `s3guard_metadatastore_throttle_rate` and
`s3guard_metadatastore_throttled`.
>>>>>>> de8b6ca5ef8... HADOOP-13786 Add S3A committer for zero-rename commits to S3 endpoints.
