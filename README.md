[![<abstractpaper>](https://circleci.com/gh/abstractpaper/manifold.svg?style=svg)](https://circleci.com/gh/abstractpaper/manifold)

Manifold is a tool that can be useful for streaming data across systems/system components, particularly real time systems.

It currently supports the following interfaces:
- AWS Kinesis
- AWS S3
- RabbitMQ
- Stdio
- WebSocket connections

To be implemented:
- Apache Kafka

Manifold is opinionated and biased towards being fault tolerant. Many things can go wrong in production systems and having a self-heal feature is vital in particular where data collection is happening and you want to minimize any collection loss/gap.

# Arguments

For all the interfaces listed below, there are two types of arguments that can be specified:
* Struct members: these are direct members of a struct (first letter is capitalized).
* KV Arguments: this is a dictionary or a map (first letter is small).

Let's take an example for AWS S3 struct:

```go
type S3 struct {
	Region     string
	BucketName string
	Config     *S3Config
	Args       map[string]string
	Sess       *session.Session
	buffer     *buffer
}
```

All the above (exported) attributes can be set directly, however `Args` is added for further flexibility:

```go
Args: map[string]string{
    "bufferPath": "/tmp/other/path",
}
```

# Illustration

![Manifold Illustration](/docs/manifold_illustration.png)

The yellow boxes are manifold processes that stream data between their connected systems.

# AWS Kinesis

Stream data from/to an AWS Kinesis stream.

### Consumer

You can find a full consumer example [here](./examples/kinesis-consumer/main.go).

### Producer

You can find a full producer example [here](./examples/kinesis-producer/main.go).

# AWS S3

Collect and stream data to an S3 bucket. This stream is fault tolerant and can survive restarts as data is stored locally and then uploaded.

KV Arguments:
* `bufferPath` is the path to store files in the local file system. Defaults to `/tmp/manifold/aws_s3/`.

There are two main (independent) processes involved:

1. **Collector**
   
    Receive incoming data and store it in a buffer in the local file system.

    There are two arguments that can be configured for collector:
    * `CommitFileSize` commits the active buffer if its size reaches to `CommitFileSize` KB. 
      It first copies the buffer to a new file named with the current timestmap and then clears the buffer.
    * `CommitDuration` commits the active buffer if the elapsed duration since the last commit
      reaches `CommitDuration` minutes.

    Collector watches for its two arguments and commits as soon as on of them is true.

2. **Uploader**

    Scan local file system and upload to an S3 bucket.

    Arguments:
    * `UploadEvery` uploads the delta of the local file system and S3 bucket every `UploadEvery` period is passed.

Example:

```go
dest := stream.S3{
    Region:     "us-east-1",
    BucketName: "logs",
    Sess:       aws_sess,
    Config: &stream.S3Config{
        Folder:         "orders/failed",
        CommitFileSize: 1024, // KB
        CommitDuration: 5,    // Minutes
        UploadEvery:    10,   // Seconds
    },
}
```


# RabbitMQ

Stream data from/to RabbitMQ.

### Consumer

Example:

```go
dest := stream.RabbitMQ{
    URL: fmt.Sprintf("amqp://username:password@%s/%s", rabbitMQEndPoint, rabbitMQVHost),
    Header: nil,
    Args: map[string]string{
        "exchange": "logs",
        "key":      "webserver_errors",
    },
}
```

### Producer

Example:

```go
src := stream.RabbitMQ{
    URL: fmt.Sprintf("amqp://username:password@%s/%s", rabbitMQEndPoint, rabbitMQVHost),
    Header: nil,
    Args: map[string]string{
        "queue":    "webserver_errors_to_s3",
        "consumer": "logs-archiver",
    },
}
```


# WebSocket

Connect to any websocket connection with the following aspects considered:

* You can specify `reconnect_every` to swap the connection every time this period passes.
* If the server side closes the connection for any reason then a new connection is made, this tackles unexpected adhoc closure. If the new connection cannot be made, the process exits.

### Consumer

Example:

```go
src := stream.WebSocket{
    URL:    "wss://stream.universe.com:9999",
    Header: http.Header{"APIKEY": []string{kwargs["apiKey"]}},
    Args: map[string]string{
        "reconnect_every": strconv.Itoa(int(12 * time.Hour)),
    },
}
```