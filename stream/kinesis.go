package stream

import (
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"

	log "github.com/sirupsen/logrus"
)

type Kinesis struct {
	ConsumerName 	string
	StreamARN    	string
	AWSSess      	*session.Session
	client			*kinesis.Kinesis
	consumer     	*kinesis.Consumer
	stream		 	*kinesis.SubscribeToShardEventStream
}

func (k *Kinesis) Connect() (err error) {
	// kinesis client
	k.client = kinesis.New(k.AWSSess)

	return
}

func (k *Kinesis) Disconnect() (err error) {
	if k.consumer != nil {
		log.Info("Deregistering consumer...")
		_, err = deregisterConsumer(k.client, k.ConsumerName, k.StreamARN)
		if err != nil {
			log.Error(err)
		}
	}

	return
}

func (k *Kinesis) Info() {
	log.Info("Kinesis.ConsumerName: ", k.ConsumerName)
}

func (k *Kinesis) Read() (channel chan string, err error) {
	// get a consumer
	k.consumer, err = getConsumer(k.client, k.ConsumerName, k.StreamARN)
	if err != nil {
		log.Fatalln("Error getting a consumer: ", err)
		return	
	}

	// subscribe
	log.Println("Subscribing to shard.")
	shardId := "shardId-000000000000"
	shardIteratorType := "LATEST"
	k.stream, err = shardSubscribe(k.client, k.consumer, shardId, shardIteratorType)
	if err != nil {
		log.Fatalln("Error subscribing to a shard: ", err)
	}

	// loop through stream and push messages into channel
	channel = make(chan string)
	go func() {
		log.Println("Looping over event stream...")
		for e := range k.stream.Reader.Events() {
			records := e.(*kinesis.SubscribeToShardEvent).Records

			for _, rec := range records {
				log.Trace(string(rec.Data))
				channel <- string(rec.Data)
			}
		}
	}()
	return
}

func (k *Kinesis) Write(message string, partitionKey string, streamName string) (err error) {
	record := kinesis.PutRecordInput{
		Data:         []byte(message),
		PartitionKey: &partitionKey,
		StreamName:   &streamName,
	}
	_, err = k.client.PutRecord(&record)
	if err != nil {
		log.Errorln("PutRecord failed: ", err)
	}

	return
}

// Return a consumer object
func getConsumer(svc *kinesis.Kinesis, consumerName string, awsKinesisStreamARN string) (consumer *kinesis.Consumer, err error) {
	tries := 1
	for {
		if tries >= 5 {
			break
		}

		// Try to get consumer details first
		log.Info("Getting consumer details.")
		var consumerDesc *kinesis.ConsumerDescription
		consumerDesc, err = describeConsumer(svc, consumerName, awsKinesisStreamARN)
		if err != nil {
			if _, ok := err.(*kinesis.ResourceNotFoundException); ok {
				log.Info("ResourceNotFound")

				// consumer not found, register it.
				log.Info("Registering consumer...")
				_, err := registerConsumer(svc, consumerName, awsKinesisStreamARN)
				if err != nil {
					log.Error(err)
					return nil, err
				}

				// getting created consumer information
				consumerDesc, err = describeConsumer(svc, consumerName, awsKinesisStreamARN)
				if err != nil {
					log.Error("Error creating a consumer: ", err)
					return nil, err
				}
			} else {
				log.Error("describeConsumer: ", err)
				return nil, err
			}
		
		}
		log.Info(consumerDesc)

		// copy ConsumerDecsription -> Consumer
		consumer = &kinesis.Consumer{
			ConsumerARN:               consumerDesc.ConsumerARN,
			ConsumerCreationTimestamp: consumerDesc.ConsumerCreationTimestamp,
			ConsumerName:              consumerDesc.ConsumerName,
			ConsumerStatus:            consumerDesc.ConsumerStatus,
		}

		// if ACTIVE then return the consumer
		// if CREATING or DELETING then wait 5 times for 5 seconds each
		// else register a new consumer
		switch *consumer.ConsumerStatus {
		case kinesis.ConsumerStatusActive:
			log.Info("Consumer is ACTIVE, returning object.")
			return consumer, err
		case kinesis.ConsumerStatusCreating, kinesis.ConsumerStatusDeleting:
			log.Info("Consumer is ", *consumer.ConsumerStatus)
			log.Info("Retrying in 5 seconds...")
			time.Sleep(5 * time.Second)
			tries++
			continue
		}

		log.Info("Registering consumer...")
		consumer, err := registerConsumer(svc, consumerName, awsKinesisStreamARN)
		if err != nil {
			log.Error(err)
			return nil, err
		}
		log.Info(consumer)
	}

	return
}

// Describe a consumer of Kinesis Data Stream.
func describeConsumer(svc *kinesis.Kinesis, consumerName string, awsKinesisStreamARN string) (consumer *kinesis.ConsumerDescription, err error) {
	describeInput := kinesis.DescribeStreamConsumerInput{
		ConsumerName: &consumerName,
		StreamARN:    &awsKinesisStreamARN,
	}

	out, err := svc.DescribeStreamConsumer(&describeInput)
	if err != nil {
		log.Warn(err)
	}
	consumer = out.ConsumerDescription

	return
}

// Register a consumer on a Kinesis Data Stream.
func registerConsumer(svc *kinesis.Kinesis, consumerName string, awsKinesisStreamARN string) (consumer *kinesis.Consumer, err error) {
	registerInput := kinesis.RegisterStreamConsumerInput{
		ConsumerName: &consumerName,
		StreamARN:    &awsKinesisStreamARN,
	}
	out, err := svc.RegisterStreamConsumer(&registerInput)
	if err != nil {
		log.Error(err)
	}
	consumer = out.Consumer

	return
}

// Deregister a consumer on a Kinesis Data Stream.
func deregisterConsumer(svc *kinesis.Kinesis, consumerName string, awsKinesisStreamARN string) (out string, err error) {
	deregisterInput := kinesis.DeregisterStreamConsumerInput{
		ConsumerName: &consumerName,
		StreamARN:    &awsKinesisStreamARN,
	}

	resp, err := svc.DeregisterStreamConsumer(&deregisterInput)
	if err != nil {
		return
	}
	out = resp.GoString()

	return
}


// Subscribe to a shard on a Kinesis Data Stream.
func shardSubscribe(svc *kinesis.Kinesis, consumer *kinesis.Consumer, shardId string, shardIteratorType string) (eventStream *kinesis.SubscribeToShardEventStream, err error) {
	subscribeInput := kinesis.SubscribeToShardInput{
		ConsumerARN: consumer.ConsumerARN,
		ShardId:     &shardId,
		StartingPosition: &kinesis.StartingPosition{
			Type: &shardIteratorType,
		},
	}
	// SubscribeToShard
	out, err := svc.SubscribeToShard(&subscribeInput)
	if err != nil {
		log.Error(err)
	}
	eventStream = out.EventStream

	return
}
