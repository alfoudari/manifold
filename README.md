Manifold package.

# AWS S3

Collect data and stream them to an S3 bucket. This stream is fault tolerant and can survive restarts as data is stored locally and then uploaded.

There are two main processes involved:

1. **Collector**
   
    Receive incoming data and store it in the local file system.

2. **Uploader**

    Scan local file system and upload to an S3 bucket.
