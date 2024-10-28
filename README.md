# Franz
A simple and friendlier persistent message-queue / pub-sub built for speed (blazingly fast)!

The goal of Franz is to be able to handle millions of messages per second with a light memory footprint (it is however, rough on the disk).


### Usage
```doc
franz --path /path/to/store/data
```

see ```franz --help``` for more details.

### Protocol
[message length : u32]([key]=[value] : utf8)

#### mandatory keys
- version
- topic
- api

#### example key-values
 ```version=1,topic=test_topic_name,api=produce```

### Example
spin up a franz instance
```doc
franz --path /tmp/franz-test
```

in another terminal connect to the instance with netcat
```doc
echo -ne "\x00\x00\x00\x20version=1,topic=test,api=produce" | nc localhost 8085
```
send some (newline delimited) messages
```doc
hello
world
msg3
```

