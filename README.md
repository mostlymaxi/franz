# Franz-with-Benefits
A simple and friendlier alternative to Apache Kafka


### Usage
```doc
franz --path /path/to/store/data
```

### Protocol
The majority of the protocol is newline delimited and the order of messages is more important

1. the kind of client (num)
2. topic name

```doc
0\ntest\n
^   ^
|   |
|   topic name
|
client kind
```

client kind is defined as a number:
0 => producer
1 => consumer

### Example
spin up a franz instance
```doc
franz --path /tmp/franz-test
```

in another terminal connect to the instance with netcat
```doc
nc localhost 8085
```

make a producer client by sending a "0"
```doc
0
```

select and create a topic by sending a "test_topic"
```doc
test_topic
```

send some messages
```doc
hello
world
msg3
```

