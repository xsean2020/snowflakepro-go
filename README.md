snowflakepro
====
snowflakepro is a plus version with 128 bits  
  
### ID Format

```
A SFID is a 16 byte snowflake  Sortable Identifier
The components are encoded as 16 octets.
Each component is encoded with the MSB first (network byte order).
0                   1                   2                   3
0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      32_bit_uint_time_high                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     16_bit_uint_time_low      |       16_bit_uint_node_id     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       32_bit_uint_random                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     8_bit_uint_random |          24_bit_uint_sn               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```



## Getting Started

### Installing

This assumes you already have a working Go environment, if not please see
[this page](https://golang.org/doc/install) first.

```sh
go get github.com/xsean2020/snowflakepro-go 
```

### Usage

Import the package into your project then construct a new snowflake Node using a
unique node number and a random number in [0, 2^40-1]. 
With the  object call the Next() method to 
generate and return a unique snowflake ID. 


**Example Program:**

```go

	var s, _ = NewSnowflakePro(100, uint64(rand.Int63n(int64(MaxNonce))))
	id := s.Next()
	if id.Time() != s.tms {
		log.Fatal("time error")
	}

	if id.Nonce() != s.nonce {
		log.Fatal("nonce error", id.Nonce(), s.nonce)
	}

	if id.Node() != s.node {
		log.Fatal("node error")
	}

	if id.SN() != s.sn {
		log.Fatal("sn error")
	}

```


