# Pipelining

Redis [pipelining](https://redis.io/topics/pipelining) is a way to reduce round trip as well as socket I/O costs on server and client side.

This is done by sending more than one request (command) to the server at once instead of reading the response immediately after sending a command.

This article is going to discuss two options using pipelining supported by this client.

## [TODO]
* asynchrounous nature of RESP3 and the client (implicit pipelining)
* single connection performance / throughput (no connection pooling) 
* example benchmark

### Request - Response
[TODO]

### Explicit Pipelining
[TODO]

### Implicit Pipelining
[TODO]

### When using Explicit Pipelining
[TODO]
