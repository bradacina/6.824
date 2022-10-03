# questions
- what happens when a follower is split, runs a number of elections incrementing the term
than rejoins the network, and tries to get elected leader. it may now have the highest
term due to the number of restarted elections while it was in a split.


# run
`go test -run 2A`

# My personal notes on the test harness

## config

Config is a struct that contains a
- pretend Network with:
    - a channel on which all servers send messages
    - an array of servers
    - an array of client endpoints
    - an array of connections between client endpoints and servers
    - flag to track if the network is reliable
    - flag to track if the network has long delays on transmitting messages
    - flag to track if the network sends messages out of order

- an array of Rafts (which are the states of each server on the network)

- stats like how many messages and bytes have been sent over the network

# request

Request contains
- the name of the client endpoint sending the request
- the server and method that will be called
- arguments
- the reply channel on which the reply should be sent