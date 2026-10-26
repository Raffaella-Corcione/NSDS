# Evaluation lab - Akka

## Group number: 24

## Group members

- Capaccioni Chiara
- Corcione Raffaella
- Cusano Virgilio

## Description of message flows
### Client behaviour
At the start of the system, the client receives a setup message (`ClientSetupMsg`) containing the reference to the balancer.
The client executes the ask pattern on get requests, receiving either a `TimeoutMsg` or a `ReplyMsg` containing the value.
The client sends asynchronous put messages (`PutMsg`).

### Balancer behaviour
At the start of the system, the balancer receives a setup message (`BalancerSetupMsg`) containing the reference to the two workers.
The put requests are forwarded to the two workers according to the name. Get messages are forwarded to the workers with the ask pattern with a timeout of 1 second.
If the ask to one of the workers returns a reply, that is forwarded to the client. Otherwise, the balancer sends a timeout message to the client.

### Worker Behaviour
Workers start in normal behaviour. They receive `PutPrimaryMsg` and `PutReplicaMsg` messages from the balancer
to insert new values into the address book. When a query (`GetMsg`) is received,
the worker searches for the value first in the primary and then in the replica copy and a `ReplyMsg`
is returned to the balancer. If the value is not found, the reply is empty.
A `RestMsg` changes context to resting behaviour, in which
all messages are silently dropped, except for `WakeupMsg` that puts the worker in normal behaviour again.
