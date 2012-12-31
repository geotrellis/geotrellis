---
layout: gettingstarted
title: Parallel and Distributed Execution

tutorial: gettingstarted
num: 4
---

## Parallel and distributed execution

#### Parallel Execution
Most operations have "child operations" which are the inputs or incoming data
streams that the operation acts on. GeoTrellis will automatically run all child
operations in parallel. For example, if you are adding each cell of five
rasters together, the service will calculate each input raster in parallel (if
resources allow).

If working with a single large data set (e.g. a large raster), it is usually
beneficial to transform the raster into a tiled raster which allows the system
to parallelize the work and to reduce I/O.

#### Distributed Execution
With a single command, an operation can be transformed into a RemoteOperation
that sends each of its child operations to a remote GeoTrellis server for
execution.


    // calculate each input raster on remote cluster
    local.Add(r1,r2,r3).remote(cluster)

The remote cluster or server is an Akka actor or load balancer, defined using
the Akka library configuration. See the [Akka documenation](http://akka.io/docs/akka/2.0-M4)
for more information. Akka configuration can be placed directly in your
GeoTrellis configuration file (application.conf).

For operations that do not need to pass much data over the network, the
overhead can be as low as a few milliseconds. When passing raster data, the
time overhead depends on the network and the size of the rasters. 

#### Architecture Concepts
Behind the scenes, GeoTrellis is using the actor model for parallel and
distributed computation--the primary abstraction is message passing instead of
shared memory. This event-based model allows us to re-use the same threads by
interleaving calculations for different processes on the same threads, but
requires operations to be asynchronous. Operations are not allowed to block
while waiting on another result because that would hold up the other work that
is scheduled to execute on the same thread. 

By using the [continuation passing style](http://en.wikipedia.org/wiki/Continuation-passing_style)
from functional programming we perform work in a series of stages:

 + Calculation begins on a particular thread.
 + If calculation can complete, it does so (and we are done).
 + Otherwise, the calculation and its subsidiaries are sent to a dispatcher.
 + Calculation exits, freeing the thread.
 + Subsidiary calculations are sent to new threads to be processed.
 + When all subsidiary results are available, the next calculation step begins on a new thread.

Thus, we can perform complex calculations across many threads, without ever
blocking execution on an individual thread.

Operations are sent as immutable messages, and are referrentially transparent.
This allows us to perform optimizations such as combining  
local raster operations. 
