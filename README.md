erles - Erlang client for Event Store
=====

*erles is an Erlang client library for [Event Store](http://geteventstore.com/).

erles is licensed under [The MIT License](http://opensource.org/licenses/MIT).

Copyright © 2013 Andrii Nakryiko <andrii.nakryiko@gmail.com>


Features
========

erles aims to provide a convenient and reliable client for using Event Store.
Currently it is on par with official Event Store's client with regard to standard operations
and strives to be even more convenient to use thanks to dynamic nature of Erlang.

Main features:

  * Permanent connection to node/cluster with automatic reconnects.
  * User authentication support;
  * Automatic reconnection to current cluster master.
  * Batch writes to stream.
  * Explicit transaction writes.
  * Stream deletion (both soft- and hard-deletes).
  * Single event reads from streams.
  * Stream reads in forward/backward direction from usual stream or $all pseudo-stream.
  * Primitive subscriptions provided by Event Store directly.
  * Permanent catch-up subscriptions which catch-up, subscribe to live events, and reconnects on errors/connection drops.
  * Metadata set/get operations which allow to work with both structured stream metadata, as well as with raw binary metadata.
  * Ping operation (just for fun and testing that Event Store is responding :) ).


Getting started
===============



