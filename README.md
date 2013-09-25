Erles
=====
Erles is a client for Event Store ([http://geteventstore.com/](http://geteventstore.com/)) written in Erlang.
Copyright © 2013 Andrii Nakryiko <andrii.nakryiko@gmail.com>

Features
========

Erles aims to provide a convenient and reliable client for using Event Store.
Currently it is on par with official Event Store's client with regard to standard operations
and strives to be even more convenient to use thanks to dynamic nature of Erlang.

Supported operations:

  * connect/2, connect/3
  * close/1
  * ping/1
  * append events to stream (`erles:append/4`, `erles:append/5`);
  * `transaction_start/3`, `transaction_start/4`
  * `transaction_write/3`, `transaction_write/4`
  * `transaction_commit/2`, `transaction_commit/3`
  * `delete/3`, `delete/4`
  * `read_event/3`, `read_event/4`
  * `read_stream/4`, `read_stream/5`, `read_stream/6`
  * `subscribe/2`, `subscribe/3`, `unsubscribe/1`
  * `subscribe_perm/2`, `subscribe_perm/3`, `subscribe_perm/4`, `unsubscribe_perm/1`
  * `set_metadata/4`, `set_metadata/5`
  * `get_metadata/2`, `get_metadata/3`, `get_metadata/4`

License
=======

Erles is licensed under The MIT License, see LICENSE file in repository.
