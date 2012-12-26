Revori - a revision-oriented DBMS
=================================

[![Build Status](https://travis-ci.org/ReadyTalk/revori.png?branch=master)](https://travis-ci.org/ReadyTalk/revori)

Revori is a database management system designed to handle
rapidly-changing data efficiently.  In addition to traditional
relational database queries, Revori supports query subscriptions
which allow clients to retrieve the intial result of a query followed
by a stream containing live updates to that result.

The design is a hybrid of a relational database management system and
a version control system.  A database is represented as an immutable
revision from which new revisions may be derived with data added,
subtracted, or replaced.  These revisions may be compared with each
other and/or combined using a three-way merge algorithm.

Status
------

The code is currently in an alpha state, meaning some stuff works,
some stuff doesn't, and the API is not yet thoroughly documented and
may change without warning.  If you're brave and want to start playing
with it anyway, you can build and browse the JavaDoc (run "ant
javadoc") and consult the test/unittests directory for simple examples
of how to use it.

Build
-----

#### Client

You'll need to have the libreadline headers and library installed to 
build the client)

	$ gradle client:build

#### Server

	$ gradle build


#### All

If you want to build it all, just execute:

	$ gradle


Runtime
-------

#### Server

The following starts the Revori server, which listens to localhost:8017

	$ gradle start

#### Client

You can use the SQL front-end client to test out Revori:


	$ ./client/build/binaries/client localhost 8017
