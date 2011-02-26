Simple HornetQ
==============

The goal of this project is to demonstrate a very simple wrapper around the
HornetQ Core API. I wrote this to experiment with HornetQ messaging, Scala, and
Apache Ivy.


Running HornetQ
---------------
HornetQ is very easy to run out the box.

1. Download the latest: <http://www.jboss.org/hornetq/downloads>
2. Unzip/untar the files.
3. Go into the "bin" directory and ./run.sh


Running the Examples
--------------------
Build the project with "ant". All the dependancies are fetched with Ivy. The
basic steps are:

    $ ant
    $ cd target
    $ ./run.sh

The project comes with a number of example classes in src/scala/app. By default,
the run.sh script uses the SingleMessage class. You can specify a different
example as a command line parameter:

    $ ./run.sh LotsOfMessages

Happy experimenting! Contributions are always welcome.

Christoph Neumann
