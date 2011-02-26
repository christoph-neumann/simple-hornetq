/**
@author Christoph Neumann

An example of a single producer making quite a lot of messages which are then
consumed by several consumers. The messages are numbered and then verified to
be dequeued in the correct order.

The producer is run in its own thread so the consumers can begin to drain the
queue while the producer is still filling it up. Also, it's worth noting that
we need one session per thread. The underlying HornetQ ClientSession is not
thread-safe.
*/

package app

import org.apache.log4j._
import lib.SimpleHornetQ


object MultipleConsumers {
	val log = Logger.getLogger(this.getClass())

	def main(args: Array[String]) {
		val hornetq = new SimpleHornetQ()

		// Make the queue before doing anything else
		hornetq.withSession { session =>
			session.createQueue("example", "example", false)
		}

		// Make a producer that runs in its own thread. Note how it needs its own session.
		val prod_thread = new Thread() {
			override def run() {
				hornetq.withSession { session =>
					log.debug("Creating the producer and queuing a bunch of messages")
					val producer = session.createProducer("example")
					for ( i <- 1 to 20000 ) {
						if ( i % 1000 == 0 ) println("producer at "+ i)
						producer.send(i.toString)
					}
				}
			}
		}
		prod_thread.start()


		hornetq.withSession { session =>
			// Our escape mechanism for later. Think of it as an object-oriented "goto".
			class AllDoneException extends Exception

			log.debug("Creating multiple consumers consumer and receiving a single message")
			val consumers = List(
				session.createConsumer("example"),
				session.createConsumer("example"),
				session.createConsumer("example")
			)

			var count = 1
			var correct_order = true
			try {
				while ( true ) {
					var i = 0;
					consumers foreach { consumer =>
						i += 1;
						consumer.receiveUntil(500) match {
							case Some(message) => {
								val num = message.toInt
								if ( num % 1000 == 0 ) println("dequeue at "+ num)
								if ( num != count ) {
									correct_order = false
									println("Skipped: "+ count +" but found "+ num)
								}
								count += 1
							}
							case None => {
								println("Timout. All done!");
								throw new AllDoneException()
							}
						}
					}
				}
			} catch {
				case e: AllDoneException => { /* All done */ }
			}

			println("Dequeued "+ (count-1) +" messages")
			if ( correct_order ) {
				println("All the messages were in the proper order!")
			}
		}
	}
}
