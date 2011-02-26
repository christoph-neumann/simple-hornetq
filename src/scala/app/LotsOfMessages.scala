/**
@author Christoph Neumann

An example of sending a lot of messages from a single producer to a single consumer.
*/

package app

import org.apache.log4j._
import lib.SimpleHornetQ


object LotsOfMessages {
	val log = Logger.getLogger(this.getClass())

	def main(args: Array[String]) {
		val hornetq = new SimpleHornetQ()

		hornetq.withSession { session =>
			log.debug("Creating the example address + queue")
			session.createQueue("example", "example", false)

			log.debug("Creating the producer and queuing a bunch of messages")
			val producer = session.createProducer("example")
			for ( i <- 1 to 2000 ) {
				producer.send(i.toString)
			}

			log.debug("Creating the consumer and receiving all the messages")
			val consumer = session.createConsumer("example")

			var done = false
			while (! done) {
				consumer.receiveUntil(500) match {
					case Some(message) => {
						println("Got: "+ message)
					}
					case None => {
						println("Timout. All done!")
						done = true
					}
				}
			}
		}
	}
}
