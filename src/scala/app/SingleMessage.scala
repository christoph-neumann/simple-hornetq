/**
@author Christoph Neumann

An example of a single message being sent from a producer to a consumer.
*/

package app

import org.apache.log4j._
import lib.SimpleHornetQ


object SingleMessage {
	val log = Logger.getLogger(this.getClass())

	def main(args: Array[String]) {
		val hornetq = new SimpleHornetQ()

		hornetq.withSession { session =>
			log.debug("Creating the example address + queue")
			session.createQueue("example", "example", false)

			log.debug("Creating the producer and queuing a single message")
			val producer = session.createProducer("example")
			producer.send("Take me to your leader!")

			log.debug("Creating the consumer and receiving a single message")
			val consumer = session.createConsumer("example")
			println("The message was: "+ consumer.receive)
		}
	}
}
