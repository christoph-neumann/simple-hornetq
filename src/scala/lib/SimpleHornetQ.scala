/**
@author Christoph Neumann

Class to simplify the usage of HornetQ. This class does not expose all the nifty features of
HornetQ. The goal of this class is to create the least number steps to start using HornetQ.

Instances of this class are thread safe, but Sessions, Producers, and Consumers are not.
*/

package lib

import java.util.HashMap

import org.hornetq.api.core.client.ClientConsumer
import org.hornetq.api.core.client.ClientMessage
import org.hornetq.api.core.client.ClientProducer
import org.hornetq.api.core.client.ClientSession
import org.hornetq.api.core.client.ClientSessionFactory
import org.hornetq.api.core.client.HornetQClient
import org.hornetq.api.core.HornetQException
import org.hornetq.api.core.SimpleString
import org.hornetq.api.core.TransportConfiguration
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory
import org.hornetq.core.remoting.impl.netty.TransportConstants

import org.apache.log4j._

class SimpleHornetQ {
	val log = Logger.getLogger(this.getClass())

	// This makes all *sorts* of assumptions about how to connect.
	val connectionParams = new HashMap[String, Object]()
	val transportConfiguration = new TransportConfiguration(classOf[NettyConnectorFactory].getName(), connectionParams)

	// The factory is thread-safe, so everyone can share.
	val factory: ClientSessionFactory = HornetQClient.createClientSessionFactory(transportConfiguration)

	// Create our own "Producer" which wraps the real HornetQ producer. This is not intended to be
	// complete, just simple.
	class Producer(session: ClientSession, producer: ClientProducer) {
		def send(message: String, durable: Boolean = false) {
			val cmsg: ClientMessage = session.createMessage(durable)
			cmsg.getBodyBuffer().writeString(message)
			producer.send(cmsg)
		}
	}

	// Create our own "Consumer" which wraps the real HornetQ consumer. This is not intended to be
	// complete, just simple.
	class Consumer(session: ClientSession, consumer: ClientConsumer) {
		// Wait for a message. We assume it's a text message.
		def receive(): String = {
			val msg: ClientMessage = consumer.receive()
			val str = msg.getBodyBuffer().readString()
			msg.acknowledge()
			return str
		}

		// Wait for a message until the timeout occurs. Like before, we assume it is a text message.
		def receiveUntil(timeout: Int): Option[String] = {
			val msg: ClientMessage = consumer.receive(timeout)
			if ( msg == null ) {
				return None
			}
			val str = msg.getBodyBuffer().readString()
			msg.acknowledge()
			return Some(str)
		}
	}

	// Create our own "Session" which wraps the real HornetQ ClientSession.
	class Session(session: ClientSession) {
		// Create one of our special consumers
		def createConsumer(queue: String): Consumer = {
			val consumer: ClientConsumer = session.createConsumer(queue)
			return new Consumer(session, consumer)
		}

		// Create one of our special producers
		def createProducer(address: String): Producer = {
			val producer: ClientProducer = session.createProducer(address)
			return new Producer(session, producer)
		}

		// Create a queue on the server. It's OK if it is already there. You
		// can call this just to make sure the queue is there.
		def createQueue(addr: String, queue: String, durable: Boolean) {
			_createQueue(session, addr, queue, durable)
		}
	}

	// It's imperative that a session is always closed. This method will always own the session and
	// make sure it gets closed. The closure you provide will be given the "wrapped" session, not
	// the real one.
	def withSession(closure: (Session => Unit)) {
		val session = factory.createSession(true, true)
		session.start()
		try {
			closure(new Session(session))
		} finally {
			session.close()
		}
	}

	// Idiom for making a queue. It would be nice if there was an exception-free, atomic way to do
	// this and still handle the queue already existing.
	private def _createQueue(session: ClientSession, addr: String, queue: String, durable: Boolean) {
		try {
			if ( ! session.queueQuery(new SimpleString(queue)).isExists() ) {
				session.createQueue(addr, queue, durable)
			}
		} catch {
			case e: HornetQException =>
				if ( e.getCode() == HornetQException.QUEUE_EXISTS ) {
					// Looks like two clients tried to make the queue at the same time.
					log.debug("FYI: Handled race condition when making queue: "+ queue) 
				} else {
					throw e
				}
		}
	}
}
