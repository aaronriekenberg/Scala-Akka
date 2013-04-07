package org.aaron.scala.akka

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationLong
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.Logging
import akka.routing.BroadcastRouter
import akka.routing.RoundRobinRouter

object ProducerConsumer extends App {

  sealed trait ConsumerMessage
  case class IntegerMessage(i: Int) extends ConsumerMessage

  class Consumer extends Actor {

    private val log = Logging(context.system, this)

    def receive = {
      case IntegerMessage(i) =>
        log.info(s"${self.path} received ${i}")
    }
  }

  sealed trait ProducerMessage
  case class TimeToProduceMessage extends ProducerMessage

  class Producer extends Actor {

    private val log = Logging(context.system, this)

    private val consumerRouter = context.actorOf(
      Props[Consumer].withRouter(RoundRobinRouter(4)),
      name = "ConsumerRouter")

    private val i = new AtomicInteger(0)

    def receive = {
      case TimeToProduceMessage => {
        val message = IntegerMessage(i.getAndIncrement())
        log.info(s"producing ${message.i}")
        consumerRouter ! message
      }
    }
  }

  def start {
    val system = ActorSystem("ProducerConsumer")

    val producer = system.actorOf(Props[Producer], name = "Producer")

    import system.dispatcher

    val cancellable =
      system.scheduler.schedule(
        initialDelay = 500 milliseconds,
        interval = 500 milliseconds,
        receiver = producer,
        message = TimeToProduceMessage)
  }

  start

}