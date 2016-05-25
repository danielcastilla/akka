/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.remote.artery

import scala.concurrent.duration._
import scala.util.Success
import scala.util.control.NoStackTrace
import akka.remote.EndpointManager.Send
import akka.remote.UniqueAddress
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.stream.stage.TimerGraphStageLogic
import java.util.concurrent.TimeUnit

/**
 * INTERNAL API
 */
private[akka] object OutboundHandshake {

  /**
   * Stream is failed with this exception if the handshake is not completed
   * within the handshake timeout.
   */
  class HandshakeTimeoutException(msg: String) extends RuntimeException(msg) with NoStackTrace

  // FIXME serialization for these messages
  final case class HandshakeReq(from: UniqueAddress) extends ControlMessage
  final case class HandshakeRsp(from: UniqueAddress) extends Reply

  private sealed trait HandshakeState
  private case object Start extends HandshakeState
  private case object ReqInProgress extends HandshakeState
  private case object Completed extends HandshakeState

  private case object HandshakeTimeout
  private case object HandshakeRetryTick

}

/**
 * INTERNAL API
 */
private[akka] class OutboundHandshake(outboundContext: OutboundContext, timeout: FiniteDuration,
                                      retryInterval: FiniteDuration, injectHandshakeInterval: FiniteDuration)
  extends GraphStage[FlowShape[Send, Send]] {

  val in: Inlet[Send] = Inlet("OutboundHandshake.in")
  val out: Outlet[Send] = Outlet("OutboundHandshake.out")
  override val shape: FlowShape[Send, Send] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {
      import OutboundHandshake._

      private var handshakeState: HandshakeState = Start
      private var lastMessageTime = System.nanoTime()
      private val injectHandshakeIntervalNanos = injectHandshakeInterval.toNanos

      override def preStart(): Unit = {
        val uniqueRemoteAddress = outboundContext.associationState.uniqueRemoteAddress
        if (uniqueRemoteAddress.isCompleted) {
          handshakeState = Completed
        } else {
          // The InboundHandshake stage will complete the uniqueRemoteAddress future
          // when it receives the HandshakeRsp reply
          implicit val ec = materializer.executionContext
          uniqueRemoteAddress.foreach {
            getAsyncCallback[UniqueAddress] { a ⇒
              if (handshakeState != Completed) {
                handshakeCompleted()
                if (isAvailable(out))
                  pull(in)
              }
            }.invoke
          }
        }
      }

      // InHandler
      override def onPush(): Unit = {
        if (handshakeState != Completed)
          throw new IllegalStateException(s"onPush before handshake completed, was [$handshakeState]")

        // inject a HandshakeReq once in a while to trigger a new handshake when destination
        // system has been restarted
        // FIXME if nanoTime for each message is too costly we could let the TaskRunner update
        //       a volatile field with current time less frequently (low resolution time is ok for this)
        val now = System.nanoTime()
        if (System.nanoTime() - lastMessageTime >= injectHandshakeIntervalNanos)
          outboundContext.sendControl(HandshakeReq(outboundContext.localAddress))
        lastMessageTime = now

        push(out, grab(in))
      }

      // OutHandler
      override def onPull(): Unit = {
        handshakeState match {
          case Completed ⇒ pull(in)
          case Start ⇒
            // will pull when handshake reply is received (uniqueRemoteAddress completed)
            handshakeState = ReqInProgress
            scheduleOnce(HandshakeTimeout, timeout)
            schedulePeriodically(HandshakeRetryTick, retryInterval)
            sendHandshakeReq()
          case ReqInProgress ⇒ // will pull when handshake reply is received
        }
      }

      private def sendHandshakeReq(): Unit =
        outboundContext.sendControl(HandshakeReq(outboundContext.localAddress))

      private def handshakeCompleted(): Unit = {
        handshakeState = Completed
        cancelTimer(HandshakeRetryTick)
        cancelTimer(HandshakeTimeout)
      }

      override protected def onTimer(timerKey: Any): Unit =
        timerKey match {
          case HandshakeRetryTick ⇒
            sendHandshakeReq()
          case HandshakeTimeout ⇒
            // FIXME would it make sense to retry a few times before failing?
            failStage(new HandshakeTimeoutException(
              s"Handshake with [${outboundContext.remoteAddress}] did not complete within ${timeout.toMillis} ms"))
        }

      setHandlers(in, out, this)
    }

}

/**
 * INTERNAL API
 */
private[akka] class InboundHandshake(inboundContext: InboundContext, inControlStream: Boolean) extends GraphStage[FlowShape[InboundEnvelope, InboundEnvelope]] {
  val in: Inlet[InboundEnvelope] = Inlet("InboundHandshake.in")
  val out: Outlet[InboundEnvelope] = Outlet("InboundHandshake.out")
  override val shape: FlowShape[InboundEnvelope, InboundEnvelope] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with OutHandler with StageLogging {
      import OutboundHandshake._

      // InHandler
      if (inControlStream)
        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            grab(in) match {
              case InboundEnvelope(_, _, HandshakeReq(from), _, _) ⇒
                inboundContext.completeHandshake(from)
                inboundContext.sendControl(from.address, HandshakeRsp(inboundContext.localAddress))
                pull(in)
              case InboundEnvelope(_, _, HandshakeRsp(from), _, _) ⇒
                inboundContext.completeHandshake(from)
                pull(in)
              case other ⇒ onMessage(other)
            }
          }
        })
      else
        setHandler(in, new InHandler {
          override def onPush(): Unit = onMessage(grab(in))
        })

      private def onMessage(env: InboundEnvelope): Unit = {
        if (isKnownOrigin(env.originUid))
          push(out, env)
        else {
          // FIXME remove, only debug
          log.warning(s"Dropping message [{}] from unknown system with UID [{}]. " +
            "This system with UID [{}] was probably restarted. " +
            "Messages will be accepted when new handshake has been completed.",
            env.message.getClass.getName, inboundContext.localAddress.uid, env.originUid)
          if (log.isDebugEnabled)
            log.debug(s"Dropping message [{}] from unknown system with UID [{}]. " +
              "This system with UID [{}] was probably restarted. " +
              "Messages will be accepted when new handshake has been completed.",
              env.message.getClass.getName, inboundContext.localAddress.uid, env.originUid)
          pull(in)
        }
      }

      private def isKnownOrigin(originUid: Long): Boolean = {
        // FIXME these association lookups are probably too costly for each message, need local cache or something
        (inboundContext.association(originUid) ne null)
      }

      // OutHandler
      override def onPull(): Unit = pull(in)

      setHandler(out, this)

    }

}
