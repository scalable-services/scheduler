package services.scalable.scheduler

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.ClusterEvent._
import akka.cluster.typed.{Cluster, Subscribe}

object ClusterListener {

  sealed trait Event

  // internal adapted cluster events only
  private final case class ReachabilityChange(reachabilityEvent: ReachabilityEvent) extends Event

  private final case class MemberChange(event: MemberEvent) extends Event

  def apply(): Behavior[Event] = Behaviors.setup { ctx =>
    val memberEventAdapter: ActorRef[MemberEvent] = ctx.messageAdapter(MemberChange)
    Cluster(ctx.system).subscriptions ! Subscribe(memberEventAdapter, classOf[MemberEvent])

    val reachabilityAdapter = ctx.messageAdapter(ReachabilityChange)
    Cluster(ctx.system).subscriptions ! Subscribe(reachabilityAdapter, classOf[ReachabilityEvent])

    Behaviors.receiveMessage { message =>
      message match {
        case ReachabilityChange(reachabilityEvent) =>
          reachabilityEvent match {
            case UnreachableMember(member) =>
              ctx.log.info("Member detected as unreachable: {}", Seq(member))
            case ReachableMember(member) =>
              ctx.log.info("Member back to reachable: {}", Seq(member))
          }

        case MemberChange(changeEvent) =>
          changeEvent match {
            case MemberUp(member) =>
              ctx.log.info("Member is Up: {}", member.address)
            case MemberRemoved(member, previousStatus) =>
              ctx.log.info("Member is Removed: {} after {}",
                Seq(member.address, previousStatus))
            case _: MemberEvent => // ignore
          }
      }
      Behaviors.same
    }
  }

}