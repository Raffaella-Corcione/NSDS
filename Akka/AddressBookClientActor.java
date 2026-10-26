package com.lab.evaluation25;

import static java.util.concurrent.TimeUnit.SECONDS;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.Patterns;


public class AddressBookClientActor extends AbstractActor {

	private scala.concurrent.duration.Duration timeout = scala.concurrent.duration.Duration.create(3, SECONDS);
	private ActorRef balancer;

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(ClientSetupMsg.class, msg -> {
					this.balancer = msg.getBalancer();
				})
				.match(PutMsg.class, this::putEntry)
				.match(GetMsg.class, this::query).build();
	}

	void putEntry(PutMsg msg) {
		System.out.println("CLIENT: Sending new entry " + msg.getName() + " - " + msg.getEmail());
		balancer.tell(msg, getSelf());
	}

	void query(GetMsg msg) {
		System.out.println("CLIENT: Issuing query for " + msg.getName());

		scala.concurrent.Future<Object> waitingForEmail = Patterns.ask(balancer, msg, 3000);
		try {
			Object reply = waitingForEmail.result(timeout, null);
			if(reply instanceof ReplyMsg) {
				ReplyMsg replyMsg = (ReplyMsg) reply;
				if(replyMsg.isEmpty())
					System.out.println("CLIENT: Received reply, no email found!");
				else
					System.out.println("CLIENT: Received reply! " + replyMsg.getReply());
			} else if (reply instanceof TimeoutMsg) {
				System.out.println("CLIENT: Received timeout, both copies are resting!");
			}
		} catch (Exception e) {
			System.out.println("Request timed out");
		}
	}
	static Props props() {
		return Props.create(AddressBookClientActor.class);
	}

}
