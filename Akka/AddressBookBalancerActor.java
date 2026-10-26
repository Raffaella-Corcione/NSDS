package com.lab.evaluation25;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.Patterns;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class AddressBookBalancerActor extends AbstractActor {
	private scala.concurrent.duration.Duration timeout = scala.concurrent.duration.Duration.create(1, SECONDS);
	ActorRef worker0;
	ActorRef worker1;

	public AddressBookBalancerActor() {
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(BalancerSetupMsg.class, msg -> {
					this.worker0 = msg.getWorker0();
					this.worker1 = msg.getWorker1();
				})
				.match(PutMsg.class, this::storeEntry)
				.match(GetMsg.class, this::routeQuery)
				.build();
	}

	int splitByInitial(String s) {
		char firstChar = s.charAt(0);

		// Normalize case for comparison
		char upper = Character.toUpperCase(firstChar);

		if (upper >= 'A' && upper <= 'M') {
			return 0;
		} else {
			return 1;
		}
	}

	void routeQuery(GetMsg msg) {
		System.out.println("BALANCER: Received query for name " + msg.getName());

		int w = splitByInitial(msg.getName());
		ActorRef first, second;
		if (w == 0){
			first = worker0;
			second = worker1;
		} else {
			first = worker1;
			second = worker0;
		}
		scala.concurrent.Future<Object> waitingForEmail = Patterns.ask(first, msg, 1000);
		try{
			ReplyMsg reply = (ReplyMsg) Await.result(waitingForEmail, timeout);
			sender().tell(reply, self());
		} catch(TimeoutException | InterruptedException e){
			System.out.println("BALANCER: Primary copy query for name " + msg.getName() + " is resting!");
			waitingForEmail = Patterns.ask(second, msg, 1000);
			try{
				ReplyMsg reply = (ReplyMsg) Await.result(waitingForEmail, timeout);
				sender().tell(reply, self());
			} catch(TimeoutException | InterruptedException e2){
				System.out.println("BALANCER: Both copies are resting for name " + msg.getName() + "!");
				sender().tell(new TimeoutMsg(), self());
			}
		}
	}

	void storeEntry(PutMsg msg) {
		System.out.println("BALANCER: Received new entry " + msg.getName() + " - " + msg.getEmail());
		int w = splitByInitial(msg.getName());
		ActorRef primary, replica;
		if (w == 0){
			primary = worker0;
			replica = worker1;
		} else {
			primary = worker1;
			replica = worker0;
		}

		primary.tell(new PutPrimaryMsg(msg.getName(), msg.getEmail()), self());
		replica.tell(new PutReplicaMsg(msg.getName(), msg.getEmail()), self());
	}

	static Props props() {
		return Props.create(AddressBookBalancerActor.class);
	}

}
