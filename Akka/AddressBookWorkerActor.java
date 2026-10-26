package com.lab.evaluation25;

import java.util.HashMap;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class AddressBookWorkerActor extends AbstractActor {
	private HashMap<String, String> primaryAddresses;
	private HashMap<String, String> replicaAddresses;

	public AddressBookWorkerActor() {
		this.primaryAddresses = new HashMap<String, String>();
		this.replicaAddresses = new HashMap<String, String>();
	}

	@Override
	public Receive createReceive() {
		return normalBehavior();
	}

	private Receive normalBehavior() {
		return receiveBuilder().match(PutPrimaryMsg.class, this::putPrimary)
				.match(PutReplicaMsg.class, this::putReplica)
				.match(GetMsg.class, this::generateReply)
				.match(ResumeMsg.class, msg -> {})
				.match(RestMsg.class, msg -> {getContext().become(restingBehavior());}).build();
	}

	private Receive restingBehavior() {
		return receiveBuilder().match(PutPrimaryMsg.class, msg -> {})
				.match(PutReplicaMsg.class, msg -> {})
				.match(GetMsg.class, msg -> {})
				.match(ResumeMsg.class, msg -> {getContext().become(normalBehavior());})
				.match(RestMsg.class, msg -> {}).build();
	}

	public void putPrimary(PutPrimaryMsg msg) {
		primaryAddresses.put(msg.getName(), msg.getEmail());
		System.out.println(this.toString() + ": Adding name " + msg.getName() + " to primary addresses");
	}

	public void putReplica(PutReplicaMsg msg) {
		replicaAddresses.put(msg.getName(), msg.getEmail());
		System.out.println(this.toString() + ": Adding name " + msg.getName() + " to replica addresses");
	}
	
	void generateReply(GetMsg msg) {
		System.out.println(this.toString() + ": Received query for name " + msg.getName());
		if(primaryAddresses.containsKey(msg.getName())) {
			sender().tell(new ReplyMsg(primaryAddresses.get(msg.getName())), self());
		} else if(replicaAddresses.containsKey(msg.getName())) {
			sender().tell(new ReplyMsg(replicaAddresses.get(msg.getName())), self());
		} else {
			sender().tell(new ReplyMsg(""), self());
		}
	}

	static Props props() {
		return Props.create(AddressBookWorkerActor.class);
	}
}
