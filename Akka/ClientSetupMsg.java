package com.lab.evaluation25;

import akka.actor.ActorRef;

public class ClientSetupMsg {
    private final ActorRef balancer;

    public ClientSetupMsg(ActorRef balancer) {
        this.balancer = balancer;
    }

    public ActorRef getBalancer() {
        return balancer;
    }
}
