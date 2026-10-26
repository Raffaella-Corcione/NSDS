package com.lab.evaluation25;

public class ReplyMsg {
    private String reply;

    public ReplyMsg(String reply) {
        this.reply = reply;
    }

    public String getReply() {
        return reply;
    }

    public boolean isEmpty() {
        return reply.isEmpty();
    }
}
