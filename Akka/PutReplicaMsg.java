package com.lab.evaluation25;

public class PutReplicaMsg {
    private String name;
    private String email;

    public PutReplicaMsg (String name, String email) {
        this.name = name;
        this.email = email;
    }

    public String getName() {
        return name;
    }

    public String getEmail() {
        return email;
    }
}
