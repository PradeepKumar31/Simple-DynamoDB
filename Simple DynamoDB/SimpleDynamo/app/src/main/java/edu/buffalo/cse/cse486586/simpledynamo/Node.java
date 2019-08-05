package edu.buffalo.cse.cse486586.simpledynamo;

public class Node {
    String myPort;
    String myNode;
    String hashNode;
    String successorPort;
    String succossorPort2;
    boolean front;
    boolean back;

    public Node() {
        this.front = false;
        this.back = false;
    }

    public void setBack(boolean back) {
        this.back = back;
    }

    public void setFront(boolean front) {
        this.front = front;
    }

    public boolean getBack() {
        return back;
    }

    public boolean getFront() {
        return front;
    }

    public void setMyPort(String myPort) {
        this.myPort = myPort;
    }

    public String getMyPort() {
        return myPort;
    }

    public void setHashNode(String hashNode) {
        this.hashNode = hashNode;
    }

    public void setMyNode(String myNode) {
        this.myNode = myNode;
    }

    public String getMyNode() {
        return myNode;
    }

    public String getHashNode() {
        return hashNode;
    }

    public void setSuccessorPort(String successorPort) {
        this.successorPort = successorPort;
    }

    public String getSuccessorPort() {
        return successorPort;
    }

    public void setSuccessorPort2(String successorPort2) {
        this.succossorPort2 = successorPort2;
    }

    public String getSuccessorPort2() {
        return succossorPort2;
    }
}
