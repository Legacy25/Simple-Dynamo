package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by arindam on 4/11/15.
 */
public class Node implements Comparable {
    public String id;
    public String hash;

    public Node(String id) {
        this.id = id;
        this.hash = SimpleDynamoProvider.genHash(id);
    }

    @Override
    public int compareTo(Object another) {
        Node otherNode = (Node) another;
        return hash.compareTo(otherNode.hash);
    }

    public String toString() {
        return "Node: "+id+" Hash: "+hash;
    }
}
