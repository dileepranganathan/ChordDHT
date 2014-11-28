/**
 * @author Dileep Ranganathan
 */

package edu.buffalo.cse.cse486586.simpledht;

import edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider.MESSAGE_TYPES;

public class MessageParams {
    Node n;
    String remotePort;
    MESSAGE_TYPES messageType;

    public MessageParams(Node n, String remotePort, MESSAGE_TYPES msgType) {
        this.n = n;
        this.remotePort = remotePort;
        this.messageType = msgType;
    }
}
