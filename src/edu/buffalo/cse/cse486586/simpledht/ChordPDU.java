/**
 * @author Dileep Ranganathan
 */

package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;

import edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider.MESSAGE_TYPES;

public class ChordPDU implements Serializable {

    private static final long serialVersionUID = -5254716713055427138L;

    MESSAGE_TYPES messageType;
    Node messagePayload;

    public ChordPDU(MESSAGE_TYPES messageType, Node messagePayload) {
        this.messageType = messageType;
        this.messagePayload = messagePayload;
    }

}
