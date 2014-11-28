/**
 * @author Dileep Ranganathan
 */

package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import android.database.Cursor;

public class Node implements Serializable {
    /**
	 * 
	 */
    private static final long serialVersionUID = 8635936017634718701L;

    private String node_id;
    private String chord_id;
    private String predecessor_node_id;
    private String predecessor_chord_id;
    private String successor_node_id;
    private String successor_chord_id;
    private String key;
    private String value;
    private String queryInitiator;
    private String uri;
    private Map<String, String> queryCursor;
    private int deleteCount = 0;

    public Node(String node_id, String chord_id, String predecessor_node_id,
            String predecessor_chord_id, String successor_node_id,
            String successor_chord_id) {
        this.node_id = node_id;
        this.chord_id = chord_id;
        this.predecessor_node_id = predecessor_node_id;
        this.predecessor_chord_id = predecessor_chord_id;
        this.successor_node_id = successor_node_id;
        this.successor_chord_id = successor_chord_id;
    }

    public Node(String node_id, String chord_id) {
        this.node_id = node_id;
        this.chord_id = chord_id;
        this.predecessor_node_id = "";
        this.predecessor_chord_id = "";
        this.successor_node_id = node_id;
        this.successor_chord_id = chord_id;
    }

    public String getNode_id() {
        return node_id;
    }

    public void setNode_id(String node_id) {
        this.node_id = node_id;
    }

    public String getChord_id() {
        return chord_id;
    }

    public void setChord_id(String chord_id) {
        this.chord_id = chord_id;
    }

    public String getPredecessor_node_id() {
        return predecessor_node_id;
    }

    public void setPredecessor_node_id(String predecessor_node_id) {
        this.predecessor_node_id = predecessor_node_id;
    }

    public String getPredecessor_chord_id() {
        return predecessor_chord_id;
    }

    public void setPredecessor_chord_id(String predecessor_chord_id) {
        this.predecessor_chord_id = predecessor_chord_id;
    }

    public String getSuccessor_node_id() {
        return successor_node_id;
    }

    public void setSuccessor_node_id(String successor_node_id) {
        this.successor_node_id = successor_node_id;
    }

    public String getSuccessor_chord_id() {
        return successor_chord_id;
    }

    public void setSuccessor_chord_id(String successor_chord_id) {
        this.successor_chord_id = successor_chord_id;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public void setSuccessor(String nodeId, String chordId) {
        this.setSuccessor_node_id(nodeId);
        this.setSuccessor_chord_id(chordId);
    }

    public void setPredecessor(String nodeId, String chordId) {
        this.setPredecessor_node_id(nodeId);
        this.setPredecessor_chord_id(chordId);
    }

    public String getQueryInitiator() {
        return queryInitiator;
    }

    public void setQueryInitiator(String queryInitiator) {
        this.queryInitiator = queryInitiator;
    }

    public Map<String, String> getQueryCursor() {
        return queryCursor;
    }

    public void setQueryCursor(Cursor cursor) {
        queryCursor = new LinkedHashMap<String, String>();
        mergeCursor(cursor);
    }

    public void mergeCursor(Cursor cursor) {
        if (cursor == null || cursor.getCount() < 1) {
            return;
        }
        if (cursor.moveToFirst()) {
            // Log.d("DSPOCK", "Query_All Inside setQueryCursor");
            while (!cursor.isAfterLast()) {
                int keyIndex = cursor.getColumnIndex(LocalDHTable.COLUMN_KEY);
                int valueIndex = cursor
                        .getColumnIndex(LocalDHTable.COLUMN_VALUE);
                String returnKey = cursor.getString(keyIndex);
                String returnValue = cursor.getString(valueIndex);
                /*
                 * Log.d("DSPOCK", "Query_All Inside setQueryCursor > key=" +
                 * returnKey + ", value=" + returnValue);
                 */
                queryCursor.put(returnKey, returnValue);
                cursor.moveToNext();
            }
        }
    }

    public int getDeleteCount() {
        return deleteCount;
    }

    public void setDeleteCount(int deleteCount) {
        this.deleteCount = deleteCount;
    }

}
