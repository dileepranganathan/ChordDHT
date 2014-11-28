/**
 * @author Dileep Ranganathan
 */

package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    // database
    private DHTDatabaseHelper database;
    private BlockingQueue<Map<String, String>> queryResponseQueue;
    private BlockingQueue<Integer> deleteResponseQueue;
    private BlockingQueue<Map<String, String>> reHashQueue;
    static final String TAG = "SimpleDhtProvider";
    private Node nodePtr = null;
    static final String remotePort = "11108";
    static final int SERVER_PORT = 10000;

    // DSPock TODO ::: Change this to enum
    public enum MESSAGE_TYPES implements Serializable {
        // "join", "insert", "node", "predecessor", "query"
        JOIN, INSERT, INSERT_NOW, NODE, PREDECESSOR, QUERY, QUERY_ALL, QUERY_RESP, DELETE, DELETE_ALL, DELETE_RESP
    };

    @Override
    public boolean onCreate() {

        /*
         * System.out.println("!!!!!!DUMP result!!!!!"); System.out.println(
         * ".....................................................");
         * System.out.println
         * (".....................................................");
         */
        // TODO Auto-generated method stub
        /*
         * Calculate the port number that this AVD listens on. It is just a hack
         * that I came up with to get around the networking limitations of AVDs.
         * The explanation is provided in the PA1 spec.
         */
        TelephonyManager tel = (TelephonyManager) getContext()
                .getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(
                tel.getLine1Number().length() - 4);
        queryResponseQueue = new LinkedBlockingQueue<Map<String, String>>();
        deleteResponseQueue = new LinkedBlockingQueue<Integer>();
        // Create serverTask
        createServerSocket();

        try {
            String node_id = this.genHash(portStr);
            nodePtr = new Node(node_id, portStr);
            if (!portStr.equalsIgnoreCase("5554")) {
                // SEND JOIN MESSAGE TO 5554
                MessageParams params = new MessageParams(nodePtr, remotePort, MESSAGE_TYPES.JOIN);
                Log.d(TAG, "########## JOIN message ##########");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, params);
            }

        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            Log.e(TAG, e.toString());
        }
        database = new DHTDatabaseHelper(getContext());
        return false;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        SQLiteDatabase db = database.getWritableDatabase();
        if (selection.equalsIgnoreCase("@")) {
            int deleteCount = db.delete(LocalDHTable.TABLE_DHT, null, null);
            getContext().getContentResolver().notifyChange(uri, null);
            return deleteCount;
        } else {
            String key;
            if (selection.equalsIgnoreCase("*")) {
                key = selection;
                int deleteCount = db.delete(LocalDHTable.TABLE_DHT, null, null);
                getContext().getContentResolver().notifyChange(uri, null);
                Integer remPort = Integer.parseInt(nodePtr.getSuccessor_chord_id()) * 2;
                String remotePortNo = remPort.toString();
                Node keyNode = new Node(key, "KEY");
                keyNode.setKey(selection);
                keyNode.setUri(uri.getPath());
                keyNode.setDeleteCount(deleteCount);
                keyNode.setQueryInitiator(nodePtr.getChord_id());
                Log.d("DSPOCK", "Initiator > " + keyNode.getQueryInitiator()
                        + " :: DELETE_all messages");

                MessageParams nodeMsg = new MessageParams(
                        keyNode, remotePortNo,
                        MESSAGE_TYPES.DELETE_ALL);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        nodeMsg);
                try {
                    int retVal = deleteResponseQueue.take();
                    deleteResponseQueue.clear();
                    return retVal;
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else {
                try {
                    key = genHash(selection);
                } catch (NoSuchAlgorithmException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                    return 0;
                }
                Node keyNode = new Node(key, "KEY");
                if (!locateQueryNode(keyNode, nodePtr)) {
                    Integer remPort = Integer.parseInt(nodePtr
                            .getSuccessor_chord_id()) * 2;
                    String remotePortNo = remPort.toString();
                    keyNode.setKey(selection);
                    keyNode.setUri(uri.getPath());
                    keyNode.setQueryInitiator(nodePtr.getChord_id());
                    MessageParams nodeMsg = new MessageParams(keyNode,
                            remotePortNo, MESSAGE_TYPES.DELETE);
                    new ClientTask().executeOnExecutor(
                            AsyncTask.SERIAL_EXECUTOR, nodeMsg);
                    try {
                        int retVal = deleteResponseQueue.take();
                        deleteResponseQueue.clear();
                        return retVal;
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                } else {
                    Log.d("DSPOCK", "Delete key but what is selection > "
                            + selection);
                    int deleteCount = db.delete(LocalDHTable.TABLE_DHT,
                            LocalDHTable.COLUMN_KEY + " = '" + selection + "'", null);
                    // getContext().getContentResolver().notifyChange(uri,
                    // null);
                    return deleteCount;
                }
            }
        }
        return 0;
    }

    public void deleteFromDHT(ChordPDU deletePDU, Uri uri) {
        if (deletePDU.messageType == MESSAGE_TYPES.DELETE_ALL) {
            Log.d("DSPOCK",
                    "DELETE_ALL reached node > " + nodePtr.getChord_id()
                            + "  ---- Initiator > "
                            + deletePDU.messagePayload.getQueryInitiator());
            if (nodePtr.getChord_id().equals(deletePDU.messagePayload.getQueryInitiator())) {
                try {
                    deleteResponseQueue.put(deletePDU.messagePayload.getDeleteCount());
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else {
                SQLiteDatabase db = database.getWritableDatabase();
                int deleteCount = db.delete(LocalDHTable.TABLE_DHT, null, null);
                // getContext().getContentResolver().notifyChange(uri, null);
                deleteCount += deletePDU.messagePayload.getDeleteCount();
                deletePDU.messagePayload.setDeleteCount(deleteCount);
                Integer remPort = Integer.parseInt(nodePtr
                        .getSuccessor_chord_id()) * 2;
                String remotePortNo = remPort.toString();
                MessageParams nodeMsg = new MessageParams(
                        deletePDU.messagePayload, remotePortNo,
                        MESSAGE_TYPES.DELETE_ALL);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        nodeMsg);
            }
        } else {
            if (nodePtr.getChord_id().equals(deletePDU.messagePayload.getQueryInitiator())) {
                try {
                    deleteResponseQueue.put(deletePDU.messagePayload.getDeleteCount());
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                return;
            }
            if (!locateQueryNode(deletePDU.messagePayload, nodePtr)) {
                Integer remPort = Integer.parseInt(nodePtr
                        .getSuccessor_chord_id()) * 2;
                String remotePortNo = remPort.toString();
                MessageParams nodeMsg = new MessageParams(
                        deletePDU.messagePayload, remotePortNo,
                        MESSAGE_TYPES.DELETE);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        nodeMsg);
            } else {
                SQLiteDatabase db = database.getWritableDatabase();
                Log.d("DSPOCK", "Delete key remotely but what is selection > "
                        + deletePDU.messagePayload.getKey());
                int deleteCount = db.delete(LocalDHTable.TABLE_DHT,
                        LocalDHTable.COLUMN_KEY + " = '" + deletePDU.messagePayload.getKey() + "'",
                        null);
                // getContext().getContentResolver().notifyChange(uri, null);
                deletePDU.messagePayload.setDeleteCount(deleteCount);
                Integer remPort = Integer.parseInt(deletePDU.messagePayload
                        .getQueryInitiator()) * 2;
                String remotePortNo = remPort.toString();
                MessageParams nodeMsg = new MessageParams(
                        deletePDU.messagePayload, remotePortNo,
                        MESSAGE_TYPES.DELETE_RESP);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        nodeMsg);
            }
        }

    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        try {
            String key = genHash(values.getAsString(LocalDHTable.COLUMN_KEY));
            Node keyNode = new Node(key, "KEY");
            if (locateNode(keyNode, nodePtr)) {
                Integer remPort = Integer.parseInt(nodePtr.getSuccessor_chord_id()) * 2;
                String remotePortNo = remPort.toString();
                keyNode.setKey(values.getAsString(LocalDHTable.COLUMN_KEY));
                keyNode.setValue(values.getAsString(LocalDHTable.COLUMN_VALUE));
                keyNode.setUri(uri.getPath());
                MessageParams nodeMsg = new MessageParams(keyNode, remotePortNo,
                        MESSAGE_TYPES.INSERT_NOW);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeMsg);
            } else {
                Integer remPort = Integer.parseInt(nodePtr.getSuccessor_chord_id()) * 2;
                String remotePortNo = remPort.toString();
                keyNode.setKey(values.getAsString(LocalDHTable.COLUMN_KEY));
                keyNode.setValue(values.getAsString(LocalDHTable.COLUMN_VALUE));
                keyNode.setUri(uri.getPath());
                MessageParams nodeMsg = new MessageParams(keyNode, remotePortNo,
                        MESSAGE_TYPES.INSERT);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeMsg);
            }

        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            Log.e(TAG, "Insertion Failed!!!");
            Log.e(TAG, e.toString());
        }
        return null;
    }

    public Uri insertToDHT(Uri uri, ContentValues values) {
        long id = 0;
        ContentValues cv = new ContentValues(values);
        SQLiteDatabase sqlDB = database.getWritableDatabase();
        id = sqlDB.replace(LocalDHTable.TABLE_DHT, null, cv);
        if (id > 0) {
            Uri newUri = ContentUris.withAppendedId(uri, id);
            getContext().getContentResolver().notifyChange(newUri, null);
            Log.d(TAG,
                    "Inserted <KEY,VALUE> = <"
                            + cv.getAsString(LocalDHTable.COLUMN_KEY) + ", "
                            + cv.getAsString(LocalDHTable.COLUMN_VALUE) + ">");
            return newUri;

        } else {
            Log.e(TAG, "Insertion Failed!!!");
        }
        return null;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
            String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        SQLiteDatabase db = database.getWritableDatabase();
        String queryStr = "select * from " + LocalDHTable.TABLE_DHT;

        if (selection.equalsIgnoreCase("@")) {
            Cursor cursor = db.rawQuery(queryStr, null);
            // make sure that potential listeners are getting notified
            // cursor.setNotificationUri(getContext().getContentResolver(),
            // uri);
            return cursor;
        } else {
            String key;
            try {
                if (selection.equalsIgnoreCase("*")) {
                    key = selection;
                    Cursor cursor = db.rawQuery(queryStr, null);
                    // make sure that potential listeners are getting notified
                    // cursor.setNotificationUri(getContext().getContentResolver(),
                    // uri);
                    Integer remPort = Integer.parseInt(nodePtr.getSuccessor_chord_id()) * 2;
                    String remotePortNo = remPort.toString();
                    Node keyNode = new Node(key, "KEY");
                    keyNode.setKey(selection);
                    keyNode.setUri(uri.getPath());
                    keyNode.setQueryInitiator(nodePtr.getChord_id());
                    keyNode.setQueryCursor(cursor);
                    Log.d("DSPOCK", "Initiator > " + keyNode.getQueryInitiator()
                            + " :: Query all messages");
                    Log.v("QALL", "Query_all ..Initiator ----> " + keyNode.getQueryInitiator()
                            + "Cursor count ---> " + cursor.getCount());

                    MessageParams nodeMsg = new MessageParams(
                            keyNode, remotePortNo,
                            MESSAGE_TYPES.QUERY_ALL);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            nodeMsg);
                    cursor.close();
                    return generateQueryResponse();
                } else {
                    queryStr += " where key like '" + selection + "'";
                    key = genHash(selection);
                    Node keyNode = new Node(key, "KEY");
                    if (!locateQueryNode(keyNode, nodePtr)) {
                        Integer remPort = Integer.parseInt(nodePtr.getSuccessor_chord_id()) * 2;
                        String remotePortNo = remPort.toString();
                        keyNode.setKey(selection);
                        keyNode.setUri(uri.getPath());
                        keyNode.setQueryInitiator(nodePtr.getChord_id());
                        MessageParams nodeMsg = new MessageParams(keyNode, remotePortNo,
                                MESSAGE_TYPES.QUERY);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeMsg);
                        return generateQueryResponse();
                    } else {
                        Cursor cursor = db.rawQuery(queryStr, null);
                        // make sure that potential listeners are getting
                        // notified
                        // cursor.setNotificationUri(getContext().getContentResolver(),
                        // uri);
                        Log.d("DSPOCK", "Query_one Direct in Node... Query is > " + queryStr);
                        Log.d("DSPOCK", "Query_one Direct in Node... Count > " + cursor.getCount());
                        return cursor;
                    }
                }

            } catch (NoSuchAlgorithmException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return null;
    }

    private Cursor generateQueryResponse() {
        try {
            MatrixCursor mCursor = new MatrixCursor(
                    new String[] {
                            LocalDHTable.COLUMN_KEY,
                            LocalDHTable.COLUMN_VALUE
                    });
            Map<String, String> qMap = queryResponseQueue.take();
            if (qMap != null) {
                Iterator<String> itr = qMap.keySet().iterator();
                String keyIter = null;
                String valIter = null;
                while (itr.hasNext()) {
                    keyIter = (String) itr.next();
                    valIter = qMap.get(keyIter);
                    mCursor.newRow().add(keyIter).add(valIter);
                }
            }
            queryResponseQueue.clear();
            // mCursor.setNotificationUri(getContext().getContentResolver(),
            // uri);
            return mCursor;
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public void queryDHT(ChordPDU queryPDU, Uri uri) {
        if (queryPDU.messageType == MESSAGE_TYPES.QUERY_ALL) {
            if (nodePtr.getChord_id().equals(queryPDU.messagePayload.getQueryInitiator())) {
                try {
                    Log.d("DSPOCK", "Query_all ring completed... Count > "
                            + queryPDU.messagePayload.getQueryCursor().size());
                    queryResponseQueue.put(queryPDU.messagePayload.getQueryCursor());
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else {
                Log.v("QALL", "Query_all ..Node ----> "
                        + nodePtr.getChord_id()
                        + "Map count ---> "
                        + queryPDU.messagePayload.getQueryCursor().size());
                SQLiteDatabase db = database.getWritableDatabase();
                String queryStr = "select * from " + LocalDHTable.TABLE_DHT;
                Cursor cursor = db.rawQuery(queryStr, null);
                Log.v("QALL", "Query_all ..Node ----> "
                        + nodePtr.getChord_id()
                        + "Cursor count ---> "
                        + cursor.getCount());
                /*
                 * boolean curseFlag = true; // make sure that potential
                 * listeners are getting notified if(cursor.getCount() < 1) {
                 * Log.d("DSPOCK", "Query_all No records found in > " +
                 * nodePtr.getChord_id()); curseFlag = false; }
                 */
                // cursor.setNotificationUri(getContext().getContentResolver(),
                // uri);
                Integer remPort = Integer.parseInt(nodePtr.getSuccessor_chord_id()) * 2;
                String remotePortNo = remPort.toString();

                // if(curseFlag) {
                queryPDU.messagePayload.mergeCursor(cursor);
                // }
                // mergeCursor.setNotificationUri(getContext().getContentResolver(),
                // uri);
                Log.v("QALL", "Query_all ..Node ----> "
                        + nodePtr.getChord_id()
                        + "MergeCursor count ---> "
                        + queryPDU.messagePayload.getQueryCursor().size());
                MessageParams nodeMsg = new MessageParams(
                        queryPDU.messagePayload, remotePortNo,
                        MESSAGE_TYPES.QUERY_ALL);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        nodeMsg);
                cursor.close();
            }
        } else {
            if (nodePtr.getChord_id().equals(queryPDU.messagePayload.getQueryInitiator())) {
                try {
                    Log.d("DSPOCK", "Query_all ring completed... Count > "
                            + queryPDU.messagePayload.getQueryCursor().size());
                    queryResponseQueue.put(queryPDU.messagePayload.getQueryCursor());
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                return;
            }
            if (!locateQueryNode(queryPDU.messagePayload, nodePtr)) {
                Integer remPort = Integer.parseInt(nodePtr
                        .getSuccessor_chord_id()) * 2;
                String remotePortNo = remPort.toString();
                MessageParams nodeMsg = new MessageParams(
                        queryPDU.messagePayload, remotePortNo,
                        MESSAGE_TYPES.QUERY);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        nodeMsg);
            } else {
                SQLiteDatabase db = database.getWritableDatabase();
                String queryStr = "select * from " + LocalDHTable.TABLE_DHT
                        + " where key like '"
                        + queryPDU.messagePayload.getKey() + "'";
                Cursor cursor = db.rawQuery(queryStr, null);
                // make sure that potential listeners are getting notified
                // cursor.setNotificationUri(getContext().getContentResolver(),uri);
                Integer remPort = Integer.parseInt(queryPDU.messagePayload
                        .getQueryInitiator()) * 2;
                String remotePortNo = remPort.toString();
                queryPDU.messagePayload.setQueryCursor(cursor);
                MessageParams nodeMsg = new MessageParams(
                        queryPDU.messagePayload, remotePortNo,
                        MESSAGE_TYPES.QUERY_RESP);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        nodeMsg);
                cursor.close();
            }
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        uriBuilder.appendPath("dht");
        return uriBuilder.build();
    }

    public void rehashDHT() {
        SQLiteDatabase db = database.getWritableDatabase();
        String queryStr = "select * from " + LocalDHTable.TABLE_DHT;
        Cursor cursor = db.rawQuery(queryStr, null);
        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        rehashCursor(cursor, mUri);
        cursor.close();
    }

    private void rehashNow(String returnKey, String returnValue, Uri uri) {
        SQLiteDatabase db = database.getWritableDatabase();
        Integer remPort = Integer.parseInt(nodePtr.getPredecessor_chord_id()) * 2;
        String remotePortNo = remPort.toString();
        Node keyNode = new Node(returnKey, "KEY");
        keyNode.setKey(returnKey);
        keyNode.setValue(returnValue);
        keyNode.setUri(uri.getPath());
        db.delete(LocalDHTable.TABLE_DHT, LocalDHTable.COLUMN_KEY + " = '"
                + returnKey + "'", null);
        MessageParams nodeMsg = new MessageParams(keyNode, remotePortNo,
                MESSAGE_TYPES.INSERT_NOW);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeMsg);
    }

    private void rehashCursor(Cursor cursor, Uri mUri) {
        if (cursor == null || cursor.getCount() < 1) {
            return;
        }
        String returnKey = null;
        String returnValue = null;
        if (cursor.moveToFirst()) {
            while (!cursor.isAfterLast()) {
                int keyIndex = cursor.getColumnIndex(LocalDHTable.COLUMN_KEY);
                int valueIndex = cursor
                        .getColumnIndex(LocalDHTable.COLUMN_VALUE);
                returnKey = cursor.getString(keyIndex);
                returnValue = cursor.getString(valueIndex);
                // We already know that updated predecessor so rehash each key
                // Delete and then insert
                try {
                    if (genHash(returnKey).compareTo(
                            nodePtr.getPredecessor_node_id()) <= 0) {
                        rehashNow(returnKey, returnValue, mUri);
                    }
                } catch (NoSuchAlgorithmException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                cursor.moveToNext();
            }
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
            String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    public void createServerSocket() {
        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that
             * listens on the server port.
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Log.d(TAG,
                    "Server Socket Created :: " + serverSocket.getInetAddress()
                            + ":" + serverSocket.getLocalPort());
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,
                    serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    // n' < key < n'.suc return true
    // if last sector then special case break the circle
    // if n' > n'.suc then last sector
    private boolean locateNode(Node n, Node nPrime) {

        if (nPrime.getNode_id().compareTo(nPrime.getSuccessor_node_id()) == 0) {
            return true;
        }
        // Last sector
        if (nPrime.getNode_id().compareTo(nPrime.getSuccessor_node_id()) >= 0) {
            // if key is less than current and greater than successor then it
            // does not belong here.
            if ((n.getNode_id().compareTo(nPrime.getSuccessor_node_id()) > 0) &&
                    (n.getNode_id().compareTo(nPrime.getNode_id()) <= 0)) {
                return false;
            } else {
                // if key is greater than current or less than successor then it
                // belongs to successor.
                return true;
            }
        } else if ((n.getNode_id().compareTo(nPrime.getSuccessor_node_id()) <= 0)
                && (n.getNode_id().compareTo(nPrime.getNode_id()) > 0)) {
            return true;
        } else {
            return false;
        }
    }

    private boolean locateQueryNode(Node n, Node nPrime) {
        // Last sector
        if (nPrime.getNode_id().compareTo(nPrime.getSuccessor_node_id()) == 0) {
            return true;
        }
        if (nPrime.getPredecessor_node_id().compareTo(nPrime.getNode_id()) >= 0) {
            // if key is less than current and greater than successor then it
            // does not belong here.
            if ((n.getNode_id().compareTo(nPrime.getNode_id()) > 0)
                    && (n.getNode_id().compareTo(nPrime.getPredecessor_node_id()) <= 0)) {
                return false;
            } else {
                // if key is greater than current or less than successor then it
                // belongs to successor.
                return true;
            }
        } else if ((n.getNode_id().compareTo(nPrime.getNode_id()) <= 0)
                && (n.getNode_id().compareTo(nPrime.getPredecessor_node_id()) > 0)) {
            return true;
        } else {
            return false;
        }
    }

    /***
     * ServerTask for processing client request @author dileepra
     */
    public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        static final String TAG = "ServerTask";

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes
             * them to onProgressUpdate().
             */
            Log.d(TAG, "Server waiting on " + serverSocket.getLocalPort());
            Socket tempSocket = null;
            ChordPDU tempMsg = null;
            while (true) {
                try {
                    tempSocket = serverSocket.accept();
                    Log.d(TAG,
                            "Server Connected to client port "
                                    + tempSocket.getRemoteSocketAddress());
                    ObjectInputStream in = new ObjectInputStream(
                            new BufferedInputStream(tempSocket.getInputStream()));
                    tempMsg = (ChordPDU) in.readObject();
                    switch (tempMsg.messageType) {
                        case JOIN:
                            new JoinTask().executeOnExecutor(
                                    AsyncTask.SERIAL_EXECUTOR, tempMsg);
                            break;
                        case NODE: // Update nodepointer
                            nodePtr.setSuccessor(
                                    tempMsg.messagePayload.getSuccessor_node_id(),
                                    tempMsg.messagePayload.getSuccessor_chord_id());
                            nodePtr.setPredecessor(tempMsg.messagePayload
                                    .getPredecessor_node_id(),
                                    tempMsg.messagePayload
                                            .getPredecessor_chord_id());
                            break;
                        case PREDECESSOR: // Update predecessor (message
                                          // intended for successor during join)
                            nodePtr.setPredecessor(tempMsg.messagePayload
                                    .getPredecessor_node_id(),
                                    tempMsg.messagePayload
                                            .getPredecessor_chord_id());
                            rehashDHT();
                            break;
                        case INSERT:
                            ContentValues cv = new ContentValues();
                            cv.put(LocalDHTable.COLUMN_KEY, tempMsg.messagePayload.getKey());
                            cv.put(LocalDHTable.COLUMN_VALUE, tempMsg.messagePayload.getValue());
                            Uri uri = Uri.parse(tempMsg.messagePayload.getUri());
                            insert(uri, cv);
                            break;
                        case INSERT_NOW:
                            ContentValues cv1 = new ContentValues();
                            cv1.put(LocalDHTable.COLUMN_KEY, tempMsg.messagePayload.getKey());
                            cv1.put(LocalDHTable.COLUMN_VALUE, tempMsg.messagePayload.getValue());
                            Uri uri2 = Uri.parse(tempMsg.messagePayload.getUri());
                            insertToDHT(uri2, cv1);
                            break;
                        case QUERY_ALL:
                        case QUERY:
                            Uri uri1 = Uri.parse(tempMsg.messagePayload.getUri());
                            queryDHT(tempMsg, uri1);
                            break;
                        case QUERY_RESP:
                            queryResponseQueue.put(tempMsg.messagePayload.getQueryCursor());
                            break;
                        case DELETE_ALL:
                        case DELETE:
                            Uri uri3 = Uri.parse(tempMsg.messagePayload.getUri());
                            deleteFromDHT(tempMsg, uri3);
                            break;
                        case DELETE_RESP:
                            deleteResponseQueue.put(tempMsg.messagePayload.getDeleteCount());
                            break;
                        default:
                            break;
                    }
                    in.close();
                } catch (IOException e) {
                    Log.e(TAG, "IOException ::: " + e.toString());
                    break;
                } catch (ClassNotFoundException e) {
                    Log.e(TAG, "ClassNotFoundException" + e.toString());
                    break;
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    break;
                } finally {
                    if (null != tempSocket) {
                        try {
                            tempSocket.close();
                        } catch (IOException e) {
                            Log.e(TAG,
                                    "Exception occured while closing Server Socket");
                        }
                    }
                }
            }

            return null;
        }
    }

    /**
     * Process Join Messages
     * 
     * @author dileepra
     */
    public class JoinTask extends AsyncTask<ChordPDU, String, Void> {

        @Override
        protected Void doInBackground(ChordPDU... params) {
            ChordPDU chordPDU = params[0];
            Node n = chordPDU.messagePayload;
            Node nPrime = nodePtr;

            // Join condition check for whether it belongs to current interval
            if (locateNode(n, nPrime)) {
                n.setSuccessor(nPrime.getSuccessor_node_id(),
                        nPrime.getSuccessor_chord_id());
                n.setPredecessor(nPrime.getNode_id(), nPrime.getChord_id());

                nodePtr.setSuccessor(n.getNode_id(), n.getChord_id());

                // Send node message to new node
                Integer remPort = Integer.parseInt(n.getChord_id()) * 2;
                String remotePortNo = remPort.toString();
                MessageParams nodeMsg = new MessageParams(n, remotePortNo, MESSAGE_TYPES.NODE);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeMsg);

                // Send node message to successor node to update its predecessor
                Node nSuccs = new Node(n.getSuccessor_node_id(), n.getSuccessor_chord_id(),
                        n.getNode_id(),
                        n.getChord_id(), "UNKNOWN", "UNKNOWN");
                remPort = Integer.parseInt(n.getSuccessor_chord_id()) * 2;
                remotePortNo = remPort.toString();
                nodeMsg = new MessageParams(nSuccs, remotePortNo, MESSAGE_TYPES.PREDECESSOR);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeMsg);
            } else { // Pass it onto next successor to find the actual successor
                Integer remPort = Integer.parseInt(nPrime.getSuccessor_chord_id()) * 2;
                String remotePortNo = remPort.toString();
                MessageParams nodeMsg = new MessageParams(n, remotePortNo, MESSAGE_TYPES.JOIN);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeMsg);
            }
            return null;
        }

    }

    public class ClientTask extends AsyncTask<MessageParams, Void, Void> {

        static final String TAG = "ClientTask";

        @Override
        protected Void doInBackground(MessageParams... msgs) {
            ChordPDU chordPDU = null;
            MessageParams mParams = msgs[0];
            chordPDU = new ChordPDU(mParams.messageType, mParams.n);
            sendMessage(chordPDU, mParams.remotePort);
            return null;
        }

        private void sendMessage(ChordPDU message, String remotePort) {
            Socket socket = null;
            // int timeout = 5000;
            try {
                socket = new Socket(
                        InetAddress.getByAddress(new byte[] {
                                10, 0, 2, 2
                        }),
                        Integer.parseInt(remotePort));
                // socket.setKeepAlive(true);
                Log.d(TAG, "Inside Client Task :: PDU=" + message.messageType.toString() + " :: "
                        + remotePort + " :: "
                        + socket.getLocalPort());

                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                if (socket.isConnected()) {
                    ObjectOutputStream clientOut = new ObjectOutputStream(
                            new BufferedOutputStream(socket.getOutputStream()));
                    clientOut.writeObject(message);
                    clientOut.flush();
                    clientOut.close();
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "Inside ClientTask SendMessage ::: -->" + e.toString());
                e.printStackTrace();
            } catch (Exception ex) {
                Log.e(TAG, ex.toString());
            } finally {
                if (null != socket) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        Log.e(TAG, "Exception occured while closing Client Socket");
                    }
                }

            }
        }

    }
}
