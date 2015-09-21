package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    private static final String TAG = "SimpleDynamoProvider";
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private ArrayList<Node> nodeList;
    private ArrayList<String> locallyStoredKeysPrimary,
            locallyStoredKeysSecondary,
            locallyStoredKeysTertiary;

    private int currentNode, UID = 0;
    private boolean updatingKeys = true;

    private HashMap<Integer, ArrayList<String>> messageReplies;
    private ArrayList<String> queuedMessages;

    /*
     * Message Type Definitions
     */

    private static final String INSERT = "IN";
    private static final String QUERY = "QU";
    private static final String QUERY_SECONDARY = "QS";
    private static final String QUERY_REPLY = "QR";
    private static final String QUERY_ALL = "QA";
    private static final String QUERY_ALL_REPLY = "QX";
    private static final String DELETE = "DE";
    private static final String DELETE_ALL = "DA";
    private static final String ACKNOWLEDGEMENT = "ACK";
    private static final String REQUEST_PRIMARIES = "RQP";
    private static final String REQUEST_SECONDARIES = "RQS";
    private static final String KEY_DUMP = "KD";


    @Override
    public boolean onCreate() {

        /**
         * Initializations
         */
        nodeList = new ArrayList<>();
        locallyStoredKeysPrimary = new ArrayList<>();
        locallyStoredKeysSecondary = new ArrayList<>();
        locallyStoredKeysTertiary = new ArrayList<>();
        messageReplies = new HashMap<>();
        queuedMessages = new ArrayList<>();

        nodeList.add(new Node("5554"));
        nodeList.add(new Node("5556"));
        nodeList.add(new Node("5558"));
        nodeList.add(new Node("5560"));
        nodeList.add(new Node("5562"));

        Collections.sort(nodeList);
        Log.i("ON_CREATE", "Content Provider initialized. nodeList: "+Arrays.toString(nodeList.toArray()));

        /**
         * Get the current port number
         */
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        for(int i=0; i<nodeList.size(); i++) {
            if(nodeList.get(i).id.equals(portStr)) {
                currentNode = i;
                break;
            }
        }

        Log.i("ON_CREATE", "PortStr: "+portStr+"  Current Node: "+currentNode);

        /**
         * Initiate a server to listen for routing messages and updates
         */
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(10000);
        } catch (IOException e) {
            Log.e("ON_CREATE", "IOException on ServerSocket creation");
        }

        if(serverSocket != null) {
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.i("ON_CREATE", "Node created server on port 10000");
        }


        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        updateKeys();


        /**
         * Done
         */
        return true;
    }

    private void updateKeys() {

        int localUid = UID;
        UID++;

        Log.i("UPDATE_KEYS", "Update Keys");
        ArrayList<String> replies = new ArrayList<>();
        messageReplies.put(localUid, replies);
        String requestPrimaries = REQUEST_PRIMARIES + "|" +
                localUid + "|" +
                getNodeId(currentNode);

        String requestSecondaries = REQUEST_SECONDARIES + "|" +
                localUid + "|" +
                getNodeId(currentNode);

        String previous = getNodeId(( (currentNode - 1) + nodeList.size() ) % nodeList.size());
        String prevToPrevious = getNodeId(( (currentNode - 2) + nodeList.size() ) % nodeList.size());
        String next = getNodeId((currentNode + 1 ) % nodeList.size());

        sendMessage(requestPrimaries, previous);
        sendMessage(requestPrimaries, prevToPrevious);
        sendMessage(requestSecondaries, next);

        int timeout = 0;
        while(replies.size() < 3) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            timeout++;
            if(timeout >= 10) {
                break;
            }
        }

        Log.i("UPDATE_KEYS", "Got all replies");

        String pString = "", sString = "", tString = "";

        for(String reply : replies) {
            String replier = reply.split("\\|")[2];
            if(replier.equals(previous)) {
                try {
                    sString = reply.split("\\|")[3];
                } catch (ArrayIndexOutOfBoundsException e) {

                }
            }
            else if(replier.equals(prevToPrevious)) {
                try {
                    tString = reply.split("\\|")[3];
                } catch (ArrayIndexOutOfBoundsException e) {

                }
            }
            else if(replier.equals(next)) {
                try {
                    pString = reply.split("\\|")[3];
                } catch (ArrayIndexOutOfBoundsException e) {

                }
            }
        }

        if(pString.length() > 0) {
            String[] primaries = pString.split(";");

            for(String keyValPair : primaries) {
                String key = keyValPair.split(":")[0];
                String value = keyValPair.split(":")[1];

                writeKeyValuePair(key, value);
                locallyStoredKeysPrimary.add(key);
            }
        }

        if(sString.length() > 0) {
            String[] secondaries = sString.split(";");

            for(String keyValPair : secondaries) {
                String key = keyValPair.split(":")[0];
                String value = keyValPair.split(":")[1];

                writeKeyValuePair(key, value);
                locallyStoredKeysSecondary.add(key);
            }
        }

        if(tString.length() > 0) {
            String[] tertiaries = tString.split(";");

            for(String keyValPair : tertiaries) {
                String key = keyValPair.split(":")[0];
                String value = keyValPair.split(":")[1];

                writeKeyValuePair(key, value);
                locallyStoredKeysTertiary.add(key);
            }
        }

        updatingKeys = false;
        Log.i("UPDATE_KEYS", "Update done, processing queued messages... "+Arrays.toString(queuedMessages.toArray()));
        for(String m : queuedMessages) {
            new HandlerTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);
        }
    }


    /**
     * ServerTask
     */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Log.i("SERVER_TASK", "Server listening on " + serverSocket.getLocalPort());

            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String message = br.readLine();

                    Log.i("SERVER_TASK", "Received message: " + message);

                    new HandlerTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

                    br.close();
                    socket.close();
                }
            } catch (IOException e) {
                Log.e("SERVER_TASK", "IOException on ServerTask");
            }

            return null;
        }

    }


    private class HandlerTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... params) {
            String message = params[0];

            /**
             * Appropriate Handlers
             */
            if(message == null) {
                return null;
            }

            String[] messageFields = message.split("\\|");
            Log.i("HANDLER", "Received message: "+message);
            if(updatingKeys && !(messageFields[0].equals(REQUEST_PRIMARIES)
                    || messageFields[0].equals(REQUEST_SECONDARIES) || messageFields[0].equals(KEY_DUMP))) {
                Log.i("HANDLER", "Queuing message "+message+", still handling update");
                queuedMessages.add(message);
                return null;
            }
            Log.i("HANDLER", "Processing message: "+message);

            switch (messageFields[0]) {
                case INSERT:
                    handleInsert(message);
                    break;
                case QUERY:
                    handleQuery(message);
                    break;
                case QUERY_SECONDARY:
                    handleQuerySecondary(message);
                    break;
                case QUERY_REPLY:
                    handleQueryReply(message);
                    break;
                case QUERY_ALL:
                    handleQueryAll(message);
                    break;
                case QUERY_ALL_REPLY:
                    handleQueryAllReply(message);
                    break;
                case DELETE:
                    handleDelete(message);
                    break;
                case DELETE_ALL:
                    handleDeleteAll();
                    break;
                case ACKNOWLEDGEMENT:
                    handleAcknowledgement(message);
                    break;
                case REQUEST_PRIMARIES:
                case REQUEST_SECONDARIES:
                    handleKeyRequest(message);
                    break;
                case KEY_DUMP:
                    handleKeyDump(message);
                    break;
            }

            return null;
        }

        private void handleInsert(String message) {
            String[] messageFields = message.split("\\|");
            String reqUid = messageFields[1];
            String requester = messageFields[2];
            String key = messageFields[3];
            String value = messageFields[4];
            int counter = Integer.parseInt(messageFields[5]);

            Log.i("HANDLE_INSERT", "Writing key "+key+" to node "+nodeList.get(currentNode).id);
            writeKeyValuePair(key, value);

            switch(counter) {
                case 3:
                    Log.i("HANDLE_INSERT", "PRIMARY");
                    locallyStoredKeysPrimary.add(key);
                    break;
                case 2:
                    Log.i("HANDLE_INSERT", "SECONDARY");
                    locallyStoredKeysSecondary.add(key);
                    break;
                case 1:
                    Log.i("HANDLE_INSERT", "TERTIARY");
                    locallyStoredKeysTertiary.add(key);
                    break;
            }

            if(counter > 1) {
                int localUid = UID;
                UID++;

                ArrayList<String> ack = new ArrayList<>();
                messageReplies.put(localUid, ack);

                String ackReply = ACKNOWLEDGEMENT + "|"
                        + reqUid;

                sendMessage(ackReply, requester);

                String forwardMsg = INSERT +"|"
                        + localUid + "|"
                        + getNodeId(currentNode) + "|"
                        + key + "|"
                        + value + "|"
                        + String.valueOf(counter-1);

                String chainSuccessor = nodeList.get((currentNode + 1) % nodeList.size()).id;
                Log.i("HANDLE_INSERT", "Forwarding message "+forwardMsg+" to chain successor "+chainSuccessor);
                sendMessage(forwardMsg, chainSuccessor);

                if(counter == 3) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if(ack.size() < 1) {
                        forwardMsg = INSERT +"|"
                                + localUid + "|"
                                + getNodeId(currentNode) + "|"
                                + key + "|"
                                + value + "|"
                                + String.valueOf(1);

                        sendMessage(forwardMsg, nodeList.get((currentNode + 2) % nodeList.size()).id);
                    }
                    else {
                        Log.i("HANDLE_INSERT", "Received ack");
                    }
                }

            }
        }

        private void handleQuery(String message) {
            Log.i("HANDLE_QUERY", "Got message "+message);
            String[] messageFields = message.split("\\|");
            String key = messageFields[1];
            String requester = messageFields[2];
            String reqUid = messageFields[3];

            String reply = QUERY_REPLY + "|"
                    + reqUid + "|";

            Cursor cur = query(null, null, key, null, null);
            while(cur != null && cur.moveToNext()) {
                reply += cur.getString(0) + ":" + cur.getString(1) + ";";
            }
            if(cur != null) {
                cur.close();
            }

            Log.i("HANDLE_QUERY", "Replying with "+reply);

            sendMessage(reply, requester);
        }

        private void handleQuerySecondary(String message) {
            Log.i("HANDLE_QUERY_SECONDARY", "Got message "+message);
            String[] messageFields = message.split("\\|");
            String key = messageFields[1];
            String requester = messageFields[2];
            String reqUid = messageFields[3];

            String reply = QUERY_REPLY + "|"
                    + reqUid + "|";

            String value = null;
            try {
                value = readKeyValuePair(key);
            } catch (FileNotFoundException e) {
                Log.i("HANDLE_QUERY_SECONDARY", "FILE NOT FOUND!");
            }
            reply +=key + ":" + value + ";";

            Log.i("HANDLE_QUERY_SECONDARY", "Replying with "+reply);

            sendMessage(reply, requester);
        }

        private void handleQueryReply(String message) {
            String[] messageFields = message.split("\\|");
            Integer uid = Integer.parseInt(messageFields[1]);

            Log.i("HANDLE_QUERY_REPLY", "Got reply for uid: "+uid);
            messageReplies.get(uid).add(message);
        }

        private void handleQueryAll(String message) {
            Log.i("HANDLE_QUERY_ALL", "Got message "+message);
            String[] messageFields = message.split("\\|");
            String requester = messageFields[1];
            String reqUid = messageFields[2];

            String reply = QUERY_ALL_REPLY + "|"
                    + reqUid + "|"
                    + nodeList.get(currentNode).id + "|";

            Cursor cur = query(null, null, "\"@\"", null, null);
            while(cur.moveToNext()) {
                reply += cur.getString(0) + ":" + cur.getString(1) + ";";
            }
            cur.close();

            Log.i("HANDLE_QUERY_ALL", "Replying with "+reply);

            sendMessage(reply, requester);
        }

        private void handleQueryAllReply(String message) {
            String[] messageFields = message.split("\\|");
            Integer reqUid = Integer.parseInt(messageFields[1]);
            String replier = messageFields[2];

            Log.i("HANDLE_QUERY_ALL_REPLY", "Got reply for uid: "+reqUid+" from "+replier);

            messageReplies.get(reqUid).add(message);
        }

        private void handleDelete(String message) {
            String[] messageFields = message.split("\\|");
            String reqUid = messageFields[1];
            String requester = messageFields[2];
            String key = messageFields[3];
            int counter = Integer.parseInt(messageFields[4]);

            Log.i("HANDLE_DELETE", "Deleting key "+key+" from node "+getNodeId(currentNode));

            deleteKeyValuePair(key);

            switch (counter) {
                case 3:
                    Log.i("HANDLE_DELETE", "PRIMARY");
                    locallyStoredKeysPrimary.remove(key);
                    break;
                case 2:
                    Log.i("HANDLE_DELETE", "SECONDARY");
                    locallyStoredKeysSecondary.remove(key);
                    break;
                case 1:
                    Log.i("HANDLE_DELETE", "TERTIARY");
                    locallyStoredKeysTertiary.remove(key);
                    break;
            }

            if(counter >= 2) {
                int localUid = UID;
                UID++;

                ArrayList<String> ack = new ArrayList<>();
                messageReplies.put(localUid, ack);

                String ackReply = ACKNOWLEDGEMENT + "|"
                        + reqUid;

                sendMessage(ackReply, requester);

                String forwardMsg = DELETE +"|"
                        + localUid + "|"
                        + getNodeId(currentNode) + "|"
                        + key + "|"
                        + String.valueOf(counter-1);

                sendMessage(forwardMsg, getNodeId((currentNode + 1) % nodeList.size()));


                if(counter == 3) {

                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if(ack.size() < 1) {

                        Log.i("HANDLE_DELETE", "Timed out");
                        forwardMsg = DELETE +"|"
                                + localUid + "|"
                                + getNodeId(currentNode) + "|"
                                + key + "|"
                                + String.valueOf(1);

                        sendMessage(forwardMsg, nodeList.get((currentNode + 2) % nodeList.size()).id);
                    }
                    else {
                        Log.i("HANDLE_DELETE", "Received ack");
                    }
                }
            }
        }

        private void handleDeleteAll() {
            delete(null, "\"@\"", null);
        }

        private void handleAcknowledgement(String message) {
            String[] messageFields = message.split("\\|");
            Integer reqUid = Integer.parseInt(messageFields[1]);

            messageReplies.get(reqUid).add(message);
        }

        private void handleKeyRequest(String message) {
            String[] messageFields = message.split("\\|");
            String requestedKeyType = messageFields[0];
            String reqUid = messageFields[1];
            String requester = messageFields[2];

            String reply = KEY_DUMP + "|"
                    + reqUid + "|"
                    + getNodeId(currentNode) + "|";

            ArrayList<String> keys = null;

            switch(requestedKeyType) {
                case REQUEST_PRIMARIES:
                    keys = locallyStoredKeysPrimary;
                    break;
                case REQUEST_SECONDARIES:
                    keys = locallyStoredKeysSecondary;
                    break;
            }

            ArrayList<String> keysToBeRemoved = new ArrayList<>();

            for(String key : keys) {
                String value = null;
                try {
                    value = readKeyValuePair(key);
                } catch (FileNotFoundException e) {

                }
                if(value != null) {
                    reply += key + ":" + value + ";";
                }
                else {
                    keysToBeRemoved.add(key);
                }
            }

            for(String key : keysToBeRemoved) {
                keys.remove(key);
            }

            sendMessage(reply, requester);
        }

        private void handleKeyDump(String message) {
            Log.i("HANDLE_KEY_DUMP", "Got message "+message);
            String[] messageFields = message.split("\\|");
            Integer reqUid = Integer.parseInt(messageFields[1]);

            messageReplies.get(reqUid).add(message);
        }
    }




    /**
     * Provider API
     */

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        String key = values.getAsString(KEY_FIELD);
        String value = values.getAsString(VALUE_FIELD);
        String keyHash = genHash(key);

        int localUID = UID;
        UID++;

        ArrayList<String> ack = new ArrayList<>();
        messageReplies.put(localUID, ack);

        Log.i("INSERT", "Trying to insert Key: "+key+"  Value: "+value+"  Hash: "+keyHash);

        String coordinator = getNodeId(findnthSuccessor(keyHash, 0));
        Log.i("INSERT", "Coordinator: " + coordinator);

        String message = INSERT + "|"
                + localUID + "|"
                + getNodeId(currentNode) + "|"
                + key + "|"
                + value + "|"
                + String.valueOf(3);

        sendMessage(message, coordinator);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if(ack.size() < 1) {
            Log.i("INSERT", "Coordinator did not send ack for insert key "+key);
            message = INSERT + "|"
                    + localUID + "|"
                    + getNodeId(currentNode) + "|"
                    + key + "|"
                    + value + "|"
                    + String.valueOf(2);

            String coOrdSucc = getNodeId(findnthSuccessor(keyHash, 1));
            Log.i("INSERT", "Sending message "+message+" to "+coOrdSucc);

            sendMessage(message, coOrdSucc);

        }
        else {
            Log.i("INSERT", "Coordinator acknowledged for key "+key);
        }

        return uri;
	}

    @Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

        int localUid = UID;
        UID++;

        if(selection.equals("\"@\"")) {
            Log.i("QUERY", "@ query");
            MatrixCursor cur = new MatrixCursor(
                    new String[]{KEY_FIELD, VALUE_FIELD} ,
                    locallyStoredKeysPrimary.size() + locallyStoredKeysSecondary.size() + locallyStoredKeysTertiary.size()
            );

            ArrayList<String> rem1 = new ArrayList<>();
            ArrayList<String> rem2 = new ArrayList<>();
            ArrayList<String> rem3 = new ArrayList<>();

            for(String key : locallyStoredKeysPrimary) {
                try {
                    cur.addRow(new String[]{key, readKeyValuePair(key)});
                } catch (FileNotFoundException e) {
                    Log.i("QUERY", "Key "+key+" was not found, removing from primary");
                    rem1.add(key);
                }
            }
            for(String key : locallyStoredKeysSecondary) {
                try {
                    cur.addRow(new String[]{key, readKeyValuePair(key)});
                } catch (FileNotFoundException e) {
                    Log.i("QUERY", "Key "+key+" was not found, removing from secondary");
                    rem2.add(key);
                }
            }
            for(String key : locallyStoredKeysTertiary) {
                try {
                    cur.addRow(new String[]{key, readKeyValuePair(key)});
                } catch (FileNotFoundException e) {
                    Log.i("QUERY", "Key "+key+" was not found, removing from tertiary");
                    rem3.add(key);
                }
            }

            for(String key:rem1) {
                locallyStoredKeysPrimary.remove(key);
            }
            for(String key:rem2) {
                locallyStoredKeysSecondary.remove(key);
            }
            for(String key:rem3) {
                locallyStoredKeysTertiary.remove(key);
            }

            return cur;
        }

        if(selection.equals("\"*\"")) {
            Log.i("QUERY", "* query");

            ArrayList<String> replies = new ArrayList<>();
            messageReplies.put(localUid, replies);

            String message = QUERY_ALL + "|"
                    + nodeList.get(currentNode).id + "|"
                    + localUid;

            for(Node n : nodeList) {
                sendMessage(message, n.id);
            }

            while(replies.size() < 4) {
                Log.i("QUERY", "Waiting for 4 replies, got: "+replies.size()+" Messages: "+Arrays.toString(replies.toArray()));
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            MatrixCursor mCur = new MatrixCursor(new String[] {KEY_FIELD, VALUE_FIELD});
            HashSet<String> seenKeys = new HashSet<>();

            for(String reply : replies) {
                String[] keyValPairs = reply.split("\\|")[3].split(";");

                Log.i("QUERY", "Received from "+reply.split("\\|")[2]+" keyVal pairs: "+Arrays.toString(keyValPairs));

                for(String keyValPair : keyValPairs) {
                    String key = keyValPair.split(":")[0];

                    if(!seenKeys.contains(key)) {
                        String value = keyValPair.split(":")[1];
                        mCur.addRow(new String[]{key, value});
                        seenKeys.add(key);
                    }
                }
            }

            return mCur;
        }

        Log.i("QUERY", "Key: " + selection);
        boolean exitFlag = false;

        while(true) {
            if(locallyStoredKeysTertiary.contains(selection)) {
                Log.i("QUERY", "Found Locally");
                MatrixCursor mCur = new MatrixCursor(new String[] {KEY_FIELD, VALUE_FIELD});
                String value = null;
                try {
                    value = readKeyValuePair(selection);
                } catch (FileNotFoundException e) {
                    Log.i("QUERY", "FILE NOT FOUND!");
                }

                mCur.addRow(new String[] {selection, value});

                return mCur;
            }
            else if(!exitFlag) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                exitFlag = true;
            }
            else {
                break;
            }
        }


        ArrayList<String> replies = new ArrayList<>();
        messageReplies.put(localUid, replies);

        String message = QUERY + "|"
                + selection + "|"
                + getNodeId(currentNode) + "|"
                + localUid;

        String chainTail = getNodeId(findnthSuccessor(genHash(selection), 2));

        sendMessage(message, chainTail);

        Log.i("QUERY", "Key "+selection+" not found locally, sending message: "+ message+" to "+chainTail);


        int timeout = 0;
        while(replies.size() < 1) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            timeout++;
            if(timeout == 10) {
                break;
            }
        }

        if(replies.size() < 1) {
            Log.i("QUERY", "Tail node "+chainTail+" timed out");
            String alternate = getNodeId(findnthSuccessor(genHash(selection), 1));

            message = QUERY_SECONDARY + "|"
                    + selection + "|"
                    + getNodeId(currentNode) + "|"
                    + localUid;

            Log.i("QUERY", "Querying from "+alternate+" key "+selection+" Waiting for reply indefinitely...");

            sendMessage(message, alternate);

            while(replies.size() < 1) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        MatrixCursor mCur = new MatrixCursor(new String[] {KEY_FIELD, VALUE_FIELD});
        String keyValPair = replies.get(0).split("\\|")[2].split(";")[0];

        Log.i("QUERY", "Received keyVal pair: "+keyValPair);

        String key = keyValPair.split(":")[0];
        String value = keyValPair.split(":")[1];

        mCur.addRow(new String[]{key, value});

        return mCur;
	}

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        if(selection.equals("\"@\"")) {
            deleteAll();
            return 0;
        }

        if(selection.equals("\"*\"")) {
            deleteStar();
            return 0;
        }

        int localUID = UID;
        UID++;
        ArrayList<String> ack = new ArrayList<>();
        messageReplies.put(localUID, ack);
        int successor = findnthSuccessor(genHash(selection), 0);

        Log.i("DELETE", "Trying to delete Key: "+selection+" Coordinator: "+successor);

        String message = DELETE + "|"
                + localUID + "|"
                + getNodeId(currentNode) +"|"
                + selection + "|"
                + String.valueOf(3);

        sendMessage(message, nodeList.get(successor).id);


        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        if(ack.size() < 1) {
            Log.i("DELETE", "Coordinator did not send ack for delete key "+selection);
            message = DELETE + "|"
                    + localUID + "|"
                    + getNodeId(currentNode) +"|"
                    + selection + "|"
                    + String.valueOf(2);

            String coOrdSucc = getNodeId(findnthSuccessor(genHash(selection), 1));
            Log.i("DELETE", "Sending message "+message+" to "+coOrdSucc);

            sendMessage(message, coOrdSucc);

        }
        else {
            Log.i("DELETE", "Coordinator acknowledged for key "+selection);
        }

        return 1;
    }

    @Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		return 0;
	}

    @Override
    public String getType(Uri uri) {
        return null;
    }




    /**
     * Helpers
     */

    private void sendMessage(String message, String destinationPort) {
        String addressedMessage = destinationPort
                + "$"               /* Field Separator */
                + message;

        new SenderTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, addressedMessage);
    }

    private String getNodeId(int index) {
        return nodeList.get(index).id;
    }

    public static String genHash(String input) {

        MessageDigest sha1 = null;

        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "NoSuchAlgorithmException at genHash");
        }

        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }

        return formatter.toString();
    }

    private int findnthSuccessor(String keyHash, int n) {
        for(int i=0; i<nodeList.size(); i++) {
            if(liesInInterval(
                    keyHash,
                    nodeList.get(i).hash,
                    nodeList.get((i+1) % nodeList.size()).hash)
                    ) {



                return (i+1+n) % nodeList.size();
            }
        }

        return -1;
    }

    private boolean liesInInterval(String hashToCheck, String hashCurrent, String hashNext) {
        if(hashCurrent.compareToIgnoreCase(hashNext) <= 0) {
            /**
             * Normal Case
             */
            return (hashToCheck.compareToIgnoreCase(hashCurrent) > 0
                    && hashToCheck.compareToIgnoreCase(hashNext) <= 0);

        }
        else {
            /**
             * Wraparound case
             */
            return (hashToCheck.compareToIgnoreCase(hashCurrent) > 0
                    || hashToCheck.compareToIgnoreCase(hashNext) <= 0);
        }
    }

    private void writeKeyValuePair(String key, String value) {
        try {
            BufferedWriter bw = new BufferedWriter(
                    new OutputStreamWriter(
                            getContext().openFileOutput(key, Context.MODE_PRIVATE)
                    )
            );

            bw.write(value+"\n");
            bw.close();
        }
        catch (IOException e) {
            Log.e("WRITE_KV_PAIR", "IOException on key file write");
            e.printStackTrace();
        }
    }

    private String readKeyValuePair(String key) throws FileNotFoundException {
        String value = null;

        BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        getContext().openFileInput(key)
                )
        );
        try {
            value = br.readLine();
            br.close();
        } catch (IOException e) {
            Log.i("READ_KV_PAIR", "IOException");
        }

        return value;
    }

    private void deleteKeyValuePair(String key) {
        Log.i("DELETE_KV_PAIR", "Deleting key: "+key);

        getContext().deleteFile(key);
    }

    private void deleteAll() {
        for(String key: locallyStoredKeysPrimary) {
            locallyStoredKeysPrimary.remove(key);
            deleteKeyValuePair(key);
        }
        for(String key: locallyStoredKeysSecondary) {
            locallyStoredKeysSecondary.remove(key);
            deleteKeyValuePair(key);
        }
        for(String key: locallyStoredKeysTertiary) {
            locallyStoredKeysTertiary.remove(key);
            deleteKeyValuePair(key);
        }
    }

    private void deleteStar() {
        for(Node node : nodeList) {
            String message = DELETE_ALL;
            sendMessage(message, node.id);
        }
    }



    /**
     * SenderTask
     */
    private class SenderTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... message) {
            String[] messageFields = message[0].split("\\$");
            int sendPort = Integer.parseInt(messageFields[0]) * 2;
            String messageToSend = messageFields[1];

            Log.i("SENDER_TASK", "Sending message "+messageToSend+" to "+sendPort);

            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        sendPort);

                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                bw.write(messageToSend);

                bw.close();
                socket.close();

            }
            catch (IOException e) {
                Log.e("SENDER_TASK", "IOException on SenderTask");
                e.printStackTrace();
            }

            return null;
        }
    }
}
