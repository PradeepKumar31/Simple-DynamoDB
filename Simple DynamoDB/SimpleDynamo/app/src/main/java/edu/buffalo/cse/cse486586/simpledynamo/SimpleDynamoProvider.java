package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;

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
    /*
    * Define all the variables needed for the program such as
    * QUEUE to store files
    * HASHPORT to keep the port and hash value of the port
    * PORTHASH to keep all the hash values of port in sorted order
    * READWRITELOCK to make QUEUE accesses protected
     */
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    Node NODE = new Node();
    ArrayList<String> PORTS = new ArrayList<String>(Arrays.asList("5554", "5556", "5558", "5560", "5562"));
    HashMap<String, String> QUEUE = new HashMap<String, String>();
    ArrayList<String> HASHPORT = new ArrayList<String>();
    HashMap<String, String> PORTHASH = new HashMap<String, String> ();
    ReadWriteLock READWRITELOCK = new ReentrantReadWriteLock();

    /*
    * If "*", it will call clientTask with a delete message. So all clients will get the message and delete files
    * If "@", we will simply clear the QUEUE for that particular port
    * else we will call clientTask with a delete message so all replicas of that file will be deleted
     */
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if (selection.equals("*")) {
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "2", NODE.getMyPort(), selection);
        } else if (selection.equals("@")) {
            QUEUE.clear();
        } else {
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "2", NODE.getMyPort(), selection);
        }
        return 0;
    }

    /*
    * Here we are simply storing the values in 3 different ports, which we are getting by calling a
    * function port.
     */
    @Override
    public  Uri insert(Uri uri, ContentValues values) {
        while(NODE.getFront() || NODE.getBack());
        String key = values.get("key").toString();
        String value = values.get("value").toString();

        for (int j = 0; j < 3; j++) {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        port(key, j));
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                writer.println(NODE.getMyPort() + "#" + key + ":" + value + "#1");
                socket.close();
            }  catch (Exception exception) {
                Log.e(TAG, "Insert Exception: " + exception.getMessage());
            }
        }
        return uri;
    }

    /*
    * If "*", it will send message to all the 5 ports and add the values to the cursor.
    * If "@", we simply read QUEUE and add the values to the cursor object.
    * else it will send message to 3 replicas port, which we will find out with port function.
     */
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        while(NODE.getFront() || NODE.getBack());
        MatrixCursor cursor = new MatrixCursor(new String[] {"key", "value"});
        if (selection.equals("@")) {
            for (Map.Entry<String, String> entry : QUEUE.entrySet()) {
                READWRITELOCK.readLock().lock();
                String[] query = new String[2];
                query[0] = entry.getKey();
                query[1] = QUEUE.get(entry.getKey());
                cursor.addRow(query);
                READWRITELOCK.readLock().unlock();
            }
        } else if (selection.equals("*")) {
            for (String port : PORTS) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[] {10, 0, 2, 2}),
                            Integer.parseInt(port) * 2);
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    writer.println(NODE.getMyPort() + "#" + selection + "#0");
                    String[] data = reader.readLine().split("#")[1].split(":");

                    for (int i = 0; i < data.length-1; i = i + 2) {
                        String[] query = new String[2];
                        query[0] = data[i];
                        query[1] = data[i+1];
                        cursor.addRow(query);
                    }
                    socket.close();
                } catch (Exception exception) {
                    Log.e(TAG, "Query Exception: " + exception.getMessage());
                }
            }
        } else {
            for(int k = 0; k < 3;  k++) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            port(selection, k));
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    writer.println(NODE.getMyPort() + "#" + selection + "#0");
                    String data = reader.readLine().split("#")[1];

                    if (data == null) {
                        socket.close();
                        continue;
                    }
                    socket.close();
                    String[] query = new String[2];
                    query[0] = selection;
                    query[1] = data;
                    cursor.addRow(query);
                    break;
                } catch (Exception exception) {
                    Log.e(TAG, "Query Exception: " + exception.getMessage());
                }
            }
        }
        return cursor;
    }

    /*
    * Here we are simply creating Node object and set all the values to it such as port, node, successor
    * and successor2 and then call clientTask for successor and successor2.
    * We will start the serverTask at the port 10000
     */
    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        NODE.setMyNode(portStr);
        NODE.setMyPort(myPort);

        try {
            for (String node : PORTS) {
                HASHPORT.add(genHash(node));
                PORTHASH.put(genHash(node), node);
            }
            Collections.sort(HASHPORT);
            NODE.setHashNode(genHash(NODE.getMyNode()));
            int port = Integer.parseInt(portStr);
            String successor;
            String successor2;

            if (port == 5554) {
                successor = "11116";
                successor2 = "11112";
            } else if (port == 5556) {
                successor = "11108";
                successor2 = "11124";
            } else if (port == 5558) {
                successor = "11120";
                successor2 = "11108";
            } else if (port == 5560) {
                successor = "11124";
                successor2 = "11116";
            } else {
                successor = "11112";
                successor2 = "11120";
            }

            NODE.setSuccessorPort(successor);
            NODE.setSuccessorPort2(successor2);
            NODE.setFront(true);
            NODE.setBack(true);
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "4", myPort);
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "5", myPort);
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (NoSuchAlgorithmException noSuchAlgorithmException) {
            Log.e(TAG, noSuchAlgorithmException.getMessage());
        } catch (IOException ioException) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
        return false;
    }

    /*
    * Here we will returns the index value at which the value to be inserted or if we want to delete
    * or query then at which place the value can be found
     */
    int orderedInsert(String key) {
        int index = 0;
        try {
            String position = genHash(key);
            for (; index < 5; index++) {
                if (checkCondition(position, index, (index+4)%5)) {
                    break;
                } else if (index == 0 && checkCondition1(position, 0, 4)) {
                    break;
                } else if (index == 0 && checkCondition2(position,0, 4)) {
                    break;
                }
            }
        } catch (NoSuchAlgorithmException noSuchAlgorithmException) {
            Log.e(TAG, noSuchAlgorithmException.getMessage());
        }
        return index;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String[] message = reader.readLine().split("#");
                    String newMessage = "";
                    while(NODE.getFront() || NODE.getBack());
                    if (message[2].equals("1")) {
                        READWRITELOCK.writeLock().lock();
                        QUEUE.put(message[1].split(":")[0], message[1].split(":")[1]);
                        READWRITELOCK.writeLock().unlock();
                    } else if (message[2].equals("0")) {
                        if (message[1].equals("*")) {
                            for (String key : QUEUE.keySet()) {
                                READWRITELOCK.readLock().lock();
                                if (!newMessage.equals(""))
                                    newMessage += ":" + key + ":" + QUEUE.get(key);
                                else
                                    newMessage = key + ":" + QUEUE.get(key);
                                READWRITELOCK.readLock().unlock();
                            }
                        } else {
                            READWRITELOCK.readLock().lock();
                            newMessage = QUEUE.get(message[1]);
                            READWRITELOCK.readLock().unlock();
                        }
                        writer.println(NODE.getMyPort() + "#" + newMessage + "#" + message[2]);
                    } else {
                        if (message[1].equals("*")) {
                            QUEUE.clear();
                        } else {
                            QUEUE.remove(message[1]);
                        }
                    }
                    socket.close();
                } catch (Exception exception) {
                    Log.e(TAG, "ServerTask Exception" + exception.getMessage());
                }
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {
            if (strings[0].equals("4")) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(NODE.getSuccessorPort2()));

                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    writer.println(strings[1] + "#*#0");
                    String[] data = reader.readLine().split("#")[1].split(":");

                    for (int i = 0; i < data.length - 1; i = i + 2) {
                        int index = orderedInsert(data[i]);

                        if (checkCondition(index)) {
                            READWRITELOCK.writeLock().lock();
                            QUEUE.put(data[i], data[i+1]);
                            READWRITELOCK.writeLock().unlock();
                        }
                    }
                    socket.close();
                } catch (Exception exception) {
                    Log.e(TAG, "ClientTask Exception" + exception.getMessage());
                }
                NODE.setBack(false);
            } else if (strings[0].equals("2")) {
                if (strings[2].equals("*")) {
                    for (int i = 0; i < 5; i++) {
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    port(strings[2], i));

                            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                            writer.println(NODE.getMyPort() + "#" + strings[2] + "#2");
                        } catch (Exception exception) {
                            Log.e(TAG, "ClientTask Exception" + exception.getMessage());
                        }
                    }
                } else {
                    for (int i = 0; i < 3; i++) {
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    port(strings[2], i));

                            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                            writer.println(NODE.getMyPort() + "#" + strings[2] + "#2");
                        } catch (Exception exception) {
                            Log.e(TAG, "ClientTask Exception" + exception.getMessage());
                        }
                    }
                }
                return null;
            } else {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(NODE.getSuccessorPort()));

                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    writer.println(strings[1] + "#*#0");
                    String[] data = reader.readLine().split("#")[1].split(":");

                    for (int i = 0; i < data.length-1; i = i + 2) {
                        int index = orderedInsert(data[i]);

                        if (checkCondition(index)) {
                            READWRITELOCK.writeLock().lock();
                            QUEUE.put(data[i], data[i+1]);
                            READWRITELOCK.writeLock().unlock();
                        }
                    }
                    socket.close();
                } catch (Exception exception) {
                    Log.e(TAG, "ClientTask Exception" + exception.getMessage());
                }
                NODE.setFront(false);
            }
            return null;
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

    int port (String key, int j) {
        return Integer.parseInt(PORTHASH.get(HASHPORT.get((orderedInsert(key) + j) % 5)))*2;
    }

    public boolean checkCondition (int index) {
        return HASHPORT.get(index).equals(NODE.getHashNode()) || HASHPORT.get((index+1)%5).equals(NODE.getHashNode()) || HASHPORT.get((index+2)%5).equals(NODE.getHashNode());
    }

    public boolean checkCondition (String position, int x, int y) {
        return  position.compareTo(HASHPORT.get(x)) <= 0 && position.compareTo(HASHPORT.get(y)) > 0;
    }

    public boolean checkCondition1 (String position, int x , int y) {
        return position.compareTo(HASHPORT.get(x)) >= 0 && position.compareTo(HASHPORT.get(y)) > 0;
    }

    public boolean checkCondition2 (String position, int x, int y) {
        return position.compareTo(HASHPORT.get(x)) <= 0 && position.compareTo(HASHPORT.get(y)) < 0;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }
}