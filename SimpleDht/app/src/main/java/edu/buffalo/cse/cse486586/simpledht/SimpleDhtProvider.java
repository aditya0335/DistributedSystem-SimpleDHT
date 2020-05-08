package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import android.telephony.TelephonyManager;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import static android.content.Context.MODE_PRIVATE;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();

//    Global Variables
    String myPort = null;
    int myPortnumber;
    String myPortId;
    Boolean isJoined=false;
    String successor=null;
    String successorID;
    String predecessor=null;
    String predecessorId;
    String PORT0="11108";

    static final int SERVER_PORT = 10000;
//    Hashtable<String, String> localmessages
//            = new Hashtable<String, String>();
    Hashtable<String, String> querymessages
            = new Hashtable<String, String>();
    Hashtable<String, String> remotemessage
            = new Hashtable<String, String>();
    private static Map<String,String> localmessages = new ConcurrentHashMap<String, String>();

    Boolean isDeleting=false;
    Boolean isQuyering=true;
    private final Object lock = new Object();



    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {             //Query implement *(gloabal) and @(local) as selection parameter
        // TODO Auto-generated method stub

        if(selection.equals("@")){
            localmessages.clear();
        }
        else if(selection.equals("*")){
            localmessages.clear();
//          Send delete all message to successor
            isDeleting=true;
            if(successor!=null){
            message msg=new message(message.type.ALLDELETE,myPort,null,null,null,null,null);}

        }
        else{
            if(localmessages.containsKey(selection)){

                localmessages.remove(selection);}
            else{
                String hashkey=null;
                try {
                    hashkey=genHash(selection);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                message msg=new message(message.type.DELETE,myPort,selection,null,hashkey);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
                isDeleting=true;

            }

        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String keyid=null;
        String key=(String) values.get("key");
        try {
            keyid=genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        String value=(String) values.get("value");

        if(successor==null||predecessor==null)
        {localmessages.put(key,value);}
        else if((predecessorId.compareTo(keyid)<0 &&myPortId.compareTo(keyid)>=0)||
                (predecessorId.compareTo(myPortId)>0&&keyid.compareTo(myPortId)>0&&keyid.compareTo(predecessorId)>0)||
                (predecessorId.compareTo(myPortId)>0&&keyid.compareTo(myPortId)<0&&keyid.compareTo(predecessorId)<0)){
            localmessages.put(key,value);
        }
        else{
            message msg=new message(message.type.INSERT,myPort,key,value,keyid);
            lookupinsert(msg);
        }

        return uri;

    }


    private void lookupinsert(message msg){
        String type="LOOKUPJOIN";
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
    }

    @Override
    public boolean onCreate() {

//      To find port no.
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        myPortnumber=Integer.parseInt(myPort)/2;
        try {
            myPortId=genHash(Integer.toString(myPortnumber));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.d(TAG,"called server"+myPort);
        } catch (IOException e) {
            Log.d(TAG, "AVD- " + myPort + "OnCreate Process ErrorLogNo-1 " + "can't create ServerSocket");
            e.printStackTrace();
        }

        Log.d(TAG,"connecting chord---"+myPort);
        Log.d(TAG,"peredecessor chord---"+myPort+"with pred"+predecessor);
        Log.d(TAG,"peredecessor chord---"+myPort+"with success"+successor);
//      Join with chord
        if(!isJoined && !myPort.equals("11108")){

            join();
        }

        // TODO Auto-generated method stub
        return true;
    }

    private void join(){
        String type="FIRSTJOIN";
        message msg=new message(message.type.FIRSTJOIN,myPort,null,null,myPortId,null,null);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
    }

    private void lookupjoin(message msg){
        String type="LOOKUPJOIN";
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
    }
    private void foundjoin(message msg){
        String type="FOUNDJOIN";

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
    }

    private void lookup(message msg){
        String type="LOOKUP";
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,String sortOrder){

        MatrixCursor matrixCursor=new MatrixCursor(new String[]{"key","value"});


        if(selection.equals("@")){
            for (Map.Entry<String,String> entry : localmessages.entrySet())
            {matrixCursor.newRow().add("key",entry.getKey()).add("value",entry.getValue()); }
        }


        else if(selection.equals("*")){

            Hashtable<String, String> queryall
                    = new Hashtable<String, String>();
            queryall.putAll(localmessages);
            querymessages.putAll(localmessages);
            if(successor!=null){
                message msg=new message(message.type.ALLQUERY,myPort,null,null,queryall);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
                while(isQuyering){}
            }

            for (Map.Entry<String,String> entry : querymessages.entrySet())
            {matrixCursor.newRow().add("key",entry.getKey()).add("value",entry.getValue()); }
            querymessages.clear();
        }

        else{

            if(localmessages.containsKey(selection)){

                matrixCursor.newRow().add("key",selection).add("value",localmessages.get(selection));}
            else{
                String hashkey=null;
                try {
                    hashkey=genHash(selection);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                message msg=new message(message.type.QUERY,myPort,selection,null,hashkey);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
                synchronized (lock){
                while(isQuyering){
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }}
                isQuyering=true;

                matrixCursor.newRow().add("key",selection).add("value",querymessages.get(selection));
                querymessages.clear();
            }

        }


        Log.v("query", selection);
        return matrixCursor;
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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




    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {         //Server Class

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {

            ServerSocket serverSocket = serverSockets[0];

            while (true){

                Socket clientSocket = null;
                try{
                    Log.d(TAG,"in server"+myPort);
                    clientSocket = serverSocket.accept();
                    Log.d(TAG,"socket accepted"+myPort);
                    InputStream is = clientSocket.getInputStream();  //Opening Input Stream
                    ObjectInputStream ois_server = new ObjectInputStream(is);
                    message servermsgnew = (message) ois_server.readObject();
                    Log.d(TAG,"Port: " +servermsgnew.myport + " MsgType: " +servermsgnew.getValue());

                    if(servermsgnew.getValue()==message.type.FIRSTJOIN||servermsgnew.getValue()==message.type.LOOKUPJOIN){
                        if(predecessor==null||successor==null){
                            predecessor=servermsgnew.getMyport();
                            predecessorId=servermsgnew.getMyportid();
                            servermsgnew.setPredecessor(myPort);
                            servermsgnew.setPrecessorid(myPortId);
                            servermsgnew.setSuccessor(myPort);
                            servermsgnew.setSuccessorid(myPortId);
                            successor=servermsgnew.getMyport();
                            successorID=servermsgnew.getMyportid();
                            servermsgnew.setValue(message.type.FOUNDJOIN);
                            foundjoin(servermsgnew);
                            Log.d(TAG,"chord_node_pres||succ=null---"+myPort+"with successor--"+successor);
                            Log.d(TAG,"chord_node_pres||succ=null---"+myPort+"with predecessor--"+predecessor);
                            Log.d(TAG,"chord_node_pres||succ=null---"+servermsgnew.getMyport()+"with successor--"+servermsgnew.getSuccessor());
                            Log.d(TAG,"chord_node_pres||succ=null---"+servermsgnew.getMyport()+"with predecessor--"+servermsgnew.getSuccessor());
//                                servermsgnew.setValue(message.type.UPDATEJOIN);
//                                foundjoin(servermsgnew);

                        }
                        else if((predecessorId.compareTo(servermsgnew.getMyportid())<0 &&myPortId.compareTo(servermsgnew.getMyportid())>=0)||
                                (predecessorId.compareTo(myPortId)>0&&servermsgnew.getMyportid().compareTo(myPortId)>0&&servermsgnew.getMyportid().compareTo(predecessorId)>0)||
                                (predecessorId.compareTo(myPortId)>0&&servermsgnew.getMyportid().compareTo(myPortId)<0&&servermsgnew.getMyportid().compareTo(predecessorId)<0)){
                            servermsgnew.setPredecessor(predecessor);
                            servermsgnew.setPrecessorid(predecessorId);
                            predecessor=servermsgnew.getMyport();
                            predecessorId=servermsgnew.getMyportid();
                            servermsgnew.setSuccessor(myPort);
                            servermsgnew.setSuccessorid(myPortId);
                            servermsgnew.setValue(message.type.FOUNDJOIN);
                            foundjoin(servermsgnew);
                            Log.d(TAG,"chord_node_second_found_loop---"+myPort+"with successor--"+successor);
                            Log.d(TAG,"chord_node_second_found_loop---"+myPort+"with predecessor--"+predecessor);
                            Log.d(TAG,"chord_node_second_found_loop---"+servermsgnew.getMyport()+"with successor--"+servermsgnew.getSuccessor());
                            Log.d(TAG,"chord_node_second_found_loop---"+servermsgnew.getMyport()+"with predecessor--"+servermsgnew.getSuccessor());
                        }
                        else
                            {servermsgnew.setValue(message.type.LOOKUPJOIN);
                            lookupjoin(servermsgnew);
                                Log.d(TAG,"chord_node_lookup_loop---"+myPort+"with successor--"+successor);
                                Log.d(TAG,"chord_node_lookup_loop---"+myPort+"with predecessor--"+predecessor);
                                Log.d(TAG,"chord_node_lookup_loop---"+servermsgnew.getMyport()+"with successor--"+servermsgnew.getSuccessor());
                                Log.d(TAG,"chord_node_lookup_loop---"+servermsgnew.getMyport()+"with predecessor--"+servermsgnew.getSuccessor());}
                    }

                    else if(servermsgnew.getValue()==message.type.UPDATEJOIN){
                        successor=servermsgnew.getMyport();
                        successorID=servermsgnew.getMyportid();
                        Log.d(TAG,"chord_node_UPDATEJOIN---"+myPort+"with successor--"+successor);
                        Log.d(TAG,"chord_node_UPDATEJOIN---"+myPort+"with predecessor--"+predecessor);
                        Log.d(TAG,"chord_node_UPDATEJOIN---"+servermsgnew.getMyport()+"with successor--"+servermsgnew.getSuccessor());
                        Log.d(TAG,"chord_node_UPDATEJOIN---"+servermsgnew.getMyport()+"with predecessor--"+servermsgnew.getSuccessor());
                    }

                    else if(servermsgnew.getValue()==message.type.FOUNDJOIN){
                        predecessor=servermsgnew.getPredecessor();
                        predecessorId=servermsgnew.getPrecessorid();
                        successor=servermsgnew.getSuccessor();
                        successorID=servermsgnew.getSuccessorid();
                        Log.d(TAG,"Connected hurrah..."+myPort);
                        Log.d(TAG,"chord_node_Connected---"+myPort+"with successor--"+successor);
                        Log.d(TAG,"chord_node_Connected---"+myPort+"with predecessor--"+predecessor);
                        Log.d(TAG,"chord_node_Connected---"+servermsgnew.getMyport()+"with successor--"+servermsgnew.getSuccessor());
                        Log.d(TAG,"chord_node_Connected---"+servermsgnew.getMyport()+"with predecessor--"+servermsgnew.getSuccessor());

                    }

                    else if(servermsgnew.getValue()==message.type.INSERT){

                        if(successor==null||predecessor==null)
                        {localmessages.put(servermsgnew.key,servermsgnew.val);}
                        else if((predecessorId.compareTo(servermsgnew.getKeyId())<0 &&myPortId.compareTo(servermsgnew.getKeyId())>=0)||
                                (predecessorId.compareTo(myPortId)>0&&servermsgnew.getKeyId().compareTo(myPortId)>0&&servermsgnew.getKeyId().compareTo(predecessorId)>0)||
                                (predecessorId.compareTo(myPortId)>0&&servermsgnew.getKeyId().compareTo(myPortId)<0&&servermsgnew.getKeyId().compareTo(predecessorId)<0)){
                            localmessages.put(servermsgnew.key,servermsgnew.val);
                        }
                        else{

                            lookupinsert(servermsgnew);
                        }
                    }
                    else if(servermsgnew.getValue()==message.type.QUERY){
                        if(localmessages.containsKey(servermsgnew.getKey()))
                            {servermsgnew.setVal(localmessages.get(servermsgnew.getKey()));
                            servermsgnew.setValue(message.type.FOUNDQUERY);
                                lookup(servermsgnew);
                            }
                        else{lookup(servermsgnew);}
                        }


                    else if(servermsgnew.getValue()==message.type.ALLQUERY){
                        if(servermsgnew.getMyport().equals(myPort)){
                            querymessages.putAll(servermsgnew.getMessages());
                            isQuyering=false;
                        }
                        else{
                            Hashtable<String, String> temp
                                    = new Hashtable<String, String>();
                            temp.putAll(localmessages);
                            servermsgnew.setMessages(temp);
                            lookup(servermsgnew);
                        }
                    }
                    else if(servermsgnew.getValue()==message.type.DELETE){
                        if(servermsgnew.getMyport().equals(myPort)){isDeleting=false;}
                        else{
                            if(localmessages.containsKey(servermsgnew.getKey())){localmessages.remove(servermsgnew.getKey());}
                            else{lookup(servermsgnew);}
                        }
                    }
                    else if(servermsgnew.getValue()==message.type.ALLDELETE){
                        if(servermsgnew.getMyport().equals(myPort)){isDeleting=false;}
                        else{
                            localmessages.clear();
                            lookup(servermsgnew);

                        }
                    }
                    else if(servermsgnew.getValue()==message.type.FOUNDQUERY){
                        querymessages.put(servermsgnew.getKey(),servermsgnew.getVal());
                        synchronized (lock){
                        isQuyering=false;
                        lock.notifyAll();}
                    }
                    if(myPort.equals("11108")){
                    Log.d(TAG,"loop complete");}

                }
                catch (OptionalDataException e){e.printStackTrace();}
                catch (StreamCorruptedException e){e.printStackTrace();}
                catch (SocketTimeoutException e){e.printStackTrace();}
                catch (EOFException e){e.printStackTrace();}
                catch (UnknownHostException e){e.printStackTrace();}
                catch (IOException e){e.printStackTrace();}
                catch (Exception e){e.printStackTrace();}


            }

        }

        protected void onProgressUpdate(String... strings) {


        }



    }


    private class ClientTask extends AsyncTask<message, Void, Void> {                //Client Class

        @Override
        protected Void doInBackground(message... messages) {

            if(messages[0].getValue()==message.type.FIRSTJOIN) {

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(PORT0));
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(messages[0]);
                    oos.flush();
                    oos.close();
                    socket.close();
                    Log.d(TAG,"sending request---"+myPort);
                } catch (IOException e) {
                    Log.d(TAG,"error error");
                    e.printStackTrace();
                }
                catch(Exception e){Log.d(TAG,"error error");}
            }

            if(messages[0].getValue()==message.type.FOUNDJOIN){       //type,port,predecessor,myPort
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(messages[0].getMyport()));
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(messages[0]);
                    oos.flush();
                    oos.close();
                    socket.close();
                    Log.d("TAG","message type"+messages[0].value+"at client");

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(messages[0].getPredecessor()));

                    messages[0].setValue(message.type.UPDATEJOIN);
                    oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(messages[0]);
                    oos.flush();
                    oos.close();
                    socket.close();

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if(messages[0].getValue()==message.type.FOUNDQUERY){
                Socket socket = null;
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(messages[0].getMyport()));

                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(messages[0]);
                oos.flush();
                oos.close();
                socket.close();} catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(messages[0].getValue()==message.type.INSERT||
                    messages[0].getValue()==message.type.FORCEINSERT||
                    messages[0].getValue()==message.type.QUERY||
                    messages[0].getValue()==message.type.ALLQUERY||
                    messages[0].getValue()==message.type.DELETE||
                    messages[0].getValue()==message.type.ALLDELETE||
                    messages[0].getValue()==message.type.LOOKUPJOIN||
                    messages[0].getValue()==message.type.FORCEJOIN){
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successor));
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(messages[0]);
                    oos.flush();
                    oos.close();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

                return null;
    }



}
}
