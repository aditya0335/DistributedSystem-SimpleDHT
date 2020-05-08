package edu.buffalo.cse.cse486586.simpledht;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Hashtable;

//query and delete will only be used if we have to get global operation

public class message implements Serializable {

    public enum type{
        FIRSTJOIN,
        UPDATEJOIN,
        LOOKUPJOIN,
        FOUNDJOIN,
        FORCEJOIN,
        QUERY,
        ALLQUERY,
        DELETE,
        ALLDELETE,
        INSERT,
        FORCEINSERT,
        FOUNDQUERY

    }

    type value;

    public type getValue() {
        return value;
    }

    public void setValue(type value) {
        this.value = value;
    }

    String myport;


    public String getMyport() {
        return myport;
    }

    public void setMyport(String myport) {
        this.myport = myport;
    }

    String successor;

    public String getSuccessor() {
        return successor;
    }

    public void setSuccessor(String successor) {
        this.successor = successor;
    }

    String predecessor;

    public String getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(String predecessor) {
        this.predecessor = predecessor;
    }

    Hashtable<String, String> messages
            = new Hashtable<String, String>();


    public Hashtable<String, String> getMessages() {
        return messages;
    }

    public void setMessages(Hashtable<String, String> messages) {
        this.messages.putAll(messages);
    }

    public String getMyportid() {
        return myportid;
    }

    public void setMyportid(String myportid) {
        this.myportid = myportid;
    }

    String myportid;

    public String getPrecessorid() {
        return precessorid;
    }

    public void setPrecessorid(String precessorid) {
        this.precessorid = precessorid;
    }

    String precessorid;

    public String getSuccessorid() {
        return successorid;
    }

    public void setSuccessorid(String successorid) {
        this.successorid = successorid;
    }

    String successorid;

    public String getKeyId() {
        return keyId;
    }

    public void setKeyId(String keyId) {
        this.keyId = keyId;
    }

    String keyId;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    String key;

    public String getVal() {
        return val;
    }

    public void setVal(String val) {
        this.val = val;
    }

    String val;

    //    Constructor for insert,single query,single delete
    message(type value,String myport ,String key,String val,String keyid) {   //Initializing constructor use type.anygivenvalue
        this.value=value;
        this.myport=myport;
        this.key=key;
        this.val=val;
        this.keyId=keyid;
    }

    //    Constructor for join
    message(type value,String myport ,String predecessor, String successor,String myportid,String predecessorid,String successorid) {   //Initializing constructor use type.anygivenvalue
        this.value=value;
        this.myport=myport;
        this.successor=successor;
        this.predecessor=predecessor;
        this.myportid=myportid;
        this.precessorid=predecessorid;
        this.successorid=successorid;

    }

    //    Constructor for all query or all delete
    message(type value,String myport ,String predecessor, String successor, Hashtable<String,String> messages) {   //Initializing constructor use type.anygivenvalue
        this.value=value;
        this.myport=myport;
        this.successor=successor;
        this.predecessor=predecessor;
        this.messages.putAll(messages);
    }




}
