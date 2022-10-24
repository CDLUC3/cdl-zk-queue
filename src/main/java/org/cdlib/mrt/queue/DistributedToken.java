/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.queue;

import java.lang.IllegalArgumentException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.TreeMap;
import java.util.Date;
import java.net.ConnectException;
import java.io.*;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.cdlib.mrt.queue.LockItem;
import org.cdlib.mrt.utility.StringUtil;


/**
 * 
 * A <a href="package.html">protocol to implement a distributed queue</a>.
 * 
 */

public class DistributedToken {
    private static final Logger LOG = Logger.getLogger(DistributedToken.class);
    public static int sessionTimeout =    40000;      // default max 20*tick 
    private static boolean DEBUG = false;

    private ZooKeeper zookeeper;
    private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    public final String node;

    public DistributedToken(String zooConnectString, String node, List<ACL> acl)
         throws IOException, KeeperException, InterruptedException
    {
        this.node = node;

        if(acl != null){
            this.acl = acl;
        }
        this.zookeeper = new ZooKeeper(zooConnectString, DistributedToken.sessionTimeout, new Ignorer());
        this.zookeeper.exists("/", null);
    }

    /**
     * Extracts the item id from a path.
     * @return item id
     */
    private String extractId (String path) {
        String[] parts = path.split("/");
        return parts[parts.length-1];
    }

    /**
     * add - add lock if it does not exist
     * @param tokenName - key of token to be added
     * @param data - data associated with key
     * @return true=key exists after operation; false=key does not exist after operation
     */
    public boolean addRetry(String tokenName, String data) 
            throws KeeperException, InterruptedException, ConnectionLossException
    {
        if (exists(tokenName)) return true;
	int attempts = 0;
	String responseNode = null;
        String name = node + "/" + tokenName;
        while (true) {
            try {
                System.out.println("submit name=" + name);
		LockItem i = new LockItem(data, new Date());
                responseNode = zookeeper.create(name, 
                                i.getBytes(), 
                                 acl, 
                                 CreateMode.PERSISTENT);
                                 //CreateMode.EPHEMERAL);
                System.out.println("responseNode=" + responseNode);
                return true;
            } catch (KeeperException.NodeExistsException nee) {
                System.err.println("[error] DistributedLock node exists: " + name);
		// exists
                return false;
            } catch (KeeperException.NoNodeException nne){
                System.err.println("[error] DistributedLock node does not exist: " + nne.getMessage());
                System.out.println("[info] DistributedLock creating node: " + this.node);
                zookeeper.create(this.node, new byte[0], acl, CreateMode.PERSISTENT);
            } catch (ConnectionLossException cle) {
		// did submit fail?
		if (attempts >= 3) throw new ConnectionLossException();  
		if (node == null) {  
                    System.err.println("[error] DistributedLock.submit() lost connection, retrying: " + cle.getMessage());
		    attempts++;
		} else {
                    System.out.println("[info] DistributedLock.submit () lost connection, but no need to retry: " + node);
		    return true;
		}
            }
        }
    }
    /**
     * add - add lock if it does not exist
     * @param tokenName - key of token to be added
     * @param data - data associated with key
     * @return true=key exists after operation; false=key does not exist after operation
     */
    public boolean add(String tokenName, String data) 
            throws KeeperException, InterruptedException, ConnectionLossException
    {
        if (exists(tokenName)) return true;
	int attempts = 0;
	String responseNode = null;
        String name = node + "/" + tokenName;
       
            try {
                if (DEBUG) System.out.println("submit name=" + name);
		LockItem i = new LockItem(data, new Date());
                responseNode = zookeeper.create(name, 
                                i.getBytes(), 
                                 acl, 
                                 CreateMode.PERSISTENT);
                                 //CreateMode.EPHEMERAL);
                if (DEBUG) System.out.println("responseNode=" + responseNode);
                return true;
                
            } catch (KeeperException.NodeExistsException nee) {
                System.err.println("[error] DistributedLock node exists: " + name);
		// exists
                return true;
                
            } catch (KeeperException.NoNodeException nne){
                System.err.println("[error] DistributedLock node does not exist: " + nne.getMessage());
                System.out.println("[info] DistributedLock creating node: " + this.node);
                return false;
                
            } catch (ConnectionLossException cle) {
		throw cle;
            }
        
    }

    /**
     * Returns a Map of the children, ordered by id.
     * @param watcher optional watcher on getChildren() operation.
     * @return map from id to child name for all children
     */
    public TreeMap<Long,String> orderedChildren(String levelName, Watcher watcher) 
            throws KeeperException, InterruptedException, ConnectionLossException {
        TreeMap<Long,String> orderedChildren = new TreeMap<Long,String>();
        
        if (StringUtil.isAllBlank(levelName)) levelName = "";
        else levelName = "/" + levelName;
        
        String name = node + levelName;
        List<String> childNames = null;
        int attempts = 0;
        
        while (true) {
            try {
                childNames = zookeeper.getChildren(name, watcher);
		break;
            } catch (ConnectionLossException cle) {
                if (attempts >= 3) throw new ConnectionLossException();
                if (childNames == null) {
                    System.err.println("[error] DistributedLock.orderedChildren() lost connection, retrying: " + cle.getMessage());
                    attempts++;
                } else {
                    System.out.println("[info] DistributedLock.orderedChildren() lost connection, but no need to retry.");
                    break;
                }
            } catch (KeeperException.NoNodeException e) {
                throw e;
            }
	}
    
	Long i = new Long(0);
        for(String childName : childNames){
            try{
                orderedChildren.put(i,childName);
		i++;
            } catch(Exception e){
	e.printStackTrace();
                System.err.println("Found child node with improper format : " + childName + " " + e.getMessage());
            }
        }

        return orderedChildren;
    }


    /**
     * Determines if token exists
     * @param tokenName name of token to be tested
     * @return true - token exists; false - token does not exist
     * @throws KeeperException
     * @throws InterruptedException 
     */
    public boolean exists(String tokenName) throws KeeperException, InterruptedException {
	// probably not needed - ephemeral nodes
        try {
            String name = node + "/" + tokenName;
                //System.out.println("exists name=" + name);
            Stat stat = zookeeper.exists(name, null);
	    if (stat == null) return false;
            //int version = stat.getVersion();
	    //System.out.println("Cleanup version: " + version);
            return true;
        } catch (KeeperException.BadVersionException ex) {
	    System.err.println("Error during cleanup: " + ex.getMessage());
            return false;
        } catch (NoSuchElementException ex) {
	    System.err.println("Error during cleanup: " + ex.getMessage());
            return false;
        }
    }

    /**
     * Close zookeeper
     * @throws KeeperException
     * @throws InterruptedException 
     */
    public void close() throws KeeperException, InterruptedException {
	// probably not needed - ephemeral nodes
        try {
	    if (DEBUG) System.out.println("Cleaning up zookeeper lock.");
	    zookeeper.close();
	    zookeeper = null;
        } catch (NoSuchElementException ex) {
	    System.err.println("Error during cleanup: " + ex.getMessage());
        }
    }

    /**
     * Remove token
     * @param tokenName token to be removed
     * @return null=token status unknown; false - token does not exist
     * @throws KeeperException
     * @throws InterruptedException 
     */
    public Boolean remove(String tokenName) throws KeeperException, InterruptedException {
	// probably not needed - ephemeral nodes
        try {
            String name = node + "/" + tokenName;
            Stat stat = zookeeper.exists(name, null);
	    if (stat == null) return false;
            int version = stat.getVersion();
	    if (DEBUG) System.out.println("Cleanup version: " + version);
            zookeeper.delete(name, version);
            return false;
            
        } catch (KeeperException.BadVersionException ex) {
	    System.err.println("Error during cleanup: " + ex.getMessage());
            return null;
            
        } catch (NoSuchElementException ex) {
	    System.err.println("Error during cleanup: " + ex.getMessage());
            return null;
            
        }
    }

    /**
     * Same as exists 
     */
    public boolean verify(String tokenName) throws KeeperException, InterruptedException {
	// probably not needed - ephemeral nodes
        return exists(tokenName);
    }

    /**
     * Get content of token if exists
     * @param tokenName name of token with content to be returned
     * @return returned content
     * @throws KeeperException
     * @throws InterruptedException 
     */
    public LockItem getData(String tokenName)
        throws KeeperException, InterruptedException {
	// probably not needed - ephemeral nodes
        try {
            if (!exists(tokenName)) return null;
            String name = node + "/" + tokenName;
            byte[] datab = zookeeper.getData(name, false, null);
            LockItem lockItem = LockItem.fromBytes(datab);
            return lockItem;
            
        } catch(Exception e){
            return null;
        }
    }

    public ZooKeeper getZookeeper() {
        return zookeeper;
    }
        
    public static class Ignorer implements Watcher {
        public void process(WatchedEvent event){}
    }

}
