/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.cdlib.mrt.queue;

import java.lang.IllegalArgumentException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
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



/**
 * 
 * A <a href="package.html">protocol to implement a distributed queue</a>.
 * 
 */

public class DistributedQueue {
    private static final Logger LOG = Logger.getLogger(DistributedQueue.class);
    public static int sessionTimeout =    40000;      // default max 20*tick 

    public final String dir;

    private ZooKeeper zookeeper;
    private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    private final String prefix = "mrtQ-";
    private static final String priority = "00";	// default priority

    private String name;

    public DistributedQueue(ZooKeeper zookeeper, String dir, List<ACL> acl){
        this(zookeeper, dir, priority, acl);
    }

    public DistributedQueue(ZooKeeper zookeeper, String dir, String priority, List<ACL> acl){
        this.dir = dir;
        this.name = String.format("%s/%s%s", this.dir, this.prefix, priority);

        if(acl != null){
            this.acl = acl;
        }
        this.zookeeper = zookeeper;
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
     * Returns a Map of the children, ordered by id.
     * @param watcher optional watcher on getChildren() operation.
     * @return map from id to child name for all children
     */
    public TreeMap<Long,String> orderedChildren(Watcher watcher) throws KeeperException, InterruptedException, ConnectionLossException {
        TreeMap<Long,String> orderedChildren = new TreeMap<Long,String>();

        List<String> childNames = null;
        int attempts = 0;
        while (true) {
            try{
                childNames = zookeeper.getChildren(dir, watcher);
		break;
            } catch (ConnectionLossException cle) {
                // did submit fail?
                if (attempts >= 3) throw new ConnectionLossException();
                if (childNames == null) {
                    System.err.println("[error] DistributedQueue.orderedChildren() lost connection, retrying: " + cle.getMessage());
                    attempts++;
                } else {
                    System.out.println("[info] DistributedQueue.orderedChildren() lost connection, but no need to retry.");
                    break;
                }
            } catch (KeeperException.NoNodeException e){
                throw e;
            }
	}
    
        for(String childName : childNames){
            try{
                //Check format
                if(!childName.regionMatches(0, prefix, 0, prefix.length())){
                    LOG.warn("Found child node with improper name: " + childName);
                    continue;
                }
                String suffix = childName.substring(prefix.length());
                Long childId = new Long(suffix);
                orderedChildren.put(childId,childName);
            }catch(NumberFormatException e){
                LOG.warn("Found child node with improper format : " + childName + " " + e,e);
            }
        }

        return orderedChildren;
    }
    
    private static class Next extends Throwable{};
    
    private abstract class Handler<T> {
        public abstract T handle (String path, byte[] data, Stat stat) 
            throws Next, KeeperException, InterruptedException;
        
        public T doit (boolean block)
            throws NoSuchElementException, KeeperException, InterruptedException {
            
            TreeMap<Long,String> orderedChildren;
            
            // element, take, and remove follow the same pattern.
            // We want to return the child node with the smallest sequence number.
            // Since other clients are remove()ing and take()ing nodes concurrently, 
            // the child with the smallest sequence number in orderedChildren might be gone by the time we check.
            // We don't call getChildren again until we have tried the rest of the nodes in sequence order.
            while (true) {
                try {
                    orderedChildren = orderedChildren(null);
                } catch (KeeperException.NoNodeException e) {
                    throw new NoSuchElementException();
                }
                if (orderedChildren.size() == 0) {
                    throw new NoSuchElementException();
                }
                for (String headNode : orderedChildren.values()) {
                    if (headNode != null){
                        try{
                            String path = String.format("%s/%s", dir, headNode);
                            Stat stat = new Stat();
                            byte[] data = zookeeper.getData(path, false, stat);
                            try {
                                return this.handle(path, data, stat);
                            } catch (Next n) {
                                continue;
                            }
                        } catch (KeeperException.NoNodeException e) {
                            //Another client removed the node first, try next
                        }
                    }
                }
                if (!block) break;
            }
            throw new NoSuchElementException();
        }
    }

    public Item peek() 
        throws NoSuchElementException, KeeperException, InterruptedException {
        Handler<Item> handler = new Handler<Item>() {
            public Item handle (String path, byte[] data, Stat stat) throws Next {
                Item item = Item.fromBytes(data);
                if (item.getStatus() == Item.PENDING) {
                    return item;
                } else {
                    throw new Next();
                }
            }
        };

        return handler.doit(false);
    }

    public Item consume() 
        throws KeeperException, InterruptedException {
        Handler<Item> handler = new Handler<Item>() {
            public Item handle (String path, byte[] data, Stat stat) 
            throws Next, KeeperException, InterruptedException {
                try {
                    if (data[0] == Item.PENDING) {
                        Item item = Item.fromBytes(data);
                        int version = stat.getVersion();
                        Item newItem = new Item(Item.CONSUMED, item.getData(), 
                                                new Date(), extractId(path));
                        zookeeper.setData(path, newItem.getBytes(), version);
                        return newItem;
                    } else {
                        throw new Next();
                    }
               // } catch (ConnectException ce) {
                    // server down
                    //ce.printStackTrace(System.err);
                    //System.out.println("------------------ ZOOKEEPER DOWN --------");
                } catch (KeeperException.BadVersionException ex) {
                    // Somebody got here first, next!
                    throw new Next();
                }
            }
        };

        return handler.doit(false);
    }

    public Item delete() 
        throws NoSuchElementException, KeeperException, InterruptedException {
        Handler<Item> handler = new Handler<Item>() {
            public Item handle (String path, byte[] data, Stat stat) 
              throws Next, KeeperException, InterruptedException {
                try {
                    Item item = Item.fromBytes(data);
                    if (item.getStatus() == Item.PENDING) {
                        int version = stat.getVersion();
                        Item newItem = new Item(Item.DELETED, item.getData(), 
                                                new Date(), extractId(path));
                        zookeeper.setData(path, newItem.getBytes(), version);
                        return newItem;
                    } else {
                        throw new Next();
                    }               
                } catch (KeeperException.BadVersionException ex) {
                    // Somebody got here first, next!
                    throw new Next();
                }
            }
        };

        return handler.doit(false);
    }
    
    public Item delete(String id)
        throws KeeperException, InterruptedException {
        return updateStatus(id, Item.CONSUMED, Item.DELETED);
    }

    public Item deletec(String id)
        throws KeeperException, InterruptedException {
        return updateStatus(id, Item.COMPLETED, Item.DELETED);
    }

    public Item deletef(String id)
        throws KeeperException, InterruptedException {
        return updateStatus(id, Item.FAILED, Item.DELETED);
    }

    public Item deletep(String id)
        throws KeeperException, InterruptedException {
        return updateStatus(id, Item.PENDING, Item.DELETED);
    }

    public Item complete(String id)
        throws KeeperException, InterruptedException {
        return updateStatus(id, Item.CONSUMED, Item.COMPLETED);
    }

    public Item fail(String id)
        throws KeeperException, InterruptedException {
        return updateStatus(id, Item.CONSUMED, Item.FAILED);
    }

    public Item requeue(String id)
        throws KeeperException, InterruptedException {
        return updateStatus(id, Item.CONSUMED, Item.PENDING);
    }

    public Item requeuec(String id)
        throws KeeperException, InterruptedException {
        return updateStatus(id, Item.COMPLETED, Item.PENDING);
    }

    public Item requeuef(String id)
        throws KeeperException, InterruptedException {
        return updateStatus(id, Item.FAILED, Item.PENDING);
    }

    // put this back in queue, at the end
    public boolean requeue(Item item)
        throws KeeperException, InterruptedException {
        while (true) {
            try {
                Item newItem = new Item(Item.PENDING, item.getData(), item.getTimestamp());
                zookeeper.create(this.name, newItem.getBytes(),
                                 acl, CreateMode.PERSISTENT_SEQUENTIAL);
                updateStatus(item.getId(), item.getStatus(), Item.DELETED);
                return true;
            } catch (KeeperException.NoNodeException e){
                zookeeper.create(dir, new byte[0], acl, CreateMode.PERSISTENT);
            }
        }
    }

    public Item updateStatus(String id, byte from, byte to)
        throws KeeperException, InterruptedException {
        try {
            String path = String.format("%s/%s", dir, id);
            Stat stat = new Stat();
            byte[] data = zookeeper.getData(path, false, stat);
            Item item = Item.fromBytes(data);
            if (item.getStatus() == from) {
                int version = stat.getVersion();
                Item newItem = new Item(to, item.getData(), new Date(), id);
                zookeeper.setData(path, newItem.getBytes(), version);
                return newItem;
            } else {
                throw new RuntimeException("Bad status.");
            }
        } catch (KeeperException.BadVersionException ex) {
            // Somebody got here first
            throw new RuntimeException();
        }
    }
        
    public boolean submit(byte[] data) throws KeeperException, InterruptedException, ConnectionLossException{
	int attempts = 0;
	String node = null;
        while (true) {
            Item i = new Item(Item.PENDING, data,  new Date());
            try {
                node = zookeeper.create(this.name, i.getBytes(), 
                                 acl, CreateMode.PERSISTENT_SEQUENTIAL);
                return true;
            } catch (ConnectionLossException cle) {
		// did submit fail?
		if (attempts >= 3) throw new ConnectionLossException();  
		if (node == null) {  
                    System.err.println("[error] DistributedQueue.submit() lost connection, requeuing: " + cle.getMessage());
		    attempts++;
		} else {
                    System.out.println("[info] DistributedQueue.submit () lost connection, but no need to requeue: " + node);
		    return true;
		}
            } catch (KeeperException.NoNodeException e){
                zookeeper.create(dir, new byte[0], acl, CreateMode.PERSISTENT);
            }
        }
    }

    public void cleanup(final byte status) 
        throws KeeperException, InterruptedException {
        Handler<Item> handler = new Handler<Item>() {
            public Item handle (String path, byte[] data, Stat stat) 
            throws Next, KeeperException, InterruptedException {
                try {
                    if (data[0] == status) {
                        int version = stat.getVersion();
                        zookeeper.delete(path, version);
                        throw new Next();
                    } else {
                        throw new Next();
                    }
                } catch (KeeperException.BadVersionException ex) {
                    // Somebody got here first, next!
                    throw new Next();
                } catch (NoSuchElementException ex) {
                    /* finished */
                }
                return null;
            }
        };

        try {
            handler.doit(false);
        } catch (NoSuchElementException ex) {
            /* finished */
	    throw ex;
        }
    }
        
    public static class Ignorer implements Watcher {
        public void process(WatchedEvent event){}
    }

    public static void main (String[] args) 
        throws java.io.IOException, KeeperException, InterruptedException {
        String queueNode = args[0];
        String cmd = args[1];
        ZooKeeper zk = new ZooKeeper("localhost:2181", 10000, new Ignorer());
        DistributedQueue q;
        if (args.length > 3) {
            q = new DistributedQueue(zk, queueNode, args[3], null);	//  priority
	} else {
            q = new DistributedQueue(zk, queueNode, null);
	}
	try {
            if (cmd.equals("submit")) {
		Properties p = new Properties();
		try {
	            p.load(new FileInputStream(args[2]));
		} catch (IllegalArgumentException iae) {
	            throw new Exception("[Error] Input data : " + args[1]
			+ " -- " + iae.getMessage());
		}
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
      	        oos.writeObject(p);
                oos.flush();
                oos.close();
                bos.close();
                q.submit(bos.toByteArray());
            } else if (cmd.equals("submittest")) {
                for (int i = 0 ; i < 1000 ; i++) {
                    q.submit(Integer.toString(i).getBytes());
                }
            } else if (cmd.equals("peek")) {
                System.out.println(q.peek());
            } else if (cmd.equals("consume")) {
                System.out.println(q.consume());
            } else if (cmd.equals("consumetest")) {
                for (int i = 0 ; i < 1000 ; i++) {
                    System.out.println(q.consume());
                }
            } else if (cmd.equals("delete")) {
                String d1;
                if (args.length == 1) {
                    // delete top
                    System.out.println(q.delete());
                } else {
                    System.out.println(q.delete(args[1]));
                }
            } else if (cmd.equals("requeue")) {
                System.out.println(q.requeue(args[2]));
            } else if (cmd.equals("requeuec")) {
                System.out.println(q.requeuec(args[2]));
            } else if (cmd.equals("complete")) {
                System.out.println(q.complete(args[2]));
            } else if (cmd.equals("list") || cmd.equals("lsp")) {
		boolean isShort = cmd.equals("lsp");
                TreeMap<Long,String> orderedChildren;
                try {
                    orderedChildren = q.orderedChildren(null);
                } catch(KeeperException.NoNodeException e){
                    throw new NoSuchElementException();
                }
                for (String headNode : orderedChildren.values()) {
                    String path = String.format("%s/%s", q.dir, headNode);
                    try {
                        byte[] data = zk.getData(path, false, null);
			if (isShort) {
                            Item item = Item.fromBytes(data);
			    if (item.getStatus() == Item.PENDING) {
                                System.out.println(String.format("%s: %s", headNode, Item.fromBytes(data)));
			    }
			} else {
                            System.out.println(String.format("%s: %s", headNode, Item.fromBytes(data)));
			}
                    } catch(KeeperException.NoNodeException e){}
                }
            } else {
                System.err.println("Bad command.");
                System.exit(1);
            }        
        } catch (FileNotFoundException fe) {
	    System.err.println("[Error] Input file not found: " + args[1]);
	    System.exit(1);
        } catch (Exception e) {
	    e.printStackTrace(System.err);
        }
    }
}
