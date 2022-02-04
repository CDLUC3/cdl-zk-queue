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


/**
 * 
 * A <a href="package.html">protocol to implement a distributed queue</a>.
 * 
 */

public class DistributedLock {
    private static final Logger LOG = Logger.getLogger(DistributedLock.class);
    public static int sessionTimeout =    40000;      // default max 20*tick 

    private ZooKeeper zookeeper;
    private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    public final String node;
    private String name;

    public DistributedLock(ZooKeeper zookeeper, String node, String path, List<ACL> acl){
        this.node = node;
        this.name = String.format("%s/%s", node, path);

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

    public boolean submit(String data) throws KeeperException, InterruptedException, ConnectionLossException{
	int attempts = 0;
	String node = null;
        while (true) {
            try {
		LockItem i = new LockItem(data, new Date());
                node = zookeeper.create(this.name, i.getBytes(), 
                                 acl, CreateMode.EPHEMERAL);
                return true;
            } catch (KeeperException.NodeExistsException nee) {
                System.err.println("[error] DistributedLock node exists: " + this.name);
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
     * Returns a Map of the children, ordered by id.
     * @param watcher optional watcher on getChildren() operation.
     * @return map from id to child name for all children
     */
    public TreeMap<Long,String> orderedChildren(Watcher watcher) throws KeeperException, InterruptedException, ConnectionLossException {
        TreeMap<Long,String> orderedChildren = new TreeMap<Long,String>();

        List<String> childNames = null;
        int attempts = 0;
        while (true) {
            try {
                childNames = zookeeper.getChildren(node, watcher);
		break;
            } catch (ConnectionLossException cle) {
                if (attempts >= 3) throw new ConnectionLossException();
                if (childNames == null) {
                    System.err.println("[error] DistributedQueue.orderedChildren() lost connection, retrying: " + cle.getMessage());
                    attempts++;
                } else {
                    System.out.println("[info] DistributedQueue.orderedChildren() lost connection, but no need to retry.");
                    break;
                }
            } catch (KeeperException.NoNodeException e) {
                throw e;
            }
	}
    
	Long i = new Long(0);
        for(String childName : childNames){
            try{
		String prefix = "ark";
                // Check format
                if(!childName.regionMatches(0, prefix, 0, prefix.length())){
                    System.err.println("Found child node with improper name: " + childName);
                    continue;
                }
                orderedChildren.put(i,childName);
		i++;
            } catch(Exception e){
	e.printStackTrace();
                System.err.println("Found child node with improper format : " + childName + " " + e.getMessage());
            }
        }

        return orderedChildren;
    }


    public void cleanup() throws KeeperException, InterruptedException {
	// probably not needed - ephemeral nodes
        try {
            Stat stat = zookeeper.exists(this.name, null);
	    if (stat == null) return;
            int version = stat.getVersion();
	    System.out.println("Cleanup version: " + version);
            zookeeper.delete(this.name, version);
        } catch (KeeperException.BadVersionException ex) {
	    System.err.println("Error during cleanup: " + ex.getMessage());
        } catch (NoSuchElementException ex) {
	    System.err.println("Error during cleanup: " + ex.getMessage());
        }
    }
        
    public static class Ignorer implements Watcher {
        public void process(WatchedEvent event){}
    }

    public static void main (String[] args) 
        throws java.io.IOException, KeeperException, InterruptedException {
        if (args.length != 4) {
	    System.out.println(args.length + "Usage: PGM <queue node> <cmd> <path> <data>");
	    System.exit(1);
	}
        String queueNode = args[0];
        String cmd = args[1];
        String path = args[2];
        String data = args[3];
        ZooKeeper zk = new ZooKeeper("localhost:2181", 10000, new Ignorer());
        DistributedLock l = null;
        l = new DistributedLock(zk, queueNode, path, null);

	try {
            if (cmd.equals("submit")) {
                System.out.println(l.submit(data));
            } else if (cmd.equals("submit-multiple")) {
		// expect to fail after first node is created
                for (int i = 0 ; i < 10 ; i++) {
                    System.out.println(l.submit(Integer.toString(i)));
                }
            } else if (cmd.equals("cleanup")) {
                l.cleanup();
            } else if (cmd.equals("list")) {
                System.out.println(l.submit(data));
                TreeMap<Long,String> orderedChildren;
                try {
                    orderedChildren = l.orderedChildren(null);
                } catch(KeeperException.NoNodeException e){
                    throw new NoSuchElementException();
                }
                for (String headNode : orderedChildren.values()) {
                    String pathname = String.format("%s/%s", l.node, headNode);
                    try {
                        byte[] datab = zk.getData(pathname, false, null);
			String datas = new String(datab);
                        System.out.println(String.format("%s: %s", headNode, datas));
                    } catch(KeeperException.NoNodeException e){}
                }
                l.cleanup();
            } else {
                System.err.println("Bad command.");
                System.exit(1);
            }        
        } catch (Exception e) {
	    e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
