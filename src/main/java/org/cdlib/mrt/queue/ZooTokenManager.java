/******************************************************************************
Copyright (c) 2005-2012, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
*******************************************************************************/
package org.cdlib.mrt.queue;
import java.util.TreeMap;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.cdlib.mrt.queue.LockItem;
import org.cdlib.mrt.queue.DistributedToken;

import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;


/**
 * Run fixity
 * @author dloy
 */
public class ZooTokenManager
{

    protected static final String NAME = "ZooTokenManager";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;

    // Support lock
    private ZooKeeper zooKeeper;
    private String zooConnectString = null;
    private String zooLockNode = null;
    private LoggerInf logger = null;
    private DistributedToken distributedToken;

    
    
    public static ZooTokenManager getZooTokenManager(
            String zooConnectString, 
            String zooLockNode,
            LoggerInf logger)
        throws TException
    {
        return new ZooTokenManager(zooConnectString, zooLockNode, logger);
    }
    
    protected ZooTokenManager(
            String zooConnectString, 
            String zooLockNode,
            LoggerInf logger)
        throws TException
    {
        
        try {
            //isValidNode(node, objectID, connection, logger);
            this.zooConnectString = zooConnectString;
            this.zooLockNode = zooLockNode;
            this.logger = logger;
            init();
                
        } catch (Exception ex) {
        }
    }
    
    
    /**
     * Lock on primary identifier.  Will loop unitil lock obtained.
     *
     * @param String primary ID of object (ark)
     * @param String jobID
     * @return Boolean result of obtaining lock
     */
    public void init() 
            throws TException
    {
    try {

       distributedToken = new DistributedToken(zooConnectString, zooLockNode, null);

        } catch (Exception e) {
            e.printStackTrace();
            throw new TException(e);
        }
    }

    /**
     * Lock on primary identifier.  Will loop unitil lock obtained.
     *
     * @param String primary ID of object (ark)
     * @param String jobID
     * @return Boolean result of obtaining lock
     */
    public Boolean addLock(String tokenName, String payload) {
    try {

       // SSM vars
       //String zooConnectString = InventoryConfig.qService;
       //String zooLockNode = InventoryConfig.lockName;

       // Zookeeper treats slashes as nodes
        if (verifyLock(tokenName)) {
            return true;
        }
        boolean lvlLock = addIntermediateLevel(tokenName);
        if (!lvlLock) return false;
        return distributedToken.add(tokenName, payload);

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * Create intemediate znodes that are missing
     * @param tokenName name of toke with intermediate levels
     * @return true=intermediate znodes exist; false=one or more intermediate znodes do not exist;
     */
    protected boolean addIntermediateLevel(String tokenName) {
        String lvl = "";
        if (StringUtil.isAllBlank(tokenName)) return false;
        try {
            String[] levelNames=tokenName.split("/");
            if (levelNames.length == 1) return true;
            int occ = levelNames.length - 1;
            for (int i=0; i < occ; i++) {
                String levelName = levelNames[i];
                if (lvl.length() > 0) {
                    lvl += "/";
                }
                lvl += levelName;
                if (verifyLock(lvl)) continue;
                distributedToken.add(lvl, lvl);
                System.out.println("Add intermediate level:" + lvl);
            }
            return distributedToken.verify(lvl);

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public Boolean replaceLock(String tokenName, String payload) 
        throws TException
    {         
        try {
            if (distributedToken.exists(tokenName)) {
                distributedToken.remove(tokenName);
            }
            return addLock(tokenName, payload);

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }


    public LockItem getData(String tokenName) {
        try {
            return distributedToken.getData(tokenName);

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    /**
     * Release lock
     *
     * @param none needed inputs are global
     * @return status of token at completion - true=exists; false=does not exist
     */
    public Boolean removeLock(String tokenName) {
        try {

                Boolean status = distributedToken.remove(tokenName);
                if (status == null) {
                    return exists(tokenName);
                }
                return status;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public Boolean exists(String tokenName) 
        throws TException
    {
        try {
            return distributedToken.exists(tokenName);

        } catch (Exception e) {
            throw new TException(e);
        }
    }
    
    public Boolean verifyLock(String tokenName) 
        throws TException
    {
        try {
            return distributedToken.verify(tokenName);

        } catch (Exception e) {
            throw new TException(e);
        }
    }
    
    public void close() 
        throws TException
    {
        try {
            distributedToken.close();

        } catch (Exception e) {
            throw new TException(e);
        }
    }
    
    
    
    public TreeMap<Long,String> orderedChildren(String levelName, Watcher watcher)
        throws TException
    {
        try {
            return distributedToken.orderedChildren(levelName, watcher);

        } catch (Exception e) {
            throw new TException(e);
        }
    }

    public String getZooLockNode() {
        return zooLockNode;
    }

}
