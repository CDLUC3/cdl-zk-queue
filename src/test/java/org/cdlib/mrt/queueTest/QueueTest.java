package org.cdlib.mrt.queueTest;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.util.List;
import java.util.NoSuchElementException;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;

import org.cdlib.mrt.queue.*;

import org.testng.annotations.*;
import org.testng.Assert;

public class QueueTest {

    ZooKeeper zk;
    DistributedQueue q;
    
    final String Q_NAME = "/queueTest";

    @BeforeClass
    public void setUp() {
        try {
            this.zk = new ZooKeeper("localhost:2181", 10000, new DistributedQueue.Ignorer());
            this.q = new DistributedQueue(zk, Q_NAME, null);
            for (String child : this.zk.getChildren(Q_NAME, false)) {
                this.zk.delete(String.format("%s/%s", Q_NAME, child), -1);
            }
        } catch (IOException ex) {
            assert(false);
        } catch (InterruptedException ex) {
            assert(false);
        } catch (KeeperException ex) {
            assert(false);
        }
    }
    
    @Test(groups = { "functional" },
          dependsOnMethods = { "emptyQueueShouldRaiseEx" })
    public void canAddToQueue() {
        try {
            assert(q.submit("hello world".getBytes("UTF-8")));
            Item item = q.consume();
            q.complete(item.getId());
        } catch (IOException ex) {
            assert(false);
        } catch (InterruptedException ex) {
            assert(false);
        } catch (KeeperException ex) {
            assert(false);
        }
    }

    @Test(groups = { "functional" })
    public void canCleanup() {
        try {
            /* cleanup throws an exception when it is done */
            q.cleanup(Item.COMPLETED);
            assert(false);
        } catch (NoSuchElementException ex) {
            assert(true);
        } catch (KeeperException ex) {
            assert(false);
        } catch (InterruptedException ex) {
            assert(false);
        }
    }

    @Test(groups = { "functional" },
          dependsOnMethods = { "canCleanup"})
    public void queueShouldBeEmpty() {
        try {
            List<String> children = this.zk.getChildren(Q_NAME, false);
            assert(children.size() == 0);
        } catch (KeeperException ex) {
            assert(false);
        } catch (InterruptedException ex) {
            assert(false);
        }
    }

    @Test(groups = { "functional" })
    public void emptyQueueShouldRaiseEx() {
        try {
            q.consume();
            assert(false);
        } catch (NoSuchElementException ex) {
            assert(true);
        } catch (KeeperException ex) {
            assert(false);
        } catch (InterruptedException ex) {
            assert(false);
        }
    }

    @Test(groups = { "functional" })
    public void canRequeue() {
        try {
            byte[] content1 = "1".getBytes("UTF-8");
            byte[] content2 = "2".getBytes("UTF-8");
            Assert.assertTrue(q.submit(content1));
            Assert.assertTrue(q.submit(content2));
            Item item = q.consume();
            Assert.assertEquals(item.getData(), content1);
            q.requeue(item);
            item = q.consume();
            Assert.assertEquals(item.getData(), content2);
            q.complete(item.getId());
            item = q.consume();
            Assert.assertEquals(item.getData(), content1);
            q.complete(item.getId());
        } catch (NoSuchElementException ex) {
            assert(false);
        } catch (KeeperException ex) {
            assert(false);
        } catch (InterruptedException ex) {
            assert(false);
        } catch (UnsupportedEncodingException ex) {
            assert(false);
        }
    }
}
