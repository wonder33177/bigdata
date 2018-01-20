package com.mutou.bigdata.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestZK implements Watcher {
    public static Stat stat = new Stat();
    ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String p = "/mutou/data";
        ZooKeeper zooKeeper = new ZooKeeper("192.168.20.115:2181", 5000, new TestZK());

        // zooKeeper.create(p,"test_data".getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,
        //CreateMode.PERSISTENT);
        //zooKeeper.setData(p,"test_data_new".getBytes(),-1);
        ///zooKeeper.delete(p,-1);

        /*String s = zooKeeper.create(p, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(s);

        Thread.sleep(1000 * 100);*/

        //exists register watch
        zooKeeper.exists(p, true);
        String path = zooKeeper.create(p, "456".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        //get register watch
        zooKeeper.getData(path, true, stat);
        zooKeeper.setData(path, "hhhh".getBytes(), -1);

        zooKeeper.exists(path, true);
        //Thread.sleep(1000*20);
        //System.out.println("20 after delete");
        //exists register watch
        zooKeeper.delete(path, -1);

    }

    public void process(WatchedEvent event) {
        System.out.println(event.getPath() + "_" + event.getState().name() + "_" + event.getType().name());
        if (Event.KeeperState.SyncConnected == event.getState()) {
            if (Event.EventType.None == event.getType() && null == event.getPath()) {
                //connectedSemaphore.countDown();
                System.out.println("Zookeeper session established");
            } else if (Event.EventType.NodeCreated == event.getType()) {
                System.out.println("success create znode");

            } else if (Event.EventType.NodeDataChanged == event.getType()) {
                System.out.println("success change znode: " + event.getPath());

            } else if (Event.EventType.NodeDeleted == event.getType()) {
                System.out.println("success delete znode");

            } else if (Event.EventType.NodeChildrenChanged == event.getType()) {
                System.out.println("NodeChildrenChanged");

            }

        }
    }
}
