package com.mutou.bigdata.zk;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZK操作工具类</br>
 * 需初始化后调用其它方法:ZooKeeperUtils.init()...</br>
 * 使用后必须调用ZooKeeperUtils.close()
 *
 * @author wonder
 * @date 2016年1月12日 下午6:44:59
 */
public class ZooKeeperUtils {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperUtils.class);
    private static ZooKeeper zooKeeper = null;

    /**
     * 初使化zk连接
     *
     * @param zkConnect zk连接字符串
     * @return true or false
     */
    public static boolean doInit(String zkConnect) {
        try {
            if (zooKeeper == null) {
                zooKeeper = new ZooKeeper(zkConnect, 5000, new Watcher() {

                    public void process(WatchedEvent event) {
                        logger.info("event path:{}\tevent state:{}\tevent type:{}",
                                event.getPath(), event.getState().getIntValue(), event.getType().getIntValue());
                    }
                });
            }
        } catch (Exception e) {
            logger.error("init zookeeper error", e);
            return false;
        }

        return true;
    }

    /**
     * 获取当前节点子节点
     *
     * @param path 节点路径
     * @return {@link List<String>}
     */
    public static List<String> getChildren(String path) {
        List<String> children = null;
        try {
            children = zooKeeper.getChildren(path, false);
        } catch (Exception e) {
            logger.error("getChildren error", e);
        }

        return children;
    }

    /**
     * 获取节点数据
     *
     * @param path 节点路径
     * @return 节点数据
     */
    public static String getNodeData(String path) {
        String data = null;
        try {
            data = new String(zooKeeper.getData(path, false, null));
        } catch (Exception e) {
            logger.error("get node data error", e);
        }

        return data;
    }

    /**
     * 在zookeeper创建节点
     *
     * @param path 节点path
     * @param data 节点数据
     * @return 创建成功返回true, 失败返回false
     */
    public static boolean createNodeData(String path, String data) {
        try {
            // 创建节点,并设置标记
            if (data == null) {
                zooKeeper.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                zooKeeper.create(path, data.getBytes("utf-8"), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            return true;
        } catch (Exception e) {
            logger.info("zookeeper createNodeData error : " + e.getMessage());
            return false;
        }
    }

    /**
     * 创建临时节点
     *
     * @param path 节点path
     * @param data 节点数据
     * @return 创建成功返回true, 失败返回false
     */
    public static boolean createEphemeralNode(String path, String data) {
        try {
            // 创建节点,并设置标记
            if (data == null) {
                zooKeeper.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } else {
                zooKeeper.create(path, data.getBytes("utf-8"), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }
            return true;
        } catch (Exception e) {
            logger.info("zookeeper createEphemeralNode error : " + e.getMessage());
            return false;
        }
    }

    /**
     * 在zookeeper更新节点数据
     *
     * @param path 节点path
     * @param data 节点数据
     * @return 创建成功返回true, 失败返回false
     */
    public static boolean setNodeData(String path, String data) {
        try {
            // 更新节点数据
            if (data == null) {
                zooKeeper.setData(path, null, -1);
            } else {
                zooKeeper.setData(path, data.getBytes("utf-8"), -1);
            }
            return true;
        } catch (Exception e) {
            logger.info("zookeeper setNodeData error : " + e.getMessage());
            return false;
        }
    }

    /**
     * 获取节点数据
     *
     * @param path 节点path
     * @return 返回当前path节点数据, 有异常返回null
     */
    public static boolean deleteNodeData(String path) {
        try {
            zooKeeper.delete(path, -1);
            return true;
        } catch (Exception e) {
            logger.error("deleteNodeData error : " + e.getMessage());
            return false;
        }
    }

    /**
     * 判断节点是否存在
     *
     * @param path 节点路径
     * @return true or false
     */
    public static boolean isExistNode(String path) {
        Stat stat = null;
        try {
            stat = zooKeeper.exists(path, null);
        } catch (Exception e) {
            logger.info("isExistNode error : " + e.getMessage());
        }

        return stat == null ? false : true;
    }

    /**
     * 关闭zk连接
     */
    public static void close() {
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (Exception e) {
                logger.error("zookeeper close error", e);
            }
        }
    }
}
