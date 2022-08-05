package zk;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @author wentao
 * @date 2022-08-05  16:54
 */

public class DistributeServer {
    private static String connecting = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zk = null;
    private String parentNode = "/servers";

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        // 1 获取 zk 连接
        DistributeServer server = new DistributeServer();
        server.getConnect();
        //注册服务器到zk集群
        server.regist(args[0]);

    }

    private void getConnect () throws IOException {
        zk = new ZooKeeper(connecting, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
            }
        });
    }

    /**
     * 注册服务器
     */
    private void regist(String hostname) throws InterruptedException, KeeperException {
        String create = zk.create(parentNode + "/server",hostname.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println(hostname +" is online " + create);
    }

    /**
     * 业务功能
     */
    private void business(String hostname) throws InterruptedException {
        System.out.println(hostname + " is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }


}
