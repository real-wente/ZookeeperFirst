package zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author wentao
 * &#064;date  2022-08-05  14:13
 */

public class ZkClientTest {


    /**
     * 2000毫秒
     */
    private  int sessionTimeout = 2000;
    /**
     * 不能有空格
     */
    private  String connectString = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
    private  ZooKeeper zkClient;

    /**
     * "@Test"的类命名应该以Test为结尾，驼峰命名法
     * 创建客户端远程连接
     */
    @Before
    public void init () throws IOException {
        // ctrl + p 提示参数
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("==============================");
                List<String> children = null;
                try {
                    children = zkClient.getChildren("/", true);
                    for (String child : children) {
                        System.out.println(child);
                    }
                    System.out.println("==============================");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

    }

    /**
     * 连接集群以后创建子节点
     */
    @Test
    public void create() throws InterruptedException, KeeperException {
        String nodeCreated = zkClient.create("/Unicorn", "黄四郎".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 监听节点的变化
     */
    @Test
    public void getChildren() throws InterruptedException, KeeperException {
        List<String> children = zkClient.getChildren("/", true);
        /*
          children.for  快捷循环 for (String child : children)
          child.sout 快捷打印  System.out.println(child)
         */
        for (String child : children) {
            System.out.println(child);
        }
        //延迟
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 判断结点是否存在
     */
    @Test
    public void exist() throws InterruptedException, KeeperException {
        Stat stat = zkClient.exists("/Unicorn", false);
        System.out.println(stat==null ? "not exist":"exist");
    }

}
