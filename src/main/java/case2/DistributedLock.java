package case2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author wentao
 * @date 2022-08-09  11:01
 */

public class DistributedLock {

    private final String connectString = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
    private final int sessionTimeout= 2000;
    private final ZooKeeper zk;
    private CountDownLatch connectLatch = new CountDownLatch(1);
    private CountDownLatch waitLatch = new CountDownLatch(1);
    private String waitPath;
    private String currentMode;

    public DistributedLock() throws IOException, InterruptedException, KeeperException {
        // 获取连接

        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // connectLatch 如果连接上zk 可以释放
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected){
                    connectLatch.countDown();
                }
                // waitLatch 需要释放
                if (watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getType().equals(waitPath)){
                    waitLatch.countDown();
                }

            }
        });
        // 等待zookeeper正常连接后往下走
        connectLatch.await();
        // 判断根节点/locks是否存在
        Stat stat = zk.exists("/locks", false);
        if (stat == null){
            // 创建一下根节点
            zk.create("/locks","locks".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }
    }


    public void zklock(){
        // 对zk加锁

        // 创建临时带序号节点
        try {
            currentMode = zk.create("/locks/" + "seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            // 判断创建的节点是否是最小的序号节点，如果是获取到锁，如果不是，监听他序号的前一个节点
            List<String> children = zk.getChildren("/locks", false);
            // 如果Children只有一个值，那就直接获取锁，如果有多个节点那就进行判断谁最小
            if (children.size() == 1 ){
                return;
            }else{
                // 使用collections集合对children进行排序
                Collections.sort(children);
                // 获取对应的结点名称 seq-00000000
                String thisNode = currentMode.substring("/locks".length());
                // 通过seq-00000000获取该节点在children集合的位置
                int index = children.indexOf(thisNode);
                if (index == -1) {
                    System.out.println("数据异常");
                }else if (index == 0){
                    // 只有一个结点，可以获取锁
                    return;
                }else {
                    // 需要监听，他前一个节点的变化
                    waitPath = "/locks/"+children.get(index - 1);
                    zk.getData("waitPath",true,null);

                    // 等待监听
                    waitLatch.await();
                    return;
                }
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public void unZklock(){
        // 解锁

        // 删除节点
        try {
            zk.delete(currentMode,-1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }

    }


}
