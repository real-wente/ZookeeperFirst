package zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wentao
 * @date 2022-08-05  17:27
 */

public class DistributeClient {

    private static String connecting = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zk;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        DistributeClient client = new DistributeClient();
        client.getConnect();
        client.getServerList();
        client.business();

        //1 zk连接
        //2 监听结点变化
        //3 业务逻辑 睡觉
    }
    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getServerList() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren("/servers", true);

        ArrayList<String> servers = new ArrayList<>();

        for (String child : children) {

            byte[] data = zk.getData("/servers/" + child, false, null);

            servers.add(new String(data));
        }
        // 打印
        System.out.println(servers);
    }

    private void getConnect() throws IOException {
        zk = new ZooKeeper(connecting, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

                try {
                    getServerList();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
