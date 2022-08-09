package case2;

/**
 * @author wentao
 * @date 2022-08-09  15:17
 */

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class DistributedLockTest {

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {

        final  DistributedLock lock1 = new DistributedLock();

        final  DistributedLock lock2 = new DistributedLock();

        new Thread(new Runnable() {
            @Override
            public void run() {
                lock1.zklock();
                System.out.println("线程1 启动，获取到锁");
                try {
                    Thread.sleep(5*1000);
                    lock1.unZklock();
                    System.out.println("线程1 释放锁");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();


        new Thread(new Runnable() {
            @Override
            public void run() {
                lock2.zklock();
                System.out.println("线程2 启动，获取到锁");
                try {
                    Thread.sleep(5*1000);
                    lock2.unZklock();
                    System.out.println("线程2 释放锁");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

    }
}