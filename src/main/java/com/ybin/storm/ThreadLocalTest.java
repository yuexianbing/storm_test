package com.ybin.storm;

/**
 * @author yuebing
 * @version 1.0 2017/9/4
 */
public class ThreadLocalTest {
    public static void main(String[] args) {
        final ThreadLocal<String> local = new ThreadLocal<String>();

        class InnerThreadRun extends Thread{
            @Override
            public void run() {
                local.set("yuebing");
                System.out.println("线程1"+Thread.currentThread().getName() + ",值="+local.get());
            }
        }
        class InnerThreadRun2 extends Thread{
            @Override
            public void run() {
                System.out.println("线程2"+Thread.currentThread().getName() + ",值="+local.get());
            }
        }
        InnerThreadRun run1 = new InnerThreadRun();
        InnerThreadRun2 run2 = new InnerThreadRun2();
        run1.start();
        run2.start();
    }
}
