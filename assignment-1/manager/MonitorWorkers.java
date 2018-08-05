package dsps.ass1.manager;

import dsps.ass1.manager.Manager;

public class MonitorWorkers implements Runnable {

    private static final int SLEEP_TIME = 30000;
    static int workersNeeded = 0;
    static Object lock = new Object();
    
    // Monitor workers to ensure enough worker nodes are running
    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                Thread.sleep(SLEEP_TIME);
                Manager.nodesCreationPool.execute(new CreateWorkers(workersNeeded));
            }
        } catch (InterruptedException ex) {
            System.err.println("Caught an exception while monitoring workers");
            ex.printStackTrace();
        }
    }
    
}
