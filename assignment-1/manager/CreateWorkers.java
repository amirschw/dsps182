package dsps.ass1.manager;

public class CreateWorkers implements Runnable {
    
    private int workersNeeded;

    public CreateWorkers(int workersNeeded) {
        this.workersNeeded = workersNeeded;
    }

    // Create more worker nodes if necessary
    @Override
    public void run() {
        int workersCount = EC2Helper.countWorkers(Manager.ec2);
        int extraWorkersNeeded = workersNeeded - workersCount;
        
        if (extraWorkersNeeded > 0) {
            System.out.printf("Starting %d additional worker nodes\n", extraWorkersNeeded);
            EC2Helper.createWorkers(Manager.ec2, extraWorkersNeeded);
        } else {
            System.out.println("Enough workers are running");
        }
    }

}
