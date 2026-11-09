import java.util.TimerTask;

/**
 * Timer task used to start the REBALANCE operation every re-balance period.
 */
public class ReBalanceTimerTask extends TimerTask {

    /**
     * Timer task's run method.
     */
    public void run() {
        // Start the REBALANCE operation every re-balance period.
        synchronized (Controller.reBalanceMonitor) {Controller.handleReBalanceOperation();}
    }
}
