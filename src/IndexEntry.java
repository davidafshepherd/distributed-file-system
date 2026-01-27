import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Class used to represent an entry in the index
 */
public class IndexEntry {

    //Size of file that this entry belongs to
    private final int fileSize;
    //Ports of the Dstores used to store the file that this entry belongs to
    private final List<Integer> DstorePorts;
    //State of the file that this entry belongs to
    private String fileState;
    //Number of STORE_ACKs received for the file that this entry belongs to
    private final CountDownLatch storeACKs;
    //Number of REMOVE_ACKs received for the file that this entry belongs to
    private final CountDownLatch removeACKs;

    /**
     * Constructs an entry in the Index
     * @param fileSize size of file that this entry belongs to
     * @param DstorePorts ports of the Dstores used to store the file that this entry belongs to
     * @param fileState state of the file that this entry belongs to
     */
    public IndexEntry(int fileSize, List<Integer> DstorePorts, String fileState, int R) {
        this.fileSize = fileSize;
        this.DstorePorts = DstorePorts;
        this.fileState = fileState;
        this.storeACKs = new CountDownLatch(R);
        this.removeACKs = new CountDownLatch(R);
    }

    /**
     * Gets this entry's file size
     * @return file size
     */
    public int getFileSize() {
        return this.fileSize;
    }

    /**
     * Gets this entry's Dstore ports
     * @return Dstore ports
     */
    public List<Integer> getDstorePorts() {
        return this.DstorePorts;
    }

    /**
     * Gets this entry's file state
     * @return file state
     */
    public String getFileState() {
        return this.fileState;
    }

    /**
     * Gets this entry's store ACKs
     * @return store ACKs
     */
    public CountDownLatch getStoreACKs() {
        return this.storeACKs;
    }

    /**
     * Gets this entry's remove ACKs
     * @return remove ACKs
     */
    public CountDownLatch getRemoveACKs() {
        return this.removeACKs;
    }

    /**
     * Changes this entry's file state to a new state
     * @param newState new state
     */
    public void setFileState(String newState) {
        this.fileState = newState;
    }

    /**
     * Removes a port from this entry's Dstore ports
     * @param port port being removed
     */
    public void removePort(Integer port) {
        this.DstorePorts.remove(port);
    }

    /**
     * Adds a port to this entry's Dstore ports
     * @param port port being added
     */
    public void addPort(Integer port) {
        this.DstorePorts.add(port);
    }
}
