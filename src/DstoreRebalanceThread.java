import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Map;

/**
 * Thread used to handle a connection from a Dstore (client) to a Dstore (server).
 */
public class DstoreRebalanceThread implements Runnable {

    private final Dstore dstore;                // Dstore that is communicating with a Dstore.
    private final Socket Dstore;                // Endpoint of connection that this thread handles.
    private final Map<String, Integer> files;   // Index of the files, that the Dstore has to send, to their sizes.

    /**
     * Thread's constructor.
     * @param dstore Dstore that is communicating with a Dstore.
     * @param Dstore endpoint of connection that this thread handles.
     * @param files index of the files, that the Dstore has to send, to their sizes.
     */
    public DstoreRebalanceThread(Dstore dstore, Socket Dstore, Map<String, Integer> files) {
        this.dstore = dstore;
        this.Dstore = Dstore;
        this.files = files;
    }

    /**
     * Thread's run method.
     */
    public void run() {
        try {
            // Create objects used to print and read from a buffered textual stream.
            PrintWriter out = new PrintWriter(Dstore.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(Dstore.getInputStream()));
            String line;


            // Send each file.
            boolean reBalanceComplete = true;
            for (String fileName : files.keySet()) {
                out.println("REBALANCE_STORE " + fileName + " " + files.get(fileName));

                line = in.readLine();

                if (line.equals("ACK")) dstore.handleClientLoadDataRequest(Dstore, fileName);
                else reBalanceComplete = false;
            }

            // Tell the Dstore that all files were sent.
            if (reBalanceComplete) dstore.handleThreadAllFilesSent(Dstore);

            // Close the connection.
            Dstore.close();
        }
        catch(Exception e) {System.err.println("error: " + e);}
    }
}
