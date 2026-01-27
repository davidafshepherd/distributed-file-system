import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Map;

/**
 * Thread used to handle a connection between a Dstore (Server) and a Dstore (Client)
 * From the client side
 */
public class DstoreRebalanceThread implements Runnable {

    //Dstore that is communicating with a Dstore
    private final Dstore dstore;
    //Endpoint of connection that this thread handles
    private final Socket Dstore;
    //Index of the files, that the Dstore has to send, to their sizes
    private final Map<String, Integer> files;

    /**
     * Thread's constructor
     * @param dstore Dstore that is communicating with a Dstore
     * @param Dstore endpoint of connection that this thread handles
     * @param files index of the files, that the Dstore has to send, to their sizes
     */
    public DstoreRebalanceThread(Dstore dstore, Socket Dstore, Map<String, Integer> files) {
        this.dstore = dstore;
        this.Dstore = Dstore;
        this.files = files;
    }

    /**
     * Thread's run method
     */
    public void run() {
        try {
            //Creates objects used to print and read from a buffered textual stream
            PrintWriter out = new PrintWriter(Dstore.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(Dstore.getInputStream()));
            String line;


            //Sends each file
            boolean reBalanceComplete = true;
            for (String fileName : files.keySet()) {
                out.println("REBALANCE_STORE " + fileName + " " + files.get(fileName));

                line = in.readLine();

                if (line.equals("ACK")) dstore.handleClientLoadDataRequest(Dstore, fileName);
                else reBalanceComplete = false;
            }

            //Tells the Dstore that all files were sent
            if (reBalanceComplete) dstore.handleThreadAllFilesSent(Dstore);

            //Closes the connection
            Dstore.close();
        }
        catch(Exception e) {System.err.println("error: " + e);}
    }
}
