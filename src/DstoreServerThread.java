import java.io.*;
import java.net.Socket;

/**
 * Thread used to handle a connection from a Dstore (server) to a client (client).
 */
public class DstoreServerThread implements Runnable {

    private final Dstore dstore;    // Dstore that is communicating with a client.
    private final Socket client;    // Endpoint of connection that this thread handles.

    /**
     * Thread's constructor.
     * @param dstore Dstore that is communicating with a client.
     * @param client endpoint of connection that this thread handles.
     */
    public DstoreServerThread(Dstore dstore, Socket client) throws FileNotFoundException {
        this.dstore = dstore;
        this.client = client;
    }

    /**
     * Thread's run method.
     */
    public void run() {
        try {
            // Create objects used to print and read from a buffered textual stream.
            PrintWriter out = new PrintWriter(client.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
            String line;

            // Handle each request sent through the connection that this thread handles.
            while((line = in.readLine()) != null) {
                // Split the request up.
                String[] array = line.split(" ");

                // Handle a client STORE request.
                if (Dstore.checkIfClientStoreRequest(array)) {dstore.handleClientStoreRequest(client, out, array[1], Integer.parseInt(array[2]));}

                // Handle a client LOAD_DATA request.
                if (Dstore.checkIfClientLoadDataRequest(array)) {dstore.handleClientLoadDataRequest(client, array[1]);}

                // Handle a Dstore REBALANCE_STORE request.
                if (Dstore.checkIfDstoreStoreRequest(array)) {dstore.handleClientStoreRequest(client, out, array[1], Integer.parseInt(array[2]));}
            }

            // Close the connection.
            client.close();
        }
        catch(Exception e) {System.err.println("error: " + e);}
    }
}
