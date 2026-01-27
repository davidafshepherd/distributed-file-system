import java.io.*;
import java.net.Socket;

/**
 * Thread used to handle a connection between a Dstore (Server) and a client (Client)
 * From the server side
 */
public class DstoreServerThread implements Runnable {

    //Dstore that is communicating with a client
    private final Dstore dstore;
    //Endpoint of connection that this thread handles
    private final Socket client;

    /**
     * Thread's constructor
     * @param dstore Dstore that is communicating with a client
     * @param client endpoint of connection that this thread handles
     */
    public DstoreServerThread(Dstore dstore, Socket client) throws FileNotFoundException {
        this.dstore = dstore;
        this.client = client;
    }

    /**
     * Thread's run method
     */
    public void run() {
        try {
            //Creates objects used to print and read from a buffered textual stream
            PrintWriter out = new PrintWriter(client.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
            String line;

            //Handles each request sent through the connection that this thread handles
            while((line = in.readLine()) != null) {
                //Splits the request up
                String[] array = line.split(" ");

                //Handles a client STORE request
                if (Dstore.checkIfClientStoreRequest(array)) {dstore.handleClientStoreRequest(client, out, array[1], Integer.parseInt(array[2]));}

                //Handles a client LOAD_DATA request
                if (Dstore.checkIfClientLoadDataRequest(array)) {dstore.handleClientLoadDataRequest(client, array[1]);}

                //Handles a Dstore REBALANCE_STORE request
                if (Dstore.checkIfDstoreStoreRequest(array)) {dstore.handleClientStoreRequest(client, out, array[1], Integer.parseInt(array[2]));}
            }

            //Closes the connection
            client.close();
        }
        catch(Exception e) {System.err.println("error: " + e);}
    }
}
