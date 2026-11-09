import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Thread used to handle a connection from a Dstore (client) to the Controller (server).
 */
public class DstoreClientThread implements Runnable {

    private final Dstore dstore;        // Dstore that is communicating with the Controller.
    private final Socket controller;    // Endpoint of connection that this thread handles.
    private final int port;             // Port that the Dstore listens to.

    /**
     * Thread's constructor.
     * @param dstore Dstore that is communicating with the Controller.
     * @param controller endpoint of connection that this thread handles.
     * @param port port that the Dstore listens to.
     */
    public DstoreClientThread(Dstore dstore, Socket controller, int port) {
        this.dstore = dstore;
        this.controller = controller;
        this.port = port;
    }

    /**
     * Thread's run method.
     */
    public void run() {
        try {
            // Create objects used to print and read from a buffered textual stream.
            PrintWriter out = new PrintWriter(controller.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(controller.getInputStream()));
            String line;

            // Ask to join the controller.
            out.println("JOIN " + port);

            // Handle each request sent through the connection that this thread handles.
            while((line = in.readLine()) != null) {
                // Split the request up.
                String[] array = line.split(" ");

                // Handle a Controller REMOVE request.
                if (Dstore.checkIfControllerRemoveRequest(array)) {dstore.handleControllerRemoveRequest(controller, out, array[1]);}

                // Handle a Controller LIST request.
                else if (Dstore.checkIfControllerListRequest(array)) {dstore.handleControllerListRequest(controller, out);}

                // Handle as Controller REBALANCE request.
                else if (Dstore.checkIfControllerReBalanceRequest(array)) {
                    List<String> request = new ArrayList<>(Arrays.asList(array));
                    request.removeFirst();
                    dstore.handleControllerReBalanceRequest(controller, out, request);
                }
            }

            // Close the connection.
            controller.close();
        }
        catch(Exception e) {System.err.println("error: " + e);}
    }
}
