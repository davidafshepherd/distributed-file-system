import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Thread used to handle a connection from the Controller (server) to a Dstore or a client (client).
 */
public class ControllerServerThread implements Runnable {

    // Endpoint of connection that this thread handles.
    private final Socket sender;

    /**
     * Thread's constructor.
     * @param sender endpoint of connection that this thread handles.
     */
    public ControllerServerThread(Socket sender) {
        this.sender = sender;
    }

    /**
     * Thread's run method
     */
    public void run() {
        try {
            // Whether we should close the connection or not.
            boolean closeConnection = true;

            // Create objects used to print and read from a buffered textual stream.
            PrintWriter out = new PrintWriter(sender.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(sender.getInputStream()));
            String line;

            // Handle each request sent through the connection that this thread handles.
            while((line = in.readLine()) != null) {
                // Split the request up.
                String[] array = line.split(" ");

                // Handle a Dstore STORE_ACK acknowledgement.
                if (Controller.checkIfDstoreStoreACK(array)) {Controller.handleDstoreStoreACK(sender, array[1]);}

                // Handle a Dstore REMOVE_ACK acknowledgement.
                else if (Controller.checkIfDstoreRemoveACK(array)) {Controller.handleDstoreRemoveACK(sender, array[1]);}

                // Handle a Dstore LIST response.
                else if (Controller.checkIfDstoreListResponse(sender, array)) {
                    List<String> files = new ArrayList<>(Arrays.asList(array));
                    files.removeFirst();
                    Controller.handleDstoreListResponse(sender, files);
                }

                // Handle a Dstore REBALANCE_COMPLETE acknowledgement.
                else if (Controller.checkIfDstoreReBalanceACK(array)) {Controller.handleDstoreReBalanceACK(sender);}

                // Handle a Dstore JOIN request.
                else if (Controller.checkIfDstoreJoinRequest(array)) {
                    synchronized (Controller.reBalanceMonitor) {
                        // Handle the Dstore JOIN request.
                        Controller.handleDstoreJoinRequest(sender, Integer.parseInt(array[1]));

                        // Create a new thread to handle the messages from this Dstore. Initiate a REBALANCE operation.
                        new Thread(new ControllerServerThread(sender)).start();
                        Controller.handleReBalanceOperation();

                        // End this thread as there is a new thread handling this connection.
                        closeConnection = false;
                        break;
                    }
                }

                // Handle client requests.
                else if (Controller.checkIfClientRequest(array)) {
                    // Handle incrementing the number of ongoing client requests.
                    synchronized (Controller.askForReBalanceMonitor) {
                        if (Controller.askForReBalance) {
                            while (Controller.askForReBalance) Controller.askForReBalanceMonitor.wait();
                        }
                        synchronized (Controller.ongoingRequestsMonitor) {Controller.ongoingRequests++;}
                    }

                    // Handle the client request.
                    handleClientRequests(array, out);

                    // Handle decrementing the number of ongoing client requests.
                    synchronized (Controller.askForReBalanceMonitor) {
                        if (Controller.askForReBalance) {Controller.reBalancePauseLatch.countDown();}
                        synchronized (Controller.ongoingRequestsMonitor) {Controller.ongoingRequests--;}
                    }
                }
            }

            // If the connection dropped belongs to a Dstore, remove the Dstore.
            if (closeConnection && Controller.DstoreSocketsToPorts.containsKey(sender)) Controller.handleDstoreConnectionDropped(sender);

            // Close the connection.
            if (closeConnection) sender.close();
        }
        catch(Exception e) {System.err.println("error: " + e);}
    }

    /**
     * Handles client requests.
     * @param request client request.
     * @param out object used to send a response to the client.
     */
    public void handleClientRequests(String[] request, PrintWriter out) {
        //Handle a client request when there aren't enough Dstores.
        if (Controller.Dstores < Controller.R) {
            if (Controller.checkIfClientRequest(request)) {out.println("ERROR_NOT_ENOUGH_DSTORES");}
        }

        // Handle a client LIST request.
        else if (Controller.checkIfClientListRequest(request)) {Controller.handleClientListRequest(sender, out);}

        // Handle a client STORE request.
        else if (Controller.checkIfClientStoreRequest(request)) {Controller.handleClientStoreRequest(sender, out, request[1], Integer.parseInt(request[2]));}

        // Handle a client LOAD request.
        else if (Controller.checkIfClientLoadRequest(request)) {Controller.handleClientLoadRequest(sender, out, request[1]);}

        // Handle a client RELOAD request.
        else if (Controller.checkIfClientReloadRequest(request)) {Controller.handleClientReloadRequest(sender, out, request[1]);}

        // Handle a client REMOVE request.
        else if (Controller.checkIfClientRemoveRequest(request)) {Controller.handleClientRemoveRequest(sender, out, request[1]);}
    }
}
