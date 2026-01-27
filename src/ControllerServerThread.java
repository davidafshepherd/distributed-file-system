import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Thread used to handle a connection between the Controller (Server) and a Dstore or client (Client)
 * From the server side
 */
public class ControllerServerThread implements Runnable {

    //Endpoint of connection that this thread handles
    private final Socket sender;

    /**
     * Thread's constructor
     * @param sender endpoint of connection that this thread handles
     */
    public ControllerServerThread(Socket sender) {
        this.sender = sender;
    }

    /**
     * Thread's run method
     */
    public void run() {
        try {
            //Whether we should close the connection or not
            boolean closeConnection = true;

            //Creates objects used to print and read from a buffered textual stream
            PrintWriter out = new PrintWriter(sender.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(sender.getInputStream()));
            String line;

            //Handles each request sent through the connection that this thread handles
            while((line = in.readLine()) != null) {
                //Splits the request up
                String[] array = line.split(" ");

                //Handles a Dstore STORE_ACK acknowledgement
                if (Controller.checkIfDstoreStoreACK(array)) {Controller.handleDstoreStoreACK(sender, array[1]);}

                //Handles a Dstore REMOVE_ACK acknowledgement
                else if (Controller.checkIfDstoreRemoveACK(array)) {Controller.handleDstoreRemoveACK(sender, array[1]);}

                //Handles a Dstore LIST response
                else if (Controller.checkIfDstoreListResponse(sender, array)) {
                    List<String> files = new ArrayList<>(Arrays.asList(array));
                    files.removeFirst();
                    Controller.handleDstoreListResponse(sender, files);
                }

                //Handles a Dstore REBALANCE_COMPLETE acknowledgement
                else if (Controller.checkIfDstoreReBalanceACK(array)) {Controller.handleDstoreReBalanceACK(sender);}

                //Handles a Dstore JOIN request
                else if (Controller.checkIfDstoreJoinRequest(array)) {
                    synchronized (Controller.reBalanceMonitor) {
                        //Handles the Dstore JOIN request
                        Controller.handleDstoreJoinRequest(sender, Integer.parseInt(array[1]));

                        //Creates a new thread to handle the messages from this Dstore
                        //Initiates a REBALANCE operation
                        new Thread(new ControllerServerThread(sender)).start();
                        Controller.handleReBalanceOperation();

                        //Once the REBALANCE operation is completed
                        //Ends this thread as there is a new thread handling this connection
                        closeConnection = false;
                        break;
                    }
                }

                //Handles client requests
                else if (Controller.checkIfClientRequest(array)) {
                    //Handles incrementing the number of ongoing client requests
                    synchronized (Controller.askForReBalanceMonitor) {
                        if (Controller.askForReBalance) {
                            while (Controller.askForReBalance) Controller.askForReBalanceMonitor.wait();
                        }
                        synchronized (Controller.ongoingRequestsMonitor) {Controller.ongoingRequests++;}
                    }

                    //Handles the client request
                    handleClientRequests(array, out);

                    //Handles decrementing the number of ongoing client requests
                    synchronized (Controller.askForReBalanceMonitor) {
                        if (Controller.askForReBalance) {Controller.reBalancePauseLatch.countDown();}
                        synchronized (Controller.ongoingRequestsMonitor) {Controller.ongoingRequests--;}
                    }
                }
            }

            //If the connection dropped belongs to a Dstore, removes it
            if (closeConnection && Controller.DstoreSocketsToPorts.containsKey(sender)) Controller.handleDstoreConnectionDropped(sender);

            //Closes the connection
            if (closeConnection) sender.close();
        }
        catch(Exception e) {System.err.println("error: " + e);}
    }

    /**
     * Handles client requests
     * @param request client request
     * @param out object used to send a response to the client
     */
    public void handleClientRequests(String[] request, PrintWriter out) {
        //Handles a client request when there aren't enough Dstores
        if (Controller.Dstores < Controller.R) {
            if (Controller.checkIfClientRequest(request)) {out.println("ERROR_NOT_ENOUGH_DSTORES");}
        }

        //Handles a client LIST request
        else if (Controller.checkIfClientListRequest(request)) {Controller.handleClientListRequest(sender, out);}

        //Handles a client STORE request
        else if (Controller.checkIfClientStoreRequest(request)) {Controller.handleClientStoreRequest(sender, out, request[1], Integer.parseInt(request[2]));}

        //Handles a client LOAD request
        else if (Controller.checkIfClientLoadRequest(request)) {Controller.handleClientLoadRequest(sender, out, request[1]);}

        //Handles a client RELOAD request
        else if (Controller.checkIfClientReloadRequest(request)) {Controller.handleClientReloadRequest(sender, out, request[1]);}

        //Handles a client REMOVE request
        else if (Controller.checkIfClientRemoveRequest(request)) {Controller.handleClientRemoveRequest(sender, out, request[1]);}
    }
}
