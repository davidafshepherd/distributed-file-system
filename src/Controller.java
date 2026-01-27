import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Class used to handle communication and requests sent to the Controller
 */
public class Controller {

    //Minimum number of Dstores needed to be connected to the Controller
    static int R;
    //How long the Controller should wait for a response in milliseconds
    static int timeout;

    //Number of Dstores connected to the Controller
    static int Dstores = 0;
    //List used to store the port of each Dstore connected to the Controller
    static final List<Integer> DstorePorts = new ArrayList<>();
    //Index of TCP connections with Dstores to the ports that the Dstores listen on
    static final Map<Socket, Integer> DstoreSocketsToPorts = new HashMap<>();
    //Index of ports that Dstores listen on to the TCP connections with the Dstores
    static final Map<Integer, Socket> DstorePortsToSockets = new HashMap<>();
    //Index of ports that Dstores listen on to the number of files that they store
    static final Map<Integer, Integer> DstorePortsToFileNumbers = new HashMap<>();

    //Index of file names to their sizes, ports of Dstores that store them and their current state
    static final Map<String, IndexEntry> index = new HashMap<>();
    //Index of sockets and the ports of Dstores that can be used to load a file from
    static final Map<Socket, List<Integer>> loadPorts = new HashMap<>();

    //Whether the Controller is asking to start a REBALANCE operation or not
    static boolean askForReBalance = false;
    //Object as a monitor to queue incoming JOIN and client requests
    static final Object askForReBalanceMonitor = new Object();
    //Object used as a monitor to queue any other REBALANCE operations
    static final Object reBalanceMonitor = new Object();

    //Number of requests still being carried out
    static int ongoingRequests = 0;
    //Object used as a monitor to queue processes trying to access the number of requests still being carried out
    static final Object ongoingRequestsMonitor = new Object();
    //Latch used to pause a REBALANCE operation while requests are still being carried out
    static CountDownLatch reBalancePauseLatch;

    //Latch used to pause a REBALANCE operation while Dstores send a list of the files they store
    static CountDownLatch filesPauseLatch = new CountDownLatch(0);
    //Index of sockets to the list of files that they store
    static final Map<Socket, List<String>> DstoresToFiles = new HashMap<>();

    //Index of the sockets of Dstores to the files that they should remove
    static Map<Socket, List<String>> filesToDelete;
    //Index of the sockets of Dstores to the files that they should send
    static Map<Socket, List<String>> filesToAdd;
    //Index of the sockets of Dstores to the files that they should now store
    static Map<Socket, Integer> fileNumbers;
    //Index of the sockets of Dstores to the file re-allocations they should perform
    static final Map<Socket, String> fileAllocations = new HashMap<>();
    //Latch used to pause a REBALANCE operation while Dstores complete a REBALANCE operation
    static CountDownLatch reBalanceCompletePauseLatch = new CountDownLatch(0);


    /**
     * Controller's main method
     * @param args arguments passed into the Controller
     */
    public static void main(String[] args) {
        //Checks if the arguments given to this main method are in a valid format
        if (!checkArguments(args)) System.err.println("error: " + new IllegalArgumentException());

        //Stores the arguments given to the main method
        int port = Integer.parseInt(args[0]);
        R = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        int reBalancePeriod = Integer.parseInt(args[3]) * 1000;

        //Creates a timer task to handle the REBALANCE operation
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new ReBalanceTimerTask(), reBalancePeriod, reBalancePeriod);

        //Sets the Controller's server socket to null
        ServerSocket socket = null;

        //Handles each connection established with the Controller
        try {
            //Initialises the Controller's server socket
            socket = new ServerSocket(port);

            //Creates a thread for each connection established with the Controller
            //Where each thread handles that connection
            while (true) {
                try {
                    Socket sender = socket.accept();
                    new Thread(new ControllerServerThread(sender)).start();
                }
                catch(Exception e) { System.err.println("error: " + e); }
            }
        }
        catch(Exception e) {System.err.println("error: " + e);}
        finally {
            //Closes the connection if it's still open
            if (socket != null) {
                try { socket.close(); }
                catch (IOException e) { System.err.println("error: " + e); }
            }
        }
    }

    /**
     * Checks if the arguments given to the main method are in a valid format
     * @param args arguments given to the main method
     * @return whether the arguments given to the main method are in a valid format
     */
    public static boolean checkArguments(String[] args) {
        if (args.length != 4) return false;
        try {
            Integer.parseInt(args[0]);
            Integer.parseInt(args[1]);
            Integer.parseInt(args[2]);
            Integer.parseInt(args[3]);
            return true;
        } catch (NumberFormatException e) {return false;}
    }

    /**
     * Checks if a request is a valid JOIN request sent by a Dstore
     * @param request request received by controller
     * @return whether the request is a valid JOIN request sent by a Dstore or not
     */
    public static boolean checkIfDstoreJoinRequest(String[] request) {
        if (request.length != 2) return false;
        if (!request[0].equals("JOIN")) return false;
        try {
            Integer.parseInt(request[1]);
            return true;
        } catch (NumberFormatException e) {return false;}
    }

    /**
     * Checks if an acknowledgement is a valid STORE_ACK acknowledgement sent by a Dstore
     * @param acknowledgement acknowledgement received by controller
     * @return whether the acknowledgement is a valid STORE_ACK acknowledgement sent by a Dstore or not
     */
    public static boolean checkIfDstoreStoreACK(String[] acknowledgement) {
        if (acknowledgement.length != 2) return false;
        return acknowledgement[0].equals("STORE_ACK");
    }

    /**
     * Checks if an acknowledgement is a valid REMOVE_ACK acknowledgement sent by a Dstore
     * @param acknowledgement acknowledgement received by controller
     * @return whether the acknowledgement is a valid REMOVE_ACK acknowledgement sent by a Dstore or not
     */
    public static boolean checkIfDstoreRemoveACK(String[] acknowledgement) {
        if (acknowledgement.length != 2) return false;
        return acknowledgement[0].equals("REMOVE_ACK") || acknowledgement[0].equals("ERROR_FILE_DOES_NOT_EXIST");
    }

    /**
     * Checks if a response is a valid LIST response sent by a Dstore
     * @param response response received by controller
     * @return whether the response is a valid LIST response sent by a Dstore or not
     */
    public static boolean checkIfDstoreListResponse(Socket Dstore, String[] response) {
        synchronized (DstorePorts) {if (!DstoreSocketsToPorts.containsKey(Dstore)) return false;}
        if (response.length < 1) return false;
        return response[0].equals("LIST");
    }

    /**
     * Checks if an acknowledgement is a valid REBALANCE_COMPLETE acknowledgement sent by a Dstore
     * @param acknowledgement acknowledgement received by controller
     * @return whether the acknowledgement is a valid REBALANCE_COMPLETE acknowledgement sent by a Dstore or not
     */
    public static boolean checkIfDstoreReBalanceACK(String[] acknowledgement) {
        if (acknowledgement.length != 1) return false;
        return acknowledgement[0].equals("REBALANCE_COMPLETE");
    }

    /**
     * Checks if a request is a valid request sent by a client
     * @param request request received by controller
     * @return whether the request is a valid request sent by a client or not
     */
    public static boolean checkIfClientRequest(String[] request) {
        return checkIfClientListRequest(request) || checkIfClientStoreRequest(request) || checkIfClientLoadRequest(request) || checkIfClientReloadRequest(request) || checkIfClientRemoveRequest(request);
    }

    /**
     * Checks if a request is a valid LIST request sent by a client
     * @param request request received by controller
     * @return whether the request is a valid LIST request sent by a client or not
     */
    public static boolean checkIfClientListRequest(String [] request) {
        if (request.length != 1) return false;
        return request[0].equals("LIST");
    }

    /**
     * Checks if a request is a valid STORE request sent by a client
     * @param request request received by controller
     * @return whether the request is a valid STORE request sent by a client or not
     */
    public static boolean checkIfClientStoreRequest(String[] request) {
        if (request.length != 3) return false;
        if (!request[0].equals("STORE")) return false;
        try {
            Integer.parseInt(request[2]);
            return true;
        } catch (NumberFormatException e) {return false;}
    }

    /**
     * Checks if a request is a valid LOAD request sent by a client
     * @param request request received by controller
     * @return whether the request is a valid LOAD request sent by a client or not
     */
    public static boolean checkIfClientLoadRequest(String [] request) {
        if (request.length != 2) return false;
        return request[0].equals("LOAD");
    }

    /**
     * Checks if a request is a valid RELOAD request sent by a client
     * @param request request received by controller
     * @return whether the request is a valid RELOAD request sent by a client or not
     */
    public static boolean checkIfClientReloadRequest(String [] request) {
        if (request.length != 2) return false;
        return request[0].equals("RELOAD");
    }

    /**
     * Checks if a request is a valid REMOVE request sent by a client
     * @param request request received by controller
     * @return whether the request is a valid REMOVE request sent by a client or not
     */
    public static boolean checkIfClientRemoveRequest(String [] request) {
        if (request.length != 2) return false;
        return request[0].equals("REMOVE");
    }

    /**
     * Handles a Dstore JOIN request
     * @param Dstore endpoint of the connection with the Dstore
     * @param DstorePort port given by the Dstore asking to join
     */
    public static void handleDstoreJoinRequest(Socket Dstore, int DstorePort) {
        synchronized (DstorePorts) {
            DstoreSocketsToPorts.put(Dstore, DstorePort);
            DstorePortsToSockets.put(DstorePort, Dstore);
            DstorePortsToFileNumbers.put(DstorePort, 0);
            DstorePorts.add(DstorePort);
            Dstores++;
        }
    }

    /**
     * Handles the connection between the controller and Dstore dropping
     * @param Dstore Dstore whose connection dropped
     */
    public static void handleDstoreConnectionDropped(Socket Dstore) {
        Integer DstorePort;

        //Removes Dstore from Dstore info
        synchronized (DstorePorts) {
            DstorePort = DstoreSocketsToPorts.get(Dstore);
            DstoreSocketsToPorts.remove(Dstore);
            DstorePortsToSockets.remove(DstorePort);
            DstorePortsToFileNumbers.remove(DstorePort);
            DstorePorts.remove(DstorePort);
            Dstores--;
        }

        //Removes Dstore's port from the index
        synchronized (index) {
            for (IndexEntry entry : index.values()) {entry.removePort(DstorePort);}
        }
    }

    /**
     * Handles a client LIST request
     * @param client endpoint of the connection with the client
     * @param out object used to send a response to the client
     */
    public static void handleClientListRequest(Socket client, PrintWriter out) {
        //Reset ports to be used for a load request by the client
        synchronized (loadPorts) {loadPorts.remove(client);}

        synchronized (index) {
            //Sends a LIST response
            StringBuilder files = new StringBuilder();
            for (String file : index.keySet()) {
                if (index.get(file).getFileState().equals("store complete")) files.append(" ").append(file);
            }
            out.println("LIST" + files);
        }
    }

    /**
     * Handles a client STORE request
     * @param client endpoint of the connection with the client
     * @param out object used to send a response to the client
     * @param fileName name of file that the client is asking to store
     * @param fileSize size of the file that the client is asking to store
     */
    public static void handleClientStoreRequest(Socket client, PrintWriter out, String fileName, int fileSize) {
        //Reset ports to be used for a load request by the client
        synchronized (loadPorts) {loadPorts.remove(client);}

        synchronized (index) {
            //Checks if the file already exists in the index
            if (index.containsKey(fileName)) {
                out.println("ERROR_FILE_ALREADY_EXISTS");
                return;
            }

            List<Integer> DstorePorts = new ArrayList<>();
            synchronized (Controller.DstorePorts) {
                //Check if there are R Dstores
                if (Dstores < R) {
                    out.println("ERROR_NOT_ENOUGH_DSTORES");
                    return;
                }

                //Selects R Dstores to store the file and stores their ports
                List<Integer> fileNumbers = new ArrayList<>(DstorePortsToFileNumbers.values());
                Collections.sort(fileNumbers);
                List<Integer> lowestFileNumbers = new ArrayList<>();
                for (int i = 0; i < R; i++) lowestFileNumbers.add(fileNumbers.get(i));
                for (int port : DstorePortsToFileNumbers.keySet()) {
                    if (!DstorePorts.contains(port) && lowestFileNumbers.contains(DstorePortsToFileNumbers.get(port))) {
                        DstorePorts.add(port);
                        lowestFileNumbers.remove(DstorePortsToFileNumbers.get(port));
                    }
                    if (DstorePorts.size() == R) break;
                }
                for (int port : DstorePorts) DstorePortsToFileNumbers.compute(port, (key, val) -> val+1);
            }

            //Updates the index
            IndexEntry entry = new IndexEntry(fileSize, DstorePorts, "store in progress", R);
            index.put(fileName, entry);

            //Sends a STORE_TO response
            StringBuilder ports = new StringBuilder();
            for (int port : DstorePorts) ports.append(" ").append(port);
            out.println("STORE_TO" + ports);
        }

        //Waits for an ACK from each Dstore
        //If an ACK from each Dstore is not received in time, removes the file's entry from the index
        try {
            boolean receivedAllACKs = index.get(fileName).getStoreACKs().await(timeout, TimeUnit.MILLISECONDS);
            if (receivedAllACKs) {
                index.get(fileName).setFileState("store complete");
                out.println("STORE_COMPLETE");
            }
            else {synchronized (index) {index.remove(fileName);}}
        }
        catch (Exception e) {System.err.println("error: " + e);}
    }

    /**
     * Handles a Dstore STORE_ACK acknowledgement
     * @param Dstore endpoint of the connection with the Dstore
     * @param fileName name of the file whose storage within a Dstore has been acknowledged
     */
    public static void handleDstoreStoreACK(Socket Dstore, String fileName) {
        synchronized (index) {
            if (DstoreSocketsToPorts.containsKey(Dstore) && index.containsKey(fileName)) {
                if (index.get(fileName).getDstorePorts().contains(DstoreSocketsToPorts.get(Dstore))) {
                    index.get(fileName).getStoreACKs().countDown();
                }
            }
        }
    }

    /**
     * Handles a client LOAD request
     * @param client endpoint of the connection with the client
     * @param out object used to send a response to the client
     * @param fileName name of file that the client is asking to load
     */
    public static void handleClientLoadRequest(Socket client, PrintWriter out, String fileName) {
        //Reset ports to be used for a load request by the client
        synchronized (loadPorts) {loadPorts.remove(client);}

        synchronized (index) {
            //Checks if the file doesn't exist or hasn't been stored yet
            if (!index.containsKey(fileName) || !index.get(fileName).getFileState().equals("store complete")) {
                out.println("ERROR_FILE_DOES_NOT_EXIST");
                return;
            }

            //Stores the ports of the Dstores that store the file
            List<Integer> ports = new ArrayList<>(index.get(fileName).getDstorePorts());
            synchronized (loadPorts) {loadPorts.put(client, ports);}

            //Sends a LOAD_FROM response
            out.println("LOAD_FROM " + loadPorts.get(client).getFirst() + " " + index.get(fileName).getFileSize());
            loadPorts.get(client).removeFirst();
        }
    }

    /**
     * Handles a client RELOAD request
     * @param client endpoint of the connection with the client
     * @param out object used to send a response to the client
     * @param fileName name of file that the client is asking to reload
     */
    public static void handleClientReloadRequest(Socket client, PrintWriter out, String fileName) {
        //If there are no more ports, send an ERROR_LOAD response
        synchronized (loadPorts) {
            if (!loadPorts.containsKey(client)) {
                out.println("ERROR_LOAD");
                return;
            }
            if (loadPorts.get(client).isEmpty()) {
                loadPorts.remove(client);
                out.println("ERROR_LOAD");
                return;
            }
        }

        synchronized (index) {
            //Checks if the file doesn't exist or hasn't been stored yet
            if (!index.containsKey(fileName) || !index.get(fileName).getFileState().equals("store complete")) {
                out.println("ERROR_FILE_DOES_NOT_EXIST");
                return;
            }

            //Sends a LOAD_FROM response
            out.println("LOAD_FROM " + loadPorts.get(client).getFirst() + " " + index.get(fileName).getFileSize());
            loadPorts.get(client).removeFirst();
        }
    }

    /**
     * Handles a client REMOVE request
     * @param client endpoint of the connection with the client
     * @param out object used to send a response to the client
     * @param fileName name of file that the client is asking to remove
     */
    public static void handleClientRemoveRequest(Socket client, PrintWriter out, String fileName) {
        //Reset ports to be used for a load request by the client
        synchronized (loadPorts) {loadPorts.remove(client);}

        synchronized (index) {
            //Checks if the file doesn't exist or hasn't been stored yet
            if (!index.containsKey(fileName) || !index.get(fileName).getFileState().equals("store complete")) {
                out.println("ERROR_FILE_DOES_NOT_EXIST");
                return;
            }

            //Updates the state of the file
            index.get(fileName).setFileState("remove in progress");

            //Stores the sockets of the Dstores that store the file
            List<Socket> dstoreSockets = new ArrayList<>();
            for (int port : index.get(fileName).getDstorePorts()) {
                dstoreSockets.add(DstorePortsToSockets.get(port));
                DstorePortsToFileNumbers.compute(port, (key, val) -> val-1);
            }


            //Sends a REMOVE request to each Dstore that stores the file
            for (Socket dstoreSocket : dstoreSockets) {
                try {
                    PrintWriter dstoreOut = new PrintWriter(dstoreSocket.getOutputStream(), true);
                    dstoreOut.println("REMOVE " + fileName);
                }
                catch (Exception e) {System.err.println("error: " + e);}
            }
        }

        //Waits for an ACK from each Dstore
        //If an ACK from each Dstore is not received in time, moves onto next request
        try {
            boolean receivedAllACKs = index.get(fileName).getRemoveACKs().await(timeout, TimeUnit.MILLISECONDS);
            if (receivedAllACKs) {
                synchronized (index) {
                    index.get(fileName).setFileState("remove complete");
                    index.remove(fileName);
                }
                out.println("REMOVE_COMPLETE");
            }
        }
        catch (Exception e) {System.err.println("error: " + e);}
    }

    /**
     * Handles a Dstore REMOVE_ACK acknowledgement
     * @param Dstore endpoint of the connection with the Dstore
     * @param filename name of the file whose removal from a Dstore has been acknowledged
     */
    public static void handleDstoreRemoveACK(Socket Dstore, String filename) {
        synchronized (index) {
            if (DstoreSocketsToPorts.containsKey(Dstore) && index.containsKey(filename)) {
                if (index.get(filename).getDstorePorts().contains(DstoreSocketsToPorts.get(Dstore))) {
                    index.get(filename).getRemoveACKs().countDown();
                }
            }
        }
    }

    public static void handleReBalanceOperation() {
        //If there aren't enough Dstores, ends REBALANCE operation
        synchronized (DstorePorts) {if (Dstores < R) {return;}}

        //Queues incoming JOIN and client requests
        synchronized (askForReBalanceMonitor) {
            askForReBalance = true;
            reBalancePauseLatch = new CountDownLatch(ongoingRequests);
        }

        try {
            //Waits for all ongoing requests to finish
            reBalancePauseLatch.await();

            //Asks each Dstore for a list of the files that it stores
            DstoresToFiles.clear();
            synchronized (DstoresToFiles) {filesPauseLatch = new CountDownLatch(Dstores);}
            for (Socket Dstore : DstoreSocketsToPorts.keySet()) {
                PrintWriter DstoreOut = new PrintWriter(Dstore.getOutputStream(), true);
                DstoreOut.println("LIST");
            }

            //Waits to receive the list of files from each Dstore
            filesPauseLatch.await(timeout, TimeUnit.MILLISECONDS);
            synchronized (DstoresToFiles) {filesPauseLatch = new CountDownLatch(0);}

            if (R <= DstoresToFiles.size()) {
                //Revises file allocation and sends a REBALANCE request to each Dstore
                fileAllocations.clear();
                reviseFileAllocation();
                synchronized (fileAllocations) {reBalanceCompletePauseLatch = new CountDownLatch(fileAllocations.size());}
                for (Socket Dstore : fileAllocations.keySet()) {
                    PrintWriter DstoreOut = new PrintWriter(Dstore.getOutputStream(), true);
                    DstoreOut.println(fileAllocations.get(Dstore));
                }

                //Waits to receive all REBALANCE_COMPLETE acknowledgements
                reBalanceCompletePauseLatch.await(timeout, TimeUnit.MILLISECONDS);
                synchronized (fileAllocations) {reBalanceCompletePauseLatch = new CountDownLatch(0);}
            }
        }
        catch (Exception e) {System.err.println("error: " + e);}

        //Ends the rebalance operation
        synchronized (askForReBalanceMonitor) {
            askForReBalance = false;
            askForReBalanceMonitor.notifyAll();
        }
    }

    /**
     * Handles a Dstore LIST response
     * @param Dstore endpoint of the connection with the Dstore
     * @param fileNames names of the files that the Dstore stores
     */
    public static void handleDstoreListResponse(Socket Dstore, List<String> fileNames) {
        synchronized (DstoresToFiles) {
            if (0 < filesPauseLatch.getCount() && !DstoresToFiles.containsKey(Dstore)) {
                DstoresToFiles.put(Dstore, fileNames);
                filesPauseLatch.countDown();
            }
        }
    }

    /**
     * Revises the file allocation of each Dstore
     */
    public static void reviseFileAllocation() {
        //Removes any files in the index that no Dstore stores
        for (String fileName : index.keySet()) {
            boolean containsFile = false;
            for (List<String> fileNames : DstoresToFiles.values()) {
                if (fileNames.contains(fileName)) {
                    containsFile = true;
                    break;
                }
            }
            if (!containsFile) index.remove(fileName);
        }

        //Creates indexes to map Dstores to the files that they should send or remove and what files they are sent
        //As well as to how many files they have after each send or removal
        Map<Socket, List<String>> filesToRemove = new HashMap<>();
        Map<Socket, Map<String, List<Integer>>> filesToSend = new HashMap<>();
        Map<Socket, List<String>> filesSent = new HashMap<>();
        Map<Socket, Integer> numberOfFiles = new HashMap<>();

        //Removes any files from a Dstore that is not on the index
        for (Socket Dstore : DstoresToFiles.keySet()) {
            filesToRemove.put(Dstore, new ArrayList<>());
            filesToSend.put(Dstore, new HashMap<>());
            filesSent.put(Dstore, new ArrayList<>());
            for (String fileName : DstoresToFiles.get(Dstore)) {
                if (!index.containsKey(fileName)) filesToRemove.get(Dstore).add(fileName);
            }
            DstoresToFiles.get(Dstore).removeAll(filesToRemove.get(Dstore));
            numberOfFiles.put(Dstore, DstoresToFiles.get(Dstore).size());
        }

        //Ensures that there are R replicas of each file
        for (String fileName : index.keySet()) {
            //Checks how many times a file is replicated
            int numberOfOccurrences = 0;
            for (List<String> fileList : DstoresToFiles.values()) {
                if (fileList.contains(fileName)) numberOfOccurrences++;
            }

            //Creates copies of the file if there aren't enough replicas
            if (numberOfOccurrences < R) {
                //Stores Dstore that contains the file
                Socket DstoreContainingFile = null;
                for (Socket Dstore : DstoresToFiles.keySet()) {
                    if (DstoresToFiles.get(Dstore).contains(fileName)) {
                        DstoreContainingFile = Dstore;
                        break;
                    }
                }
                //Copies file onto Dstore that doesn't contain the file
                for (Socket Dstore : DstoresToFiles.keySet()) {
                    if (!DstoresToFiles.get(Dstore).contains(fileName) && !filesSent.get(Dstore).contains(fileName)) {
                        if (filesToSend.get(DstoreContainingFile).containsKey(fileName)) {
                            filesToSend.get(DstoreContainingFile).get(fileName).add(DstoreSocketsToPorts.get(Dstore));
                        }
                        else {
                            List<Integer> portsToSendTo = new ArrayList<>();
                            portsToSendTo.add(DstoreSocketsToPorts.get(Dstore));
                            filesToSend.get(DstoreContainingFile).put(fileName, portsToSendTo);
                        }
                        filesSent.get(Dstore).add(fileName);
                        numberOfFiles.compute(Dstore, (key, val) -> val+1);
                        numberOfOccurrences++;
                    }
                    if (numberOfOccurrences == R) break;
                }
            }
            //Removes copies of the file if there are too many replicas
            else if (R < numberOfOccurrences) {
                for (Socket Dstore : DstoresToFiles.keySet()) {
                    if (DstoresToFiles.get(Dstore).contains(fileName)) {
                        filesToRemove.get(Dstore).add(fileName);
                        DstoresToFiles.get(Dstore).remove(fileName);
                        numberOfFiles.compute(Dstore, (key, val) -> val-1);
                        numberOfOccurrences--;
                    }
                    if (numberOfOccurrences == R) break;
                }
            }
        }

        //Calculates the minimum and maximum number of files that a Dstore can store
        int minFiles = Math.floorDiv(index.size() * R, DstoresToFiles.size());
        int maxFiles = Math.ceilDiv(index.size() * R, DstoresToFiles.size());

        //Re-allocates files if necessary
        for (Socket Dstore : DstoresToFiles.keySet()) {
            //Moves files from a Dstore with too many files to other Dstores
            if (maxFiles < numberOfFiles.get(Dstore)) {
                int currentFileIndex = 0;
                for (Socket toDstore : DstoresToFiles.keySet()) {
                    while (maxFiles < numberOfFiles.get(Dstore) && numberOfFiles.get(toDstore) < maxFiles && currentFileIndex < DstoresToFiles.get(Dstore).size()) {
                        String currentFile = DstoresToFiles.get(Dstore).get(currentFileIndex);
                        boolean differentDstore = !DstoreSocketsToPorts.get(Dstore).equals(DstoreSocketsToPorts.get(toDstore));
                        boolean notContainFile = !DstoresToFiles.get(toDstore).contains(currentFile) && !filesSent.get(toDstore).contains(currentFile);
                        if (differentDstore && notContainFile) {
                            if (filesToSend.get(Dstore).containsKey(currentFile)) {
                                filesToSend.get(Dstore).get(currentFile).add(DstoreSocketsToPorts.get(toDstore));
                            }
                            else {
                                List<Integer> portsToSendTo = new ArrayList<>();
                                portsToSendTo.add(DstoreSocketsToPorts.get(toDstore));
                                filesToSend.get(Dstore).put(currentFile, portsToSendTo);
                            }
                            filesToRemove.get(Dstore).add(currentFile);
                            filesSent.get(toDstore).add(currentFile);
                            DstoresToFiles.get(Dstore).remove(currentFile);
                            numberOfFiles.compute(Dstore, (key, val) -> val-1);
                            numberOfFiles.compute(toDstore, (key, val) -> val+1);
                        }
                        else currentFileIndex++;
                    }
                    if (numberOfFiles.get(Dstore) <= maxFiles) break;
                }
            }
            //Moves files to Dstores with not enough files from other Dstores
            else if (numberOfFiles.get(Dstore) < minFiles) {
                for (Socket fromDstore : DstoresToFiles.keySet()) {
                    int currentFileIndex = 0;
                    while (numberOfFiles.get(Dstore) < minFiles && minFiles < numberOfFiles.get(fromDstore) && currentFileIndex < DstoresToFiles.get(fromDstore).size()) {
                        String currentFile = DstoresToFiles.get(fromDstore).get(currentFileIndex);
                        boolean differentDstore = !DstoreSocketsToPorts.get(fromDstore).equals(DstoreSocketsToPorts.get(Dstore));
                        boolean notContainFile = !DstoresToFiles.get(Dstore).contains(currentFile) && !filesSent.get(Dstore).contains(currentFile);
                        if (differentDstore && notContainFile) {
                            if (filesToSend.get(Dstore).containsKey(currentFile)) {
                                filesToSend.get(fromDstore).get(currentFile).add(DstoreSocketsToPorts.get(Dstore));
                            }
                            else {
                                List<Integer> portsToSend = new ArrayList<>();
                                portsToSend.add(DstoreSocketsToPorts.get(Dstore));
                                filesToSend.get(fromDstore).put(currentFile, portsToSend);
                            }
                            filesToRemove.get(fromDstore).add(currentFile);
                            filesSent.get(Dstore).add(currentFile);
                            DstoresToFiles.get(fromDstore).remove(currentFile);
                            numberOfFiles.compute(fromDstore, (key, val) -> val-1);
                            numberOfFiles.compute(Dstore, (key, val) -> val+1);
                        }
                        else currentFileIndex++;
                    }
                    if (minFiles <= numberOfFiles.get(Dstore)) break;
                }
            }
        }

        //Creates a REBALANCE request for each Dstore using the re-allocations
        for (Socket Dstore : DstoresToFiles.keySet()) {
            StringBuilder reBalanceRequest = new StringBuilder();
            reBalanceRequest.append("REBALANCE ");

            //Creates files to send part of the REBALANCE request
            reBalanceRequest.append(filesToSend.get(Dstore).size());
            for (String file : filesToSend.get(Dstore).keySet()) {
                reBalanceRequest.append(" ").append(file).append(" ").append(filesToSend.get(Dstore).get(file).size());
                for (int port : filesToSend.get(Dstore).get(file)) reBalanceRequest.append(" ").append(port);
            }

            //Creates files to remove part of the REBALANCE request
            reBalanceRequest.append(" ").append(filesToRemove.get(Dstore).size());
            for (String file : filesToRemove.get(Dstore)) reBalanceRequest.append(" ").append(file);
            fileAllocations.put(Dstore, reBalanceRequest.toString());
        }


        //Stores the data needed to update the index
        Controller.filesToDelete = filesToRemove;
        Controller.filesToAdd = filesSent;
        Controller.fileNumbers = numberOfFiles;
    }

    /**
     * Handles a Dstore REBALANCE_COMPLETE acknowledgement
     * @param Dstore endpoint of the connection with the Dstore
     */
    public static void handleDstoreReBalanceACK(Socket Dstore) {
        synchronized (fileAllocations) {
            if (0 < reBalanceCompletePauseLatch.getCount() && fileAllocations.containsKey(Dstore)) {
                reBalanceCompletePauseLatch.countDown();

                //Updates the index
                for (String file : filesToDelete.get(Dstore)) {
                    if (index.containsKey(file)) index.get(file).removePort(DstoreSocketsToPorts.get(Dstore));
                }
                for (String file : filesToAdd.get(Dstore)) {
                    if (index.containsKey(file)) index.get(file).addPort(DstoreSocketsToPorts.get(Dstore));
                }
                DstorePortsToFileNumbers.put(DstoreSocketsToPorts.get(Dstore), fileNumbers.get(Dstore));
            }
        }
    }
}
