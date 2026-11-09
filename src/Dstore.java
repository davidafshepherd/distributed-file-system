import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Class used to handle communication and requests sent to a Dstore.
 */
public class Dstore {

    // Dstore configuration and Controller connection.
    final int timeout;
    private final File fileFolder;
    private Socket controller;

    // Connected Dstores and rebalancing synchronisation.
    private final List<Socket> Dstores = new ArrayList<>();
    private CountDownLatch reBalancePauseLatch = new CountDownLatch(0);

    /**
     * Dstore's main method.
     * @param args arguments passed into the Dstore.
     */
    public static void main(String[] args) {
        // Check if the arguments given to this main method are in a valid format.
        if (!checkArguments(args)) System.err.println("error: " + new IllegalArgumentException());

        // Initialise a Dstore.
        new Dstore(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]);
    }

    /**
     * Dstore's constructor.
     * @param port port that the Dstore listens on.
     * @param controllerPort port for the Dstore to use to communicate with the Controller.
     * @param timeout how long the Dstore should wait for a response.
     * @param fileFolder the folder that the Dstore should store files in.
     */
    public Dstore(int port, int controllerPort, int timeout, String fileFolder) {
        // Store  how long the Dstore should wait for a response in milliseconds.
        this.timeout = timeout;

        // Create a File object that represents the file folder.
        this.fileFolder = new File(fileFolder);
        clearFileFolder(this.fileFolder);

        // Set the Dstore's socket to null.
        this.controller = null;

        // Set the Dstore's server socket to null.
        ServerSocket serverSocket = null;


        // Handle the connection established between the Dstore and the Controller.
        try {
            // Initialise the Dstore's socket.
            InetAddress address = InetAddress.getLocalHost();
            this.controller = new Socket(address, controllerPort);

            // Create a thread to handle the connection established between the Dstore and the Controller.
            new Thread(new DstoreClientThread(this, this.controller, port)).start();
        }
        catch(Exception e) {System.err.println("error: " + e);}

        // Handle each connection established between the Dstore and a client.
        try {
            // Initialise the Dstore's server socket.
            serverSocket = new ServerSocket(port);

            // Creates a thread for each connection established between the Dstore and a client.
            while (true) {
                try {
                    Socket client = serverSocket.accept();
                    new Thread(new DstoreServerThread(this, client)).start();
                }
                catch(Exception e) { System.err.println("error: " + e); }
            }
        }
        catch (IOException e) {System.err.println("error: " + e);}
        finally {
            // Close the connection if it's still open.
            if (serverSocket != null) {
                try { serverSocket.close(); }
                catch (IOException e) { System.err.println("error: " + e); }
            }
        }
    }

    /**
     * Checks if the arguments given to the main method are in a valid format.
     * @param args arguments given to the main method.
     * @return whether the arguments given to the main method are in a valid format.
     */
    public static boolean checkArguments(String[] args) {
        if (args.length != 4) return false;
        try {
            Integer.parseInt(args[0]);
            Integer.parseInt(args[1]);
            Integer.parseInt(args[2]);
            return true;
        } catch (NumberFormatException e) {return false;}
    }

    /**
     * Clears the file folder.
     */
    public static void clearFileFolder(File folder) {
        // Get a list of the files and subdirectories within the file folder.
        File[] files = folder.listFiles();
        if (files != null) {
            // Iterate over each file or subdirectory.
            for (File file : files) {
                //If a directory, recursively clears it.
                if (file.isDirectory()) {
                    clearFileFolder(file);
                }
                // Delete the file or empty subdirectory.
                file.delete();
            }
        }
    }

    /**
     * Checks if a request is a valid STORE request sent by a client.
     * @param request request received by Dstore.
     * @return whether the request is a valid STORE request sent by a client or not.
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
     * Checks if a request is a valid LOAD_DATA request sent by a client.
     * @param request request received by Dstore.
     * @return whether the request is a valid LOAD_DATA request sent by a client or not.
     */
    public static boolean checkIfClientLoadDataRequest(String[] request) {
        if (request.length != 2) return false;
        return request[0].equals("LOAD_DATA");
    }

    /**
     * Checks if a request is a valid REMOVE request sent by the Controller.
     * @param request request received by Dstore.
     * @return whether the request is a valid REMOVE request sent by the Controller or not.
     */
    public static boolean checkIfControllerRemoveRequest(String [] request) {
        if (request.length != 2) return false;
        return request[0].equals("REMOVE");
    }

    /**
     * Checks if a request is a valid LIST request sent by the Controller.
     * @param request request received by the Dstore.
     * @return whether the request is a valid LIST request sent by the Controller or not.
     */
    public static boolean checkIfControllerListRequest(String [] request) {
        if (request.length != 1) return false;
        return request[0].equals("LIST");
    }

    /**
     * Checks if a request is a valid REBALANCE request sent by the Controller.
     * @param request request received by the Dstore.
     * @return whether the request is a valid REBALANCE request sent by the Controller or not.
     */
    public static boolean checkIfControllerReBalanceRequest(String [] request) {
        if (request.length < 3) return false;
        return request[0].equals("REBALANCE");
    }

    /**
     * Checks if a request is a valid REBALANCE_STORE request sent by a Dstore.
     * @param request request received by Dstore.
     * @return whether the request is a valid REBALANCE_STORE request sent by a Dstore or not.
     */
    public static boolean checkIfDstoreStoreRequest(String[] request) {
        if (request.length != 3) return false;
        if (!request[0].equals("REBALANCE_STORE")) return false;
        try {
            Integer.parseInt(request[2]);
            return true;
        } catch (NumberFormatException e) {return false;}
    }

    /**
     * Handles a client STORE request.
     * @param client endpoint of the connection with the client.
     * @param out object used to send a response to the client.
     * @param fileName name of file that the client is asking to store.
     * @param fileSize size of the file that the client is asking to store.
     */
    public void handleClientStoreRequest(Socket client, PrintWriter out, String fileName, int fileSize) {
        // Send an acknowledgement to the client that the Dstore has received its store request.
        out.println("ACK");

        // Create a new file within the file folder to store the file content that is about to be sent by the client.
        File file = new File(this.fileFolder, fileName);

        // Wait for the file content to be received. If not received in time, the file is removed from the file folder.
        try (FileOutputStream fos = new FileOutputStream(file)) {
            // Store the file content.
            client.setSoTimeout(this.timeout);
            byte[] fileContent = client.getInputStream().readNBytes(fileSize);
            client.setSoTimeout(0);

            // Write the file content to the file.
            fos.write(fileContent);

            // Send an ACK to the controller.
            PrintWriter controllerOut = new PrintWriter(this.controller.getOutputStream(), true);
            controllerOut.println("STORE_ACK " + fileName);
        }
        catch (Exception e) {
            System.err.println("error: " + e);
            file.delete();
        }
    }

    /**
     * Handles a client LOAD_DATA request.
     * @param client endpoint of the connection with the client.
     * @param fileName name of file that the client is asking to load.
     */
    public void handleClientLoadDataRequest(Socket client, String fileName) {
        // Store the file that we are about to send.
        File file = new File(this.fileFolder, fileName);

        // Send the file content to the client.
        try (FileInputStream fis = new FileInputStream(file)) {
            // Store the file content.
            byte[] fileContent = fis.readAllBytes();

            // Send the file content.
            OutputStream outputStream = client.getOutputStream();
            outputStream.write(fileContent);
        }
        catch (Exception e1) {
            System.err.println("error: " + e1);
            try {client.close();}
            catch (Exception e2) {System.err.println("error: " + e2);}
        }
    }

    /**
     * Handles a Controller REMOVE request.
     * @param controller endpoint of the connection with the Controller.
     * @param out object used to send a response to the Controller.
     * @param fileName name of file that the Controller is asking to remove.
     */
    public void handleControllerRemoveRequest(Socket controller, PrintWriter out, String fileName) {
        // Store the file that we are about to send.
        File file = new File(this.fileFolder, fileName);

        // Check if the file exists.
        if (!file.exists()) {out.println("ERROR_FILE_DOES_NOT_EXIST " + fileName);}

        // Delete the file.
        if (file.delete()) {out.println("REMOVE_ACK " + fileName);}
    }

    /**
     * Handles a Controller LIST request.
     * @param controller endpoint of the connection with the Controller.
     * @param out object used to send a response to the Controller.
     */
    public void handleControllerListRequest(Socket controller, PrintWriter out) {
        // Store the names of the files stored within the file folder.
        String[] fileNames = this.fileFolder.list();

        // Send the names of the files to the Controller.
        if (fileNames == null || fileNames.length == 0) {out.println("LIST");}
        else {
            StringBuilder string = new StringBuilder();
            for (String file : fileNames) string.append(" ").append(file);
            out.println("LIST" + string);
        }
    }

    /**
     * Handles a Controller REBALANCE request.
     * @param controller endpoint of the connection with the Controller.
     * @param out object used to send a response to the Controller.
     * @param request rebalance request sent by the controller.
     */
    public void handleControllerReBalanceRequest(Socket controller, PrintWriter out, List<String> request) {
        try {
            // Index of files to the ports that this Dstore has to send them to.
            Map<Integer, List<String>> filesToSend = new HashMap<>();

            // Store the send requests.
            int numberOfFilesToSend = Integer.parseInt(request.getFirst());
            request.removeFirst();
            for (int i = 0; i < numberOfFilesToSend; i++) {
                String file = request.getFirst();
                request.removeFirst();
                int numberOfPortsToSendTo = Integer.parseInt(request.getFirst());
                request.removeFirst();
                for (int j = 0; j < numberOfPortsToSendTo; j++) {
                    int port = Integer.parseInt(request.getFirst());
                    if (filesToSend.containsKey(port)) {
                        filesToSend.get(port).add(file);
                    } else {
                        List<String> files = new ArrayList<>();
                        files.add(file);
                        filesToSend.put(port, files);
                    }
                    request.removeFirst();
                }
            }

            // Index of files to the ports that this Dstore has to send them to.
            List<String> filesToRemove = new ArrayList<>();

            // Store the remove requests.
            int numberOfFilesToRemove = Integer.parseInt(request.getFirst());
            request.removeFirst();
            for (int i = 0; i < numberOfFilesToRemove; i++) {
                String file = request.getFirst();
                filesToRemove.add(file);
                request.removeFirst();
            }

            // Carry out the send requests.
            boolean reBalanceComplete;
            synchronized (Dstores) {
                Dstores.clear();
                reBalancePauseLatch = new CountDownLatch(filesToSend.keySet().size());
                for (int port : filesToSend.keySet()) {
                    try {
                        // Initialise the Dstore's socket.
                        InetAddress address = InetAddress.getLocalHost();
                        Socket Dstore = new Socket(address, port);
                        Dstores.add(Dstore);

                        // Store the size of the files that it needs to send.
                        Map<String, Integer> fileNamesToSizes = new HashMap<>();
                        for (String fileName : filesToSend.get(port)) {
                            File file = new File(this.fileFolder, fileName);
                            if (file.exists()) fileNamesToSizes.put(fileName, (int) file.length());
                            else throw new IOException();
                        }

                        // Create a thread to handle the connection established between the Dstore and the Dstore.
                        new Thread(new DstoreRebalanceThread(this, Dstore, fileNamesToSizes)).start();
                    } catch (Exception e) {
                        System.err.println("error: " + e);
                    }
                }

                // Wait for all send requests to be carried out.
                reBalanceComplete = reBalancePauseLatch.await(timeout, TimeUnit.MILLISECONDS);
                reBalancePauseLatch = new CountDownLatch(0);
            }

            // Close any connections still opened.
            for (Socket Dstore : Dstores) {
                if (Dstore != null) {
                    try {Dstore.close();}
                    catch (IOException e) {System.err.println("error: " + e);}
                }
            }

            // Carry out the remove requests.
            for (String fileName : filesToRemove) {
                File file = new File(this.fileFolder, fileName);
                if (!file.delete()) reBalanceComplete = false;
            }

            // Tell the controller the rebalance operation was complete.
            if (reBalanceComplete)  out.println("REBALANCE_COMPLETE");
        }
        catch (Exception ignored){}
    }

    /**
     * Handles a thread sending all the files that it was meant to send a Dstore.
     * @param Dstore endpoint of the connection with the Dstore.
     */
    public void handleThreadAllFilesSent(Socket Dstore) {
        if (0 < reBalancePauseLatch.getCount() && Dstores.contains(Dstore)) reBalancePauseLatch.countDown();
    }
}
