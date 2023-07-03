import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleServerProgram {

   public static ServerThreadListener[] openThreads(int serversNumber, int basePort, int serverId) {
        ServerThreadListener[] arrayThreadListeners = new ServerThreadListener[serversNumber];
        try {
            for (int i=0; i<serversNumber;i++) {
                if (i != serverId) {
                    ServerSocket listener_ = new ServerSocket(basePort + i + 1);
                    arrayThreadListeners[i] = new ServerThreadListener(listener_);
                    arrayThreadListeners[i].start();
                }
            }         
        } catch (Exception e) {e.printStackTrace();};
        return arrayThreadListeners;
   }

   public static BufferedWriter[] openSockets(int serversNumber, int basePort, int serverId, HashMap<Integer,String> connexionMap) {

    BufferedWriter[] arrayOutputStreams = new BufferedWriter[serversNumber];
    try {
        Thread.sleep(500);
        for (int i=0; i<serversNumber;i++) {
            if (i != serverId) {
                Socket socket = new Socket(connexionMap.get(i), basePort + serverId + 1);
                arrayOutputStreams[i] = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            }
        }
    } catch (Exception e) {e.printStackTrace();};
    return arrayOutputStreams;
}

   public static void main(String args[]) {

       ServerSocket listener = null;
       String line;
       BufferedReader is;
       BufferedWriter os;
       Socket socketOfServer = null;

       ServerThreadListener[] arrayThreadListeners = null;
       BufferedWriter[] arrayOutputStreams = null;


       HashMap<Integer,String> connexionMap = new HashMap<Integer,String>();
       HashMap<Integer, List<String>> mapServerWords = new HashMap<Integer, List<String>>();
       HashMap<Integer, Integer> mapRangeServer = new HashMap<Integer, Integer>();
    
       HashMap<Integer,HashMap<String, Integer>> mapThreadsPreShuffle = new HashMap<Integer,HashMap<String, Integer>>();

       HashMap<String, Integer> mapPreShuffle = new HashMap<String, Integer>();

       HashMap<String, List<Integer>> mapShuffleWords = new HashMap<String, List<Integer>>();
       HashMap<Integer, List<String>> mapSecondShuffleWords = new HashMap<Integer, List<String>>();


       HashMap<Integer,List<String>> mapReduced = new HashMap<Integer,List<String>>();

       Integer basePort = Integer.parseInt(args[0]);

       Integer serverId = 0;
       Integer serversNumber = 0; // n



       // Try to open a server socket on port 9999
       // Note that we can't choose a port less than 1023 if we are not
       // privileged users (root)

 
       try {
            listener = new ServerSocket(basePort);
            // Handle the server connection here
            // Create a new thread or handle it asynchronously
       } catch (IOException e) {
           System.out.println(e);
           System.exit(1);
       }

       try {
           System.out.println("Server is waiting to accept user...");

           // Accept client connection request
           // Get new Socket at Server.    
           socketOfServer = listener.accept();
           
           System.out.println("Accept a client!");

           // Open input and output streams
           is = new BufferedReader(new InputStreamReader(socketOfServer.getInputStream()));
           os = new BufferedWriter(new OutputStreamWriter(socketOfServer.getOutputStream()));
           
           boolean connexionMessage = false;


           while (true) {
               // Read data to the server (sent from client).
                if (!connexionMessage) {
                    String connexionLine = is.readLine();
                    String[] hosts = connexionLine.split(",");
                    boolean idSet = false;
                    for (int i=0; i<hosts.length;i++){
                        String[] host = hosts[i].split(" ");
                        connexionMap.put(Integer.parseInt(host[0]), host[1]);
                        if ((i != Integer.parseInt(host[0])) && !(idSet)) {
                            serverId = i;
                            idSet = true;
                        } 
                    }
            
                    connexionMessage = true;
                    serversNumber = hosts.length + 1;
                    if (!idSet) {
                        serverId = serversNumber - 1;
                    }
                    arrayThreadListeners = openThreads(serversNumber, basePort, serverId);
                    arrayOutputStreams = openSockets(serversNumber, basePort, serverId,connexionMap);
                }
                line = is.readLine(); //split message

                // If users send QUIT (To end conversation).
                if (line.equals("QUIT")) {
                    
                    System.out.println("////////////");
                    System.out.println("Printing final local map");
                    System.out.println("////////////");

                    String str = InetAddress.getLocalHost().getHostName();
                    str += " :";
                    for (Map.Entry<Integer, List<String>> entry : mapSecondShuffleWords.entrySet()) {
                        str += ":" + entry.getKey() + " -> " + entry.getValue();
            
                    }
                    System.out.println(str);

                    os.write(">> OK");
                    os.newLine();
                    os.flush();

                    os.close();
                    is.close();
                    socketOfServer.close();

                    for (int i=0; i<serversNumber; i++) {
                        if (i != serverId) {
                            arrayOutputStreams[i].close();
                        }
                    }

                    for (int i=0; i<serversNumber; i++) {
                        if (i != serverId) {
                            arrayThreadListeners[i].endThread();
                        }
                    }


                    break;


                } else if (line.equals("PRESHUFFLE")) {
                    System.out.println("Received PRESHUFFLE");
                    for (Map.Entry<Integer, List<String>> entry : mapServerWords.entrySet()) {
                        
                        if (connexionMap.keySet().contains(entry.getKey())){
                            arrayOutputStreams[entry.getKey()].write("_PRESHUFFLE$");
                            for (String word : entry.getValue()) {
                                arrayOutputStreams[entry.getKey()].write(word + "$");
                            }
                            arrayOutputStreams[entry.getKey()].newLine();
                            arrayOutputStreams[entry.getKey()].flush();
                        
                        } else {
                            for (int i=0;i<entry.getValue().size();i++){
                                if (mapPreShuffle.containsKey(entry.getValue().get(i))){
                                    mapPreShuffle.put(entry.getValue().get(i),mapPreShuffle.get(entry.getValue().get(i))+1);
                                } else {
                                    mapPreShuffle.put(entry.getValue().get(i), 1);
                                }
                            }
                        }
                    }

                    os.write(">> PRESHUFFLE_ACK");
                    os.newLine();
                    os.flush();

                } else if (line.equals("WAITSHUFFLE")) {
                    int countReady = 0;
                    while (countReady != serversNumber-1) {
                        countReady = 0;
                        for (ServerThreadListener thread : arrayThreadListeners) {
                            if (thread != null) {
                            countReady += thread.getPreshuffleReady();
                        }
                        }
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {}
                    }

                    os.write(">> WAITSHUFFLE_ACK");
                    os.newLine();
                    os.flush();

                } else if (line.equals("SHUFFLE")) {

                    System.out.println("Received SHUFFLE");

                    int id = 0;
                    for (ServerThreadListener thread : arrayThreadListeners) {
                        if (thread != null) {
                            HashMap<String, Integer> threadMap = thread.getThreadResult();
                            mapThreadsPreShuffle.put(id, threadMap);
                        }
                        id += 1;
                    }

                    for (Map.Entry<String, Integer> entry : mapPreShuffle.entrySet()) {
                        if (mapShuffleWords.containsKey(entry.getKey())){
                            mapShuffleWords.get(entry.getKey()).add(entry.getValue());
                        } else {
                            List<Integer> list = new ArrayList<>();
                            list.add(entry.getValue());
                            mapShuffleWords.put(entry.getKey(), list);
                        }
                    }

                    for (Map.Entry<Integer, HashMap<String, Integer>> entry : mapThreadsPreShuffle.entrySet()) {
                        for (Map.Entry<String, Integer> entry2 : entry.getValue().entrySet()) {
                            if (mapShuffleWords.containsKey(entry2.getKey())){
                                mapShuffleWords.get(entry2.getKey()).add(entry2.getValue());
                            } else {
                                List<Integer> list = new ArrayList<>();
                                list.add(entry2.getValue());
                                mapShuffleWords.put(entry2.getKey(), list);
                            }
                        }
                    }

                    os.write(">> SHUFFLEK_ACK");
                    os.newLine();
                    os.flush();

                    System.out.println("Starting first reduce");
                    int min = Integer.MAX_VALUE;
                    int max = Integer.MIN_VALUE;

                    for (Map.Entry<String,List<Integer>> entry: mapShuffleWords.entrySet()){
                        Integer sum = 0;
                        for (int i=0; i<entry.getValue().size(); i++){
                            sum += entry.getValue().get(i);
                        }
                        if (sum > max){
                            max = sum;
                            if (min == Integer.MAX_VALUE) {
                                min = sum;
                            }
                        } else if (sum < min){
                            min = sum;
                        }

                        if (mapReduced.containsKey(sum)){
                            mapReduced.get(sum).add(entry.getKey());
                        } else {
                            List<String> list = new ArrayList<>();
                            list.add(entry.getKey());
                            mapReduced.put(sum, list);
                        }
                    }
                    if (min == Integer.MAX_VALUE) {
                        min = 0;
                        max = 0;
                    }

                    os.write(">> Range return " + min + " " + max);
                    os.newLine();
                    os.flush();

                } else if (line.startsWith("RANGES")) {
                    
                    String[] rangesRaw = line.split(" ");
                    for (int i=1;i<rangesRaw.length;i++){
                        String[] minmax = rangesRaw[i].split("\\$");
                        for (int _range = Integer.parseInt(minmax[0]); _range <= Integer.parseInt(minmax[1]) ; _range++){
                            if (_range != 0) {
                                mapRangeServer.put(_range,i-1);
                            }
                        }
                    }
                    
                    
                    System.out.println("Starting second preshuffle step");

                    for (Map.Entry<Integer, List<String>> entry : mapReduced.entrySet()) {

                        if (mapRangeServer.get(entry.getKey()) != serverId) {
                            arrayOutputStreams[mapRangeServer.get(entry.getKey())].write("SECOND_PRESHUFFLE$" + entry.getKey() + "$");
                            for (String word : entry.getValue()) {
                                arrayOutputStreams[mapRangeServer.get(entry.getKey())].write(word + "$");
                            }

                            arrayOutputStreams[mapRangeServer.get(entry.getKey())].newLine();
                            arrayOutputStreams[mapRangeServer.get(entry.getKey())].flush();

                        } else {
                            if (mapSecondShuffleWords.containsKey(entry.getKey())) {
                                for (String word : entry.getValue()) {
                                    mapSecondShuffleWords.get(entry.getKey()).add(word);
                                }
                            } else {
                                mapSecondShuffleWords.put(entry.getKey(), entry.getValue());
                            }
                        }

                    }

                    for (int i=0; i<serversNumber;i++) {
                        if (i != serverId) {
                            arrayOutputStreams[i].write("$END_S_PRESHUFFLE$");
                            arrayOutputStreams[i].newLine();
                            arrayOutputStreams[i].flush();
                        }
                    }
                    
                    os.write(">> SECONDPRESHUFFLE_ACK");
                    os.newLine();
                    os.flush();
                
                } else if (line.equals("SECONDSHUFFLE")) {

                    int countReady = 0;
                    while (countReady != serversNumber-1) {
                        countReady = 0;
                        for (ServerThreadListener thread : arrayThreadListeners) {
                            if (thread != null) {
                            countReady += thread.getSPreshuffleReady();
                        }
                        }
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {}
                    }

                    System.out.println("Received SECONDSHUFFLE");
                    for (ServerThreadListener thread : arrayThreadListeners) {
                        if (thread != null) {
                            HashMap<Integer, List<String>> secondThreadMap = thread.getSecondThreadResult();
                            for (Map.Entry<Integer, List<String>> entry : secondThreadMap.entrySet()) {
                                if (mapSecondShuffleWords.containsKey(entry.getKey())) {
                                    for (String word : entry.getValue()) {
                                        mapSecondShuffleWords.get(entry.getKey()).add(word);
                                    }
                                } else {
                                    mapSecondShuffleWords.put(entry.getKey(), entry.getValue());
                                }
                            }

                        }
                    }


                    os.write(">> SECONDSHUFFLEK_ACK");
                    os.newLine();
                    os.flush();

                }
                
                else {
                    createDict(mapServerWords, socketOfServer.getInputStream(), serversNumber, line);
                    os.write(">> DICT_ACK");
                    os.newLine();
                    os.flush();
                }
           }

       } catch (IOException e) {
           System.out.println(e);
           e.printStackTrace();
       }
       System.out.println("Server stopped!");
    
   }

   public static void createDict(HashMap<Integer, List<String>> mapServerWords, InputStream is, Integer serversNumber, String line) throws IOException {

            // Convert the file content bytes to a string
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192]; // Set the buffer size as per your requirement

            long remainingBytes = Long.parseLong(line);
            int bytesRead;
            while (remainingBytes > 0 && (bytesRead = is.read(buffer, 0, (int) Math.min(buffer.length, remainingBytes))) != -1) {
                outputStream.write(buffer, 0, bytesRead);
                remainingBytes -= bytesRead;
            }

            byte[] fileBytes = outputStream.toByteArray();
            outputStream.close();

            String fileContent = new String(fileBytes);
            String[] fileStrings = fileContent.split("\\s+");
            for (String word: fileStrings) {
                Integer server = Math.abs(word.hashCode())%serversNumber;
                if (mapServerWords.containsKey(server)) {
                    mapServerWords.get(server).add(word);
                } else {
                    List<String> list = new ArrayList<>();
                    list.add(word);
                    mapServerWords.put(server, list);
                }
            }
            System.out.println("File received and dict built successfully.");
            

   }
}