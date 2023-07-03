import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleClient {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        String hosts[];
        int port = 0;
        try {
            hosts = new String[args.length-1];
            port = Integer.parseInt(args[0]);
            for (int i=1; i<args.length; i++){
                hosts[i-1] = args[i];
            }
        } catch (Exception e) {
            System.out.println("Error with arguments:");
            System.out.println("Entry: port serverHost0 serverHost1 ...");
            System.out.println("Example: 9976 tp-1d22-10.enst.fr tp-1d22-09.enst.fr tp-1d22-08.enst.fr");
            e.printStackTrace();
            return;
        }

        HashMap<Integer, String> connexionDict = new HashMap<>();
        for (int i = 0; i < hosts.length; i++) {
            connexionDict.put(i, hosts[i]);
        }

        Integer rangeReceived = 0;
        List<Integer> mins = new ArrayList<>();
        List<Integer> maxs = new ArrayList<>();

        Socket[] socketsOfClient = new Socket[hosts.length];
        OutputStream[] osArray = new OutputStream[hosts.length];
        BufferedWriter[] osS = new BufferedWriter[hosts.length];
        BufferedReader[] isS = new BufferedReader[hosts.length];

        try {
            for (int i=0; i<hosts.length; i++) {
                socketsOfClient[i] = new Socket(hosts[i], port);
                osArray[i] = socketsOfClient[i].getOutputStream();
                osS[i] = new BufferedWriter(new OutputStreamWriter(socketsOfClient[i].getOutputStream()));
                isS[i] = new BufferedReader(new InputStreamReader(socketsOfClient[i].getInputStream()));
            }

        } catch (UnknownHostException e) {
            System.err.println("Don't know about host");
            return;
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection");
            e.printStackTrace();
            return;
        }

        try {

            for (int j = 0; j < hosts.length; j++) {
                String connexionMessage = "";

                for (Map.Entry<Integer, String> entry : connexionDict.entrySet()) {
                    if (j == entry.getKey()) {
                        continue;
                    } else {
                        connexionMessage += entry.getKey();
                        connexionMessage += " ";
                        connexionMessage += entry.getValue();
                        if ((entry.getKey() != hosts.length-1) && !((entry.getKey() == hosts.length-2) && (j == hosts.length-1))) {
                            connexionMessage += ",";
                        }
                    }
                }
                osS[j].write(connexionMessage);
                osS[j].newLine();;
                osS[j].flush();
            }



            String sourceFilePath = "../data/CC-MAIN-20230320211022-20230321001022-00511.warc.wet";

            int numMachines = hosts.length;
            System.out.println("Started sending files");
            try (BufferedReader reader = new BufferedReader(new FileReader(sourceFilePath))) {
                long fileSize = new File(sourceFilePath).length();
                long chunkSize = fileSize / numMachines;

                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                String line;
                int bytesRead;
                for (int i = 0; i < numMachines; i++) {
                    boolean sent = false;
                    long remainingBytes = chunkSize;
                    int totalBytesRead = 0;
                    while (remainingBytes > 0) {
                        line = reader.readLine();
                        if (line == null) {
                            break;
                        }
                        if (line.trim().isEmpty()) {
                            continue;
                        }

                        byte[] lineBytes = line.getBytes();

                        bytesRead = lineBytes.length;
                        if (bytesRead >= remainingBytes) {
                            totalBytesRead += (lineBytes.length + System.lineSeparator().getBytes().length);
                            osS[i].write(Integer.toString(totalBytesRead));
                            osS[i].newLine();
                            osS[i].flush();

                            buffer.write(lineBytes);
                            buffer.write(System.lineSeparator().getBytes());

                            osArray[i].write(buffer.toByteArray());
                            osArray[i].flush();

                            sent = true;
                            buffer.reset();
                            remainingBytes = 0;
                        } else {
                            buffer.write(lineBytes);
                            buffer.write(System.lineSeparator().getBytes());
                            remainingBytes -= bytesRead + System.lineSeparator().getBytes().length;
                            totalBytesRead += (lineBytes.length + System.lineSeparator().getBytes().length);
                        }
                    }
                    if (!sent) {
                        osS[i].write(Integer.toString(totalBytesRead));
                        osS[i].newLine();
                        osS[i].flush();

                        osArray[i].write(buffer.toByteArray());
                        buffer.reset();
                        osArray[i].flush();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("File sending completed successfully.");

            // Read data sent from the server.
            // By reading the input stream of the Client Socket.

            String[] responseLines = new String[hosts.length];


            while (true) {
                boolean allResponsesOK = true;
                boolean allResponsesDictACK = true;
                boolean allResponsesWSACK = true;
                boolean allResponsesPSACK = true;
                boolean allResponsesSACK = true;
                boolean allResponsesRange = true;
                boolean allResponsesSPSACK = true;
                boolean allResponsesSSACK = true;

                for (int m = hosts.length-1; m >= 0; m--) {
                    responseLines[m] = isS[m].readLine();                
                    if (!responseLines[m].equals(">> OK")) {
                        allResponsesOK = false;
                    } 
                    if (!responseLines[m].equals(">> DICT_ACK")) {
                        allResponsesDictACK = false;
                    } 
                    if (!responseLines[m].equals(">> PRESHUFFLE_ACK")) {
                        allResponsesPSACK = false;
                    } 
                    if (!responseLines[m].equals(">> WAITSHUFFLE_ACK")) {
                        allResponsesWSACK = false;
                    } 
                    if (!responseLines[m].equals(">> SHUFFLEK_ACK")) {
                        allResponsesSACK = false;
                    } 
                    if (!responseLines[m].startsWith(">> Range")) {
                        allResponsesRange = false;
                    } 
                    if (!responseLines[m].equals(">> SECONDPRESHUFFLE_ACK")) {
                        allResponsesSPSACK = false;
                    } 
                    if (!responseLines[m].equals(">> SECONDSHUFFLEK_ACK")) {
                        allResponsesSSACK = false;
                    }
                }

                for (int m=0; m<hosts.length; m++) {
                    System.out.println("Server: " + responseLines[m]);
                }

                if (allResponsesOK) {
                    long endTime2   = System.currentTimeMillis();
                    long totalTime = endTime2 - startTime;
			        System.out.println("Time for building + print: " + totalTime + " ms");
                    break;

                } else if (allResponsesDictACK) {

                    System.out.println("Received Dict acks");

                    for (int i=0; i<hosts.length; i++){
                        osS[i].write("PRESHUFFLE");
                        osS[i].newLine();
                        osS[i].flush();
                    }

                } else if (allResponsesPSACK) {
                    System.out.println("Received preshuffles");

                    for (int i=0; i<hosts.length; i++){
                        osS[i].write("WAITSHUFFLE");
                        osS[i].newLine();
                        osS[i].flush();
                    
                    }
                } else if (allResponsesWSACK) {
                    System.out.println("Received wait shuffles");

                    for (int i=0; i<hosts.length; i++){
                        osS[i].write("SHUFFLE");
                        osS[i].newLine();
                        osS[i].flush();
                    }

                } else if (allResponsesSACK) {
                    System.out.println("Received shuffle");

                } else if (allResponsesRange) {
                    for (int m=0; m<hosts.length; m++) {
                        if (Integer.parseInt(responseLines[m].split(" ")[3]) != 0) {
                            mins.add(Integer.parseInt(responseLines[m].split(" ")[3]));
                        }
                        maxs.add(Integer.parseInt(responseLines[m].split(" ")[4]));
                        rangeReceived += 1;
                    }

                    if (rangeReceived == hosts.length) {
                        Integer gmin = Collections.min(mins);
                        Integer gmax = Collections.max(maxs);
                        Integer range = (int) Math.floor((gmax - gmin) / hosts.length);
                        System.out.println("range calculed: " + range);
                        String ranges = "RANGES";

                        if (range == 0) {
                            boolean limit = false;
                            for (int i = 0; i < rangeReceived; i++) {
                                if (!limit) {
                                    if ((gmin + i) >= gmax) {
                                        ranges += " " + gmax + "$" + gmax;
                                        limit = true;
                                    } else {
                                        ranges += " " + (gmin + i) + "$" + (gmin + i);
                                    }
                                } else {
                                    ranges += " 0$0";
                                }
                            }
                        } else {
                            for (int i = 0; i < rangeReceived; i++) {
                                String init = " " + (gmin + (i * range) + i);
                                ranges += init;
                                if (i == (rangeReceived - 1)) {
                                    ranges += "$" + gmax;
                                } else {
                                    ranges += "$" + (gmin + ((i + 1) * range) + i);
                                }
                            }
                        }

                        System.out.println("Sending ranges: " + ranges);

                        for (int i=0; i<hosts.length; i++){
                        osS[i].write(ranges);
                        osS[i].newLine();
                        osS[i].flush();
                    }

                    }

                } else if (allResponsesSPSACK) {
                    System.out.println("Received SECOND preshuffles");

                    for (int i=0; i<hosts.length; i++){
                        osS[i].write("SECONDSHUFFLE");
                        osS[i].newLine();
                        osS[i].flush();
                    }

                } else if (allResponsesSSACK) {
                    System.out.println("Received SECONDshuffle");

                    for (int i=0; i<hosts.length; i++){
                        osS[i].write("QUIT");
                        osS[i].newLine();
                        osS[i].flush();
                    }
                    long endTime   = System.currentTimeMillis();
                    long totalTime = endTime - startTime;
			        System.out.println("Time for building results: " + totalTime + " ms");
                }

            }

            for (int i=0; i<hosts.length; i++){
                        osS[i].close();
                        osArray[i].close();
                        isS[i].close();
                        socketsOfClient[i].close();
                        osS[i].newLine();
                        osS[i].flush();
                    }
        } catch (UnknownHostException e) {
            System.err.println("Trying to connect to unknown host: " + e);
        } catch (IOException e) {
            System.err.println("IOException:  " + e);
        }
    }
}