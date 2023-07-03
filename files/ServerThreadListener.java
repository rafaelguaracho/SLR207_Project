import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ServerThreadListener extends Thread {
    private ServerSocket listener_;
    private HashMap<String, Integer> mapThread = new HashMap<String, Integer>();
    private HashMap<Integer, List<String>> secondMapThread = new HashMap<Integer, List<String>>();
    private int preshuffle_r = 1;
    private int s_preshuffle_r = 0;

    public ServerThreadListener(ServerSocket listener_) {
        this.listener_ = listener_;
    }

    public void run() {
        try  {
            Socket socketOfServer = listener_.accept();
            BufferedReader is = new BufferedReader(new InputStreamReader(socketOfServer.getInputStream()));

            while (true) {
                String line_ = is.readLine();
                if (line_.startsWith("_PRESHUFFLE$")) {
                    preshuffle_r = 0;
                    String[] words = line_.split("\\$");
                    for (int i=1; i<words.length; i++){
                        if (mapThread.containsKey(words[i])){
                            mapThread.put(words[i],mapThread.get(words[i])+1);
                        } else {
                            mapThread.put(words[i], 1);
                        }
                    }
                    preshuffle_r = 1;
                } else if (line_.startsWith("SECOND_PRESHUFFLE$")) {
                    s_preshuffle_r = 0;
                    String[] words = line_.split("\\$");
                    int range = Integer.parseInt(words[1]);
                    for (int i=2; i<words.length; i++){
                        if (secondMapThread.containsKey(range)){
                            secondMapThread.get(range).add(words[i]);
                        } else {
                            List<String> list = new ArrayList<>();
                            list.add(words[i]);
                            secondMapThread.put(range, list);
                        }
                    }

                } else if (line_.equals("$END_S_PRESHUFFLE$")){
                    s_preshuffle_r = 1;
                    break;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        
    }

    public HashMap<String, Integer> getThreadResult() {
        return mapThread;
    }

    public HashMap<Integer, List<String>> getSecondThreadResult() {
        return secondMapThread;
    }

    public int getPreshuffleReady() {
        return preshuffle_r;
    }

    public int getSPreshuffleReady() {
        return s_preshuffle_r;
    }

    public void endThread() {
        try {
            listener_.close();
            interrupt();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
    
}
