package wikiedits;

import wikiedits.io.StreamExample;

public class ActiveMQAnalysis {



    public static void main(String[] args) throws Exception {
// start Flink in the background
        Thread flink = new Thread(() -> {
            try {
                StreamExample.startFlinkStream();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        flink.start();
        flink.join();

    }


}
