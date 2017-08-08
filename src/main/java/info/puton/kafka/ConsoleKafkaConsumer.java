package info.puton.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ConsoleKafkaConsumer extends Thread {

    private String topic;

    public ConsoleKafkaConsumer(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public void run() {
        Properties properties = new Properties();
        InputStream is = null;
        try {
            is = ClassLoader.getSystemResourceAsStream("consumer.properties");
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Consumer consumer = new KafkaConsumer(properties);
        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        consumer.subscribe(topics);

        while (true){
            ConsumerRecords<String,String> crs = consumer.poll(200);
            for (ConsumerRecord<String,String> cr : crs) {
                System.out.println("Key: "+cr.key()+" Value: "+cr.value());
            }
        }

    }
}
