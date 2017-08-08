package info.puton.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class RandomKafkaProducer extends Thread {

    private String topic;

    public RandomKafkaProducer(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public void run() {

        Properties properties = new Properties();
        InputStream is = null;
        try {
            is = ClassLoader.getSystemResourceAsStream("producer.properties");
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

        Producer producer = new KafkaProducer(properties);

        while (true){
            producer.send(new ProducerRecord(topic,"mykey","myvalue"));
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
