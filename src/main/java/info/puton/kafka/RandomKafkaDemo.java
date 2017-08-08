package info.puton.kafka;

public class RandomKafkaDemo {

    public static void main(String[] args) {
        String topic = "test";
        RandomKafkaProducer randomKafkaProducer = new RandomKafkaProducer(topic);
        randomKafkaProducer.start();

        ConsoleKafkaConsumer consoleKafkaConsumer = new ConsoleKafkaConsumer(topic);
        consoleKafkaConsumer.start();
    }

}
