package sistemaFacturacionAppCompras.appcompras;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class AppcomprasApplication {    

		
	private final static String TOPIC = "compra";
    private final static String BOOTSTRAP_SERVERS ="localhost:3032";
    private final static String GROUP ="compras";
    
    private static Consumer<Long, String> createConsumerCompras() {
        final Properties propiedades = new Properties();
        propiedades.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        propiedades.put(ConsumerConfig.GROUP_ID_CONFIG,GROUP);
        
        propiedades.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,LongDeserializer.class.getName());
        propiedades.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<>(propiedades);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }
    
    static void executeConsumer() throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumerCompras();

        final int giveUp = 100;   int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
 
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                System.out.println("Compra *** Key: " + record.key() + " Valor: " + record.value() + 
                					" Particion: " + record.partition() + " Topic: " + record.topic());
            });

            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("COMPRA REALIZADA");
    }
    
    
	public static void main(String[] args) throws InterruptedException {
		executeConsumer();
	    
	}
}
