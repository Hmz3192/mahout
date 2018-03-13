package recom.realtime;

/**
 * Created by dylan
 */

import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import recom.common.Constants;
import recom.common.RedisUtil;
import redis.clients.jedis.Jedis;

public class KafkaProducer{
  private static final Logger LOGGER = Logger.getLogger(KafkaProducer.class);
  private static Jedis jedis = RedisUtil.getJedis();

  /*static NewClickEvent[] newClickEvents = new NewClickEvent[]{
      new NewClickEvent(1000000L, 123L),
      new NewClickEvent(1000001L, 111L),
      new NewClickEvent(1000002L, 500L),
      new NewClickEvent(1000003L, 278L),
      new NewClickEvent(1000004L, 681L),
  };*/

  static Runnable runnable = new Runnable() {
    @Override
    public void run() {
      String pre_str = "user:";
//        String s = jedis.get("user:1");
      Set<String> set = jedis.keys(pre_str +"*");
      Iterator<String> it = set.iterator();
      System.out.println("-");
      List<NewClickEvent> newClickEvents = new ArrayList<>();
      while(it.hasNext()){
        String keyStr = it.next();
        String value = jedis.get(keyStr);
        Long keyL = Long.valueOf(Integer.valueOf(keyStr.split(":")[1]));
        Long vL = Long.valueOf(Integer.valueOf(value));
        System.out.println(keyL + "|" + vL);
        newClickEvents.add(new NewClickEvent(keyL, vL));
      }
//      send(Constants.KAFKA_TOPICS,newClickEvents);
//        new KafkaProducer(Constants.KAFKA_TOPICS,newClickEvents);
    }
  };

  public static void send(String kafkaTopics, List<NewClickEvent> newClickEvents) {
    Properties props = new Properties();
    props.put("metadata.broker.list", Constants.KAFKA_ADDR);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("producer.type", "async");
    ProducerConfig conf = new ProducerConfig(props);
    Producer<Integer, String> producer = null;
    try {
      System.out.println("Producing messages");
      producer = new Producer<>(conf);
      for (NewClickEvent event : newClickEvents) {
        String eventAsStr = JSON.toJSONString(event);
        producer.send(new KeyedMessage<Integer, String>(
                kafkaTopics, eventAsStr));
        System.out.println("Sending messages:" + eventAsStr);

      }
      System.out.println("Done sending messages");
    } catch (Exception ex) {
      LOGGER.fatal("Error while producing messages", ex);
      LOGGER.trace(null, ex);
      System.err.println("Error while producing messages：" + ex);
    } finally {
      if (producer != null) producer.close();
    }
  }

  public static void main(String[] args) throws Exception {
    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(runnable, 0, 30, TimeUnit.SECONDS);
  }
}
