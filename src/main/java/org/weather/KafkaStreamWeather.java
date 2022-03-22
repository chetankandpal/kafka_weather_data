package org.weather;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;


import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.streams.kstream.Grouped.with;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

public class KafkaStreamWeather {

    public static final String INPUT_TOPIC="weather-tmp-input";
    public static final String OUTPUT_TOPIC="weather-tmp-output";


    static void createAvgTempCalcStream(final StreamsBuilder builder){

        KStream<String, String> inputStream =builder.stream(INPUT_TOPIC);

        WindowedSerializer<String> windowedSerializer = new TimeWindowedSerializer<String>(new StringSerializer());
        Deserializer<Windowed<String>> windowedDeserializer = new TimeWindowedDeserializer<String>(new StringDeserializer());
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);



        KStream<String,Double> dayTempStream=inputStream.map((s,line) -> {

            try {
                Weather w = mapLineToWeather(line);
                return new KeyValue<>(w.day, w.temperature);
            } catch (Exception e) {
                System.err.println("Mapping error:" + e.getMessage());
                throw e;
            }

        });

    dayTempStream
            .groupByKey(with(Serdes.String(),Serdes.Double()))
            .windowedBy(TimeWindows.of(Duration.ofSeconds(2)))
            .aggregate(TempCalculator::new ,(day,temperature,tempCalculator) -> {

                try {

                    tempCalculator.incrementCounter();
                    tempCalculator.addSum(temperature);
                    return tempCalculator;
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                    throw e;
                }
            }, Materialized.with(Serdes.String(),CustomSerdes.instance()))
            .toStream()
            .peek((key, value) -> System.out.println("Outgoing record - key " +key +" value " + value))
            .mapValues((day,tempCalculator) -> tempCalculator.getAvg())
            .to(OUTPUT_TOPIC,Produced.with(windowedSerde,Serdes.Double()));

    }


    public static Weather mapLineToWeather(String csvLine){

        String[] csvfields=csvLine.split(",");
      // System.out.println(csvfields[3]);
        String date=csvfields[0];
        Double temp=Double.parseDouble(csvfields[3]);
        return new Weather(date,temp);
    }


    public static void main(final String[] args){

        //Configuring kafka stream
        Properties p=new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG,"weather");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,TimeWindowedDeserializer.class.getName());

        StreamsBuilder builder=new StreamsBuilder();

        createAvgTempCalcStream(builder);

        KafkaStreams streams=new KafkaStreams(builder.build(),p);

        final CountDownLatch latch =new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook"){

            @Override
            public void run(){

                System.out.println("Shutdown streams...");
                streams.close();
                latch.countDown();
            }
        });

            try

            {
                System.out.println("Streams started ...");
                streams.start();
                System.out.println("Waiting for events...");
                latch.await();
            }catch(final Throwable e){
                System.err.println(e.getMessage());
                System.exit(1);
            }
           System.exit(0);

    }


    static class TempCalculator{
        Integer count;
        Double sum;
        Integer min=-1000;

        public TempCalculator(){
            count=0;
            sum=0.0;
        }

        public Double getSum(){
            return sum;
        }
        public Integer getCount(){
            return count;
        }

        @JsonIgnore
        public double getAvg(){

            if(count !=0)
                return sum/count;
            else{
                System.err.println("No day record found");
                return sum;
            }
        }


        @JsonIgnore
        public void incrementCounter(){
            ++this.count;
        }
        @JsonIgnore
        public void addSum(Double sum){
            this.sum +=sum;
        }

    }

    }

//JsonSerializer-- Generic Serializer for sending Java objects to Kafka as JSON
    class JsonSerializer<T> implements Serializer<T>{

        private final ObjectMapper objectMapper=new ObjectMapper();
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            Serializer.super.configure(configs, isKey);
        }

        @Override
        public byte[] serialize(String s, T data) {

            if(data==null)
                return null;


            try{
                return objectMapper.writeValueAsBytes(data);
            }catch(Exception e){
                throw new SerializationException("Error serializing JSON message",e);
            }
        }

        @Override
        public byte[] serialize(String topic, Headers headers, T data) {
            return Serializer.super.serialize(topic, headers, data);
        }

        @Override
        public void close() {
            Serializer.super.close();
        }
    }

//Deserializer-- An interface for converting bytes to objects
    class JsonDeserializer<T> implements Deserializer<T>{

    private final ObjectMapper objectMapper=new ObjectMapper();
    private Class<T> tClass;

    public JsonDeserializer() {
    }

    public JsonDeserializer(Class<T> tClass){
        this.tClass=tClass;
    }


    @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            Deserializer.super.configure(configs, isKey);
        }

        @Override
        public T deserialize(String s, byte[] bytes) {

            if(bytes==null)
                return null;

            T data;

            try{
                data=objectMapper.readValue(bytes,tClass);

            }catch(Exception e){
                throw new SerializationException(e);
            }

            return data;
        }

        @Override
        public T deserialize(String topic, Headers headers, byte[] data) {
            return Deserializer.super.deserialize(topic, headers, data);
        }

        @Override
        public void close() {
            Deserializer.super.close();
        }
    }

  final class CustomSerdes{

    static public final class MySerde
           extends Serdes.WrapperSerde<KafkaStreamWeather.TempCalculator>{

        public MySerde(){
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(KafkaStreamWeather.TempCalculator.class));
        }
    }

      public static Serde<KafkaStreamWeather.TempCalculator> instance(){
        return new CustomSerdes.MySerde();
      }
  }