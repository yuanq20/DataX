package com.alibaba.datax.plugin.writer.kafka10writer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 写入 kafka1.0.2 插件
 *
 * @author 000623
 * @since 2019/4/9 10:45
 */
public class Kafka10Writer extends Writer {

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();

            this.validateParameter();
        }

        private void validateParameter() {
            this.writerSliceConfig.getNecessaryValue(Key.BOOTSTRAPSERVERS, Kafka10WriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.TOPIC, Kafka10WriterErrorCode.REQUIRED_VALUE);
        }

        @Override
        public void prepare() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.info("begin do split...");

            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration splitedTaskConfig = writerSliceConfig.clone();
//              splitedTaskConfig.set(Key.FILE_NAME, fileName);
                writerSplitConfigs.add(splitedTaskConfig);
            }

            LOG.info("end do split.");
            return writerSplitConfigs;
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;
        private Producer<String, String> producer = null;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
//          this.fileName = this.writerSliceConfig.getString(Key.FILE_NAME);
        }

        @Override
        public void prepare() {
            Properties props = new Properties();
            props.put("bootstrap.servers", writerSliceConfig.getString(Key.BOOTSTRAPSERVERS));
            props.put("acks", writerSliceConfig.getString(Key.ACKS, "1"));
            props.put("retries", writerSliceConfig.getInt(Key.RETRIES, 3));
            //指定异步发送中，一个批次含有多少字节数据，超过此值就会发送
            props.put("batch.size", writerSliceConfig.getInt(Key.BATCHSIZE, 16384));
            //停留时间的毫秒数，到达此值数据也会发送
            props.put("linger.ms", writerSliceConfig.getInt(Key.LINGERMS, 100));
            props.put("buffer.memory", writerSliceConfig.getLong(Key.BUFFERMEMORY, 33554432));
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            producer = new KafkaProducer<String, String>(props);

            LOG.info("init KafkaProducer success..." + props);
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            LOG.info("begin do write...");

            String fieldDelimiter = writerSliceConfig.getString(Key.FIELDDELIMITER, ",");

            Record record = null;
            long total = 0;
            while ((record = recordReceiver.getFromReader()) != null) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < record.getColumnNumber(); i++) {
                    Column column = record.getColumn(i);
                    sb.append(column.asString()).append(fieldDelimiter);
                }

                if(sb.length() > fieldDelimiter.length()) {
                    sb.setLength(sb.length() - fieldDelimiter.length());
                }

                // 发送kafka
                String topic = writerSliceConfig.getString(Key.TOPIC);
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,  sb.toString());
                producer.send(producerRecord);
            }

            LOG.info("end do write");
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {
            if(producer != null) {
                producer.close();
            }
        }
    }

}
