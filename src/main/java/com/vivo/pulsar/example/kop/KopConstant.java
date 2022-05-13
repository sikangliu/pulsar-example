package com.vivo.pulsar.example.kop;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2021-11-03 10:31
 */
public class KopConstant {


    public static final String SERVICE_URL = "pulsar://10.101.129.84:6650,10.101.129.85:6650,10.101.129.86:6650";
    public static final String TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwdWxzYXJhZG1pbiJ9.ikxLUA8g-wYHNcMPAnu_U9iM5qtlYCmzdI9bZlGSSRM";

    public static final String TOPIC_NAME = "persistent://kop-tenant/kop-test1/kop-topic1";
    //public static final String TOPIC_NAME = "kop-test";
    public static final String PRODUCER_NAME = "kop_pro";
    public static final String SUBSCRIPTION_NAME = "sub01";
    public static final String CONSUMER_NAME = "kop_con";


    public static final String DOMAIN_NAME = "10.101.129.84:9092,10.101.129.85:9092,10.101.129.86:9092";
    public static final String SERIALIZATION = "org.apache.kafka.common.serialization.StringSerializer";

    public static final String PRO_CLIENT_ID = "kop_pro";
    public static final String TENANT = "kop-tenant/kop-test1";
    public static final String PASSWORD = "token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwdWxzYXJhZG1pbiJ9.ikxLUA8g-wYHNcMPAnu_U9iM5qtlYCmzdI9bZlGSSRM";

    public static final String CON_CLIENT_ID = "kop_con";
    public static final String GROUP_NAME = "sub01";
    public static final String DESERIALIZATION = "org.apache.kafka.common.serialization.StringDeserializer";

}
