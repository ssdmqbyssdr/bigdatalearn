package hadoopexample.KafkaOption;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class UmetaKafka {

    private final KafkaProducer<String, String> producer;

    private final KafkaConsumer<String, String> consumer;

    public final static String TOPIC = "o-log-task-monitor";

    public UmetaKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "udp01:9092,udp02:9092,udp03:9092");//xxx服务器ip
        props.put("acks", "all");//所有follower都响应了才认为消息提交成功，即"committed"
        props.put("retries", 0);//retries = MAX 无限重试，直到你意识到出现了问题:)
        props.put("batch.size", 16384);//producer将试图批处理消息记录，以减少请求次数.默认的批量处理消息字节数
        //batch.size当批量的数据大小达到设定值后，就会立即发送，不顾下面的linger.ms
        props.put("linger.ms", 1);//延迟1ms发送，这项设置将通过增加小的延迟来完成--即，不是立即发送一条记录，producer将会等待给定的延迟时间以允许其他消息记录发送，这些消息记录可以批量处理
        props.put("buffer.memory", 33554432);//producer可以用来缓存数据的内存大小。
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", UUID.randomUUID().toString());


        producer = new KafkaProducer<String, String>(props);
        consumer = new KafkaConsumer<String, String>(props);

    }

    @Test
    public void produce() throws InterruptedException {
        int messageNo = 1;
        final int COUNT = 500;
        while (messageNo < COUNT) {
//        String data = String.format("{\"age\":1,\"name\":\"chenlan\",\"enheng\":\"ee\",\"name1\":\"chenlan\"}");
            String application_id = getTime()[1];
            String application_name = getTime()[1];
            String computerTime = getTime()[0];

            String[] s = getTable();
            String table_type = s[0];
            String table_name = s[1];

            String[] s1 = getTable();
            String table_type2 = s1[0];
            String table_name2 = s1[1];

            String applicationType = getApplicationType();


            String data = String.format("{\"applicationId\":\"163964717861212\",\"applicationName\":\"163964717861212\",\"applicationType\":" + "\"" + applicationType +"\"" + ",\"tableName\":" + "\"" + table_name +"\"" + ",\"operaType\":\"1\",\"tableType\":" + "\"" + table_type+"\"" + ",\"tableCount\":\"243225\",\"computerTime\":\"2021-12-16 17:32:58\"}");

//            String data = String.format("{\"applicationId\":" + "\"" + application_id +"\"" + ",\"applicationName\":" + "\"" + application_name +"\"" + ",\"applicationType\":" + "\"" + applicationType +"\"" + ",\"tableName\":" + "\"" + table_name +"\"" + ",\"operaType\":\"1\",\"tableType\":" + "\"" + table_type+"\"" + ",\"tableCount\":\"243225\",\"computerTime\":" + "\"" + computerTime+"\"" +"}");
//            String data2 = String.format("{\"applicationId\":"+ "\"" +  application_id +"\"" + ",\"applicationName\":" + "\"" + application_name +"\"" + ",\"applicationType\":" + "\"" + applicationType +"\"" + ",\"tableName\":" + "\"" + table_name2 +"\"" + ",\"operaType\":\"0\",\"tableType\":" + "\"" + table_type2 +"\"" + ",\"tableCount\":\"243225\",\"computerTime\":" + "\"" + computerTime+"\"" +"}");

            try {
                System.out.println(data);
//                System.out.println(data2);
                producer.send(new ProducerRecord<String, String>(TOPIC, data));
//                producer.send(new ProducerRecord<String, String>(TOPIC, data2));
            } catch (Exception e) {
                e.printStackTrace();
            }
            messageNo++;
//            Thread.currentThread().sleep(1);
        }
        producer.close();
    }


    //获取表及类型
    public static String[] getTable() {
        String[] Mysql_table_list = {"auth_group","auth_group_permissions","auth_permission","auth_user","auth_user_groups","auth_user_user_permissions","AUX_TABLE","axes_accessattempt","axes_accesslog","beeswax_metainstall","beeswax_queryhistory","beeswax_savedquery","beeswax_session","BUCKETING_COLS","BUNDLE_ACTIONS","BUNDLE_JOBS","CDS","chart","COLUMNS_V2","COMPACTION_QUEUE","COMPLETED_COMPACTIONS","COMPLETED_TXN_COMPONENTS","connection","COORD_ACTIONS","COORD_JOBS","dag","dag_code","dag_pickle","dag_run","dag_tag","DATABASE_PARAMS","DBS","DB_PRIVS","defaultconfiguration_groups","DELEGATION_TOKENS","desktop_connector","desktop_defaultconfiguration","desktop_document","desktop_document2","desktop_document2permission","desktop_document2_dependencies","desktop_documentpermission","desktop_documenttag","desktop_document_tags","desktop_settings","desktop_userpreferences","django_admin_log","django_content_type","django_migrations","django_session","django_site","documentpermission2_groups","documentpermission2_users","documentpermission_groups","documentpermission_users","FUNCS","FUNC_RU","GLOBAL_PRIVS","HIVE_LOCKS","IDXS","import_error","INDEX_PARAMS","job","jobsub_checkforsetup","jobsub_jobdesign","jobsub_jobhistory","jobsub_oozieaction","jobsub_ooziedesign","jobsub_ooziejavaaction","jobsub_ooziemapreduceaction","jobsub_ooziestreamingaction","KEY_CONSTRAINTS","known_event","known_event_type","kube_resource_version","kube_worker_uuid","log","MASTER_KEYS","NEXT_COMPACTION_QUEUE_ID","NEXT_LOCK_ID","NEXT_TXN_ID","NOTIFICATION_LOG","NOTIFICATION_SEQUENCE","NUCLEUS_TABLES","oozie_bundle","oozie_bundledcoordinator","oozie_coordinator","oozie_datainput","oozie_dataoutput","oozie_dataset","oozie_decision","oozie_decisionend","oozie_distcp","oozie_email","oozie_end","oozie_fork","oozie_fs","oozie_generic","oozie_history","oozie_hive","oozie_java","oozie_job","oozie_join","oozie_kill","oozie_link","oozie_mapreduce","oozie_node","oozie_pig","oozie_shell","oozie_sqoop","oozie_ssh","oozie_start","oozie_streaming","oozie_subworkflow","OOZIE_SYS","oozie_workflow","OPENJPA_SEQUENCE_TABLE","PARTITIONS","PARTITION_EVENTS","PARTITION_KEYS","PARTITION_KEY_VALS","PARTITION_PARAMS","PART_COL_PRIVS","PART_COL_STATS","PART_PRIVS","pig_document","pig_pigscript","pm_md_code","pm_md_meta_hbase_cf","pm_md_meta_hbase_column","pm_md_meta_hbase_mapping","pm_md_meta_hbase_namespace","pm_md_meta_hbase_table","pm_md_meta_hive_column","pm_md_meta_hive_database","pm_md_meta_hive_mapping","pm_md_meta_hive_table","pm_md_meta_kafka_column","pm_md_meta_kafka_database","pm_md_meta_kafka_mapping","pm_md_meta_kafka_table","pm_md_meta_kudu_column","pm_md_meta_kudu_database","pm_md_meta_kudu_mapping","pm_md_meta_kudu_table","pm_md_meta_lineage","pm_md_meta_mysql_column","pm_md_meta_mysql_database","pm_md_meta_mysql_index","pm_md_meta_mysql_mapping","pm_md_meta_mysql_table","pm_md_meta_oracle_column","pm_md_meta_oracle_database","pm_md_meta_oracle_index","pm_md_meta_oracle_mapping","pm_md_meta_oracle_table","pm_md_meta_role","pm_md_meta_user","pm_md_table_business_struct","pm_md_table_struct","QRTZ_BLOB_TRIGGERS","QRTZ_CALENDARS","QRTZ_CRON_TRIGGERS","QRTZ_FIRED_TRIGGERS","QRTZ_JOB_DETAILS","QRTZ_LOCKS","QRTZ_PAUSED_TRIGGER_GRPS","QRTZ_SCHEDULER_STATE","QRTZ_SIMPLE_TRIGGERS","QRTZ_SIMPROP_TRIGGERS","QRTZ_TRIGGERS","rendered_task_instance_fields","ROLES","ROLE_MAP","SDS","SD_PARAMS","search_collection","search_facet","search_result","search_sorting","SEQUENCE_TABLE","SERDES","SERDE_PARAMS","serialized_dag","SKEWED_COL_NAMES","SKEWED_COL_VALUE_LOC_MAP","SKEWED_STRING_LIST","SKEWED_STRING_LIST_VALUES","SKEWED_VALUES","SLA_EVENTS","sla_miss","SLA_REGISTRATION","SLA_SUMMARY","slot_pool","SORT_COLS","SPRING_SESSION","SPRING_SESSION","SPRING_SESSION_ATTRIBUTES","SPRING_SESSION_ATTRIBUTES","start_befor","t1","TABLE_PARAMS","TAB_COL_STATS","task_fail","task_instance","task_reschedule","TBLS","TBL_COL_PRIVS","TBL_PRIVS","test_1210_01","test_1210_01_rename","TXNS","TXN_COMPONENTS","TYPES","TYPE_FIELDS","t_ds_access_token","t_ds_alert","t_ds_alertgroup","t_ds_command","t_ds_datasource","t_ds_error_command","t_ds_master_server","t_ds_process_definition","t_ds_process_instance","t_ds_project","t_ds_queue","t_ds_relation_datasource_user","t_ds_relation_process_instance","t_ds_relation_project_user","t_ds_relation_resources_user","t_ds_relation_udfs_user","t_ds_relation_user_alertgroup","t_ds_resources","t_ds_schedules","t_ds_session","t_ds_task_instance","t_ds_tenant","t_ds_udfs","t_ds_user","t_ds_version","t_ds_worker_group","t_ds_worker_server","t_udp_alert_expression","t_udp_alert_notification","t_udp_alert_receiver","t_udp_alert_receiver_group","t_udp_alert_receiver_group_map","t_udp_alert_rule","t_udp_alert_rule_receiver_group_map","t_udp_alert_sender","t_udp_alert_send_record","t_udp_alert_template","t_udp_alert_template_cluster_map","t_udp_alert_template_rule_map","t_udp_alert_ucloud_sms_record","t_udp_cal_cluster_info","t_udp_cluster_alerttemplate_map","t_udp_cluster_host_mapping","t_udp_cluster_info","t_udp_cluster_state","t_udp_cluster_user_mapping","t_udp_component_info","t_udp_component_state","t_udp_config","t_udp_config_history","t_udp_element_action_const","t_udp_element_api_const","t_udp_element_entrance_const","t_udp_env_config","t_udp_host_alive","t_udp_host_env_check","t_udp_host_env_info","t_udp_host_info","t_udp_host_state","t_udp_host_update_info","t_udp_license_info","t_udp_monitor_component_state","t_udp_operation","t_udp_os","t_udp_pre_modified_config","t_udp_pullup_config","t_udp_role","t_udp_role_cluster_map","t_udp_role_element_action_map","t_udp_role_element_api_map","t_udp_role_element_entrance_map","t_udp_service_component_mapping","t_udp_service_config_mapping","t_udp_service_info","t_udp_service_log_file","t_udp_service_relationship","t_udp_service_state","t_udp_stage","t_udp_task","t_udp_user","t_udp_user_role_map","t_udp_version","useradmin_grouppermission","useradmin_huepermission","useradmin_ldapgroup","useradmin_userprofile","users","VALIDATE_CONN","variable","VERSION","WF_ACTIONS","WF_JOBS","WRITE_SET","xa_access_audit","xcom","x_access_type_def","x_access_type_def_grants","x_asset","x_audit_map","x_auth_sess","x_context_enricher_def","x_cred_store","x_datamask_type_def","x_data_hist","x_db_base","x_db_version_h","x_enum_def","x_enum_element_def","x_group","x_group_groups","x_group_module_perm","x_group_users","x_modules_master","x_perm_map","x_plugin_info","x_policy","x_policy_condition_def","x_policy_export_audit","x_policy_item","x_policy_item_access","x_policy_item_condition","x_policy_item_datamask","x_policy_item_group_perm","x_policy_item_rowfilter","x_policy_item_user_perm","x_policy_label","x_policy_label_map","x_policy_resource","x_policy_resource_map","x_portal_user","x_portal_user_role","x_resource","x_resource_def","x_service","x_service_config_def","x_service_config_map","x_service_def","x_service_resource","x_service_resource_element","x_service_resource_element_val","x_service_version_info","x_tag","x_tag_attr","x_tag_attr_def","x_tag_def","x_tag_resource_map","x_trx_log","x_ugsync_audit_info","x_user","x_user_module_perm"};
        String[] Hive_table_list = {"xueyuan_hive_01","test_1210_01"};
        String[] Hbase_table_list = {"apache_atlas_entity_audit","apache_atlas_janus","kylin_metadata"};
        String[] Kafka_table_list = {"TOPIC-UMETA-LINEAGE-TEST","test5","liujingze","o-log-task-monitor-1","flip","o-log-task-monitor-2","ora","zhanganlun","xuxiaofeng","chenlan","fos","CL-META-TEST","ATLAS_ENTITIES","usdp_test","xueyuan_test_01","o-log-task-monitor\t"};
        String[] Kudu_table_list = {"test_1210_02","xueyuan_kudu_01"};
        String[] Oracle_table_list = {"DELETE_TEST","cl_Test","CL111","CL111","LLM_TG_DATA_METER_QAWA0101","UCLOUD_TEST","WS","cl_Test","TEST_1210_01"};

        String table_type = "mysql";
        String table_name = "SPRING_SESSION";

        int i = RandomUtils.nextInt(100);
        if (i>=0 && i<=10) {
            table_type = "Hive";
            int j = Hive_table_list.length;
            table_name = Hive_table_list[RandomUtils.nextInt(j)];
        }
        else if (i>10 && i<=20) {
            table_type = "Hbase";
            int j = Hbase_table_list.length;
            table_name = Hbase_table_list[RandomUtils.nextInt(j)];
        }
        else if (i>20 && i<=30) {
            table_type = "Kafka";
            int j = Kafka_table_list.length;
            table_name = Kafka_table_list[RandomUtils.nextInt(j)];

        }
        else if (i>30 && i<=40) {
            table_type = "Kudu";
            int j = Kudu_table_list.length;
            table_name = Kudu_table_list[RandomUtils.nextInt(j)];
        }
        else if (i>40 && i<=50) {
            table_type = "Oracle";
            int j = Oracle_table_list.length;
            table_name = Oracle_table_list[RandomUtils.nextInt(j)];
        }
        else  {
            table_type = "Mysql";
            int j = Mysql_table_list.length;
            table_name = Mysql_table_list[RandomUtils.nextInt(j)];
        }
        String[] table_info = {table_type,table_name} ;
        return table_info;
    }

    //获取时间和时间戳，将时间戳作为ID
    public static String[] getTime() {
        Date date = new Date();
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String str = format.format(date);
        long currentTime = Calendar.getInstance().getTimeInMillis();
        String s = String.valueOf(currentTime);
        String[] time_list = {str, s};
        return time_list;
    }

    //随机返回任务类型
    public static String getApplicationType() {
        String[] applocation_type_list = {"Flink", "Spark"};
        int i = applocation_type_list.length;
        String applocation_type = applocation_type_list[RandomUtils.nextInt(i)];
        return applocation_type;

    }

    //        @Test
//    public String getTable(int i){
//        String[] mysql_table_list = {"xueyuan_hive_01"};
//        String[] hive_table_list = {"xueyuan_hive_01","test_1210_01"};
//    }
    public void main() {
        String table_type = getTable()[0];
        String table_name = getTable()[1];

        System.out.println(table_type);
        System.out.println(table_name);

    }
}
