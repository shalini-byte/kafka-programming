import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AdminClientExample {

    private static Logger logger = LoggerFactory.getLogger(AdminClientExample.class);

    //private static Collection<String> TOPIC_LIST=Collections.singleton("sample2");
    private static Collection<String> TOPIC_LIST= Arrays.asList("CustomerCountry", "MyCountry");

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        AdminClient adminClient=getAdminClient();
        AdminClientExample adminClientExample=new AdminClientExample();
       adminClientExample.listTopics(adminClient);
        //adminClientExample.describeTopics(adminClient,"CustomerCountry");
       //createTopic("NewCountry",1,(short) 1);
       // adminClientExample.deleteTopic(adminClient,"test");
    }

    private static AdminClient getAdminClient() {

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient adminClient = AdminClient.create(props);
        return adminClient;
    }

    public void listTopics(AdminClient adminClient) throws ExecutionException, InterruptedException {

        ListTopicsResult topics = adminClient.listTopics();
        topics.names().get().forEach(System.out::println);
    }

    public void describeTopics(AdminClient adminClient, final String topicName) throws ExecutionException, InterruptedException {

        DescribeTopicsResult demoTopic=adminClient.describeTopics(TOPIC_LIST);
        try {
            final TopicDescription topicDescription = demoTopic.values().get(topicName).get();
            System.out.println("Description of demo topic:" + topicDescription);

          /*  if (topicDescription.partitions().size() != 2) {
                System.out.println("Topic has wrong number of partitions. Exiting.");
                System.exit(-1);
            }*/
        }
        catch (ExecutionException e){
            if (! (e.getCause() instanceof UnknownTopicOrPartitionException)) {
                e.printStackTrace();
                throw e;
            }
            System.out.println("Topic " + topicName + " does not exist. Going to create it now");
            createTopic(topicName,1, (short) 1);
        }


    }

    public static void createTopic(final String topicName, final int partitions, final short replicationFactor){
        try(final AdminClient adminClient = getAdminClient()){
            final NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
            createTopicsResult.values().get(topicName).get();
            adminClient.close(Duration.ofSeconds(30));
        }
        catch (InterruptedException | ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e.getMessage(), e);
            }
            e.printStackTrace();
        }

    }

    public  void deleteTopic(AdminClient adminClient,final String topicName) throws ExecutionException, InterruptedException {
        adminClient.deleteTopics(TOPIC_LIST).all().get();
        DeleteTopicsResult d=adminClient.deleteTopics(TOPIC_LIST);
       /* try {
            DescribeTopicsResult demoTopic=adminClient.describeTopics(TOPIC_LIST);
            final TopicDescription topicDescription = demoTopic.values().get(topicName).get();
            System.out.println("Topic " + topicName + " is still around");
        } catch (ExecutionException e) {
            System.out.println("Topic " + topicName + " is gone");
        }*/

    }



}

