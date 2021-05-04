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

    private static final Logger logger = LoggerFactory.getLogger(AdminClientExample.class);

    private static final Collection<String> TOPIC_LIST = Arrays.asList("CustomerCountry", "Products", "Sample");

    private static final Collection<String> TOPIC_LIST_DELETION = Arrays.asList("Sample", "Products");

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //creating instance of AdminClient class
        AdminClient adminClient = getAdminClient();

        //Creating instance of AdminClientExample Class
        AdminClientExample adminClientExample = new AdminClientExample();

        //listing topics present in the cluster
        adminClientExample.listTopics(adminClient);
        //describe topic of the cluster
        adminClientExample.describeTopics(adminClient, "CustomerCountry");
        //create topic in the cluster
        adminClientExample.createTopic(adminClient,"NewCountry", 1, (short) 1);
        //delete topics present in the cluster
        adminClientExample.deleteTopic(adminClient, "Products");
    }

    /**
     * This method creates and returns an instance of AdminClient class with necessary configurations.
     *
     * @return AdminClient class instance with configurations as provided in the Properties.
     */
    private static AdminClient getAdminClient() {

        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

    /**
     * This method lists all topics in the cluster and print topic names by iterating over the list.
     *
     * @param adminClient AdminClient class instance
     */
    public void listTopics(final AdminClient adminClient) throws ExecutionException, InterruptedException {

        ListTopicsResult topics = adminClient.listTopics();
        topics.names().get().forEach(System.out::println);
        adminClient.close(Duration.ofSeconds(30));
    }

    /**
     * This method describes and validates the configuration of the topic if it exists otherwise creates new topic by
     * handling the exception.
     *
     * @param adminClient AdminClient class instance
     * @param topicName   Topic name to be described
     */
    public void describeTopics(AdminClient adminClient, final String topicName)
            throws ExecutionException, InterruptedException {

        final DescribeTopicsResult demoTopic = adminClient.describeTopics(TOPIC_LIST);
        try {
            final TopicDescription topicDescription = demoTopic.values().get(topicName).get();
            System.out.println("Description of demo topic:" + topicDescription);

            if (topicDescription.partitions().size() != 1) {
                System.out.println("Topic has wrong number of partitions. Exiting.");
                System.exit(-1);
            }
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                System.out.println("Topic " + topicName + " does not exist. It will be created on spot");
                createTopic(adminClient, topicName, 1, (short) 1);
            }
        }
        adminClient.close(Duration.ofSeconds(30));
    }

    /**
     * This method creates new topic with the necessary configuration as provided by the parameters and throws
     * exception if the topic name provided already exists.
     *
     * @param adminClient       AdminClient class instance
     * @param topicName         name of the topic to be created
     * @param partitions        number of partitions in the new topic
     * @param replicationFactor replication factor for the new topic
     */
    public void createTopic(final AdminClient adminClient, final String topicName, final int partitions, final short replicationFactor)
            throws ExecutionException, InterruptedException {

        try {
            logger.info("The number of partitions and replicas are optional. If there are not specified, "
                    + "the defaults configured on the Kafka brokers will be used");
            final NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
       } 
        catch (InterruptedException | ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                DescribeTopicsResult describeTopics = adminClient.describeTopics(Collections.singleton(topicName));
                TopicDescription topicDescription = describeTopics.values().get(topicName).get();
                System.out.println("Description of topic: " + topicName + " " + topicDescription);
            }
        }
        adminClient.close(Duration.ofSeconds(30));
    }

    /**
     * This method deletes the topics provided in the topic list and checks if the topic has been deleted.
     *
     * @param adminClient AdminClient class instance
     * @param topicName   Topic name to be checked for deletion
     */
    public void deleteTopic(final AdminClient adminClient, final String topicName) throws ExecutionException,
            InterruptedException {

        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(TOPIC_LIST_DELETION);
        deleteTopicsResult.all().get();

        logger.info("Check that it is deleted. Note that due to the async nature of deletes, it is possible that at "
                + "this point the topic still exists");
        try {
            DescribeTopicsResult demoTopic = adminClient.describeTopics(TOPIC_LIST);
            TopicDescription topicDescription = demoTopic.values().get(topicName).get();
            System.out.println("Topic " + topicName + " is still present");
        } catch (ExecutionException e) {
            System.out.println("Topic " + topicName + " is deleted");
        }
        adminClient.close(Duration.ofSeconds(30));
    }

}
