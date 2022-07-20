package pubsub;



import com.beanstalk.core.bigtable.BeanstalkData;
import com.beanstalk.core.spanner.entities.group.BetGroupMessage;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.*;
import io.micronaut.websocket.WebSocketBroadcaster;
import io.micronaut.websocket.WebSocketSession;

import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;

public class PubSubSubscription implements MessageReceiver {

    private final WebSocketBroadcaster broadcaster;

    private Subscriber subscriber;

    private ProjectSubscriptionName projectSubscriptionName;

    private Map<UUID, List<String>> subscribedIdentifiers;

    public PubSubSubscription(WebSocketBroadcaster broadcaster) {
        this.broadcaster = broadcaster;

        subscribedIdentifiers = new HashMap<>();

        String topic = System.getenv("TOPIC");

        ProjectTopicName projectTopicName = ProjectTopicName.parse(topic);
        String subscriptionName = projectTopicName.getTopic() + "-" + UUID.randomUUID();

        try {
            createPullSubscriptionExample(projectTopicName.getProject(), subscriptionName, projectTopicName.getTopic());
        } catch (IOException e) {
            e.printStackTrace();
        }

        projectSubscriptionName =
                ProjectSubscriptionName.of(projectTopicName.getProject(), subscriptionName);
        Subscriber.Builder builder =
                Subscriber.newBuilder(projectSubscriptionName, this);

        FlowControlSettings flowControlSettings =
                FlowControlSettings.newBuilder()
                        // 1,000 outstanding messages. Must be >0. It controls the maximum number of messages
                        // the subscriber receives before pausing the message stream.
                        .setMaxOutstandingElementCount(1000L)
                        // 100 MiB. Must be >0. It controls the maximum size of messages the subscriber
                        // receives before pausing the message stream.
                        .setMaxOutstandingRequestBytes(100L * 1024L * 1024L)
                        .build();

        try {
            builder = builder.setFlowControlSettings(flowControlSettings);
            this.subscriber = builder.build();
        } catch (Exception e) {
            System.out.println("Could not create subscriber: " + e);
            System.exit(1);
        }

    }

    public void start() {
        subscriber.startAsync().awaitRunning();
    }

    public void shutdown() {
        try {
            deleteSubscriptionExample(projectSubscriptionName.getProject(), projectSubscriptionName.getSubscription());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
        String msg = message.getData().toStringUtf8();

        System.out.println(msg);

        System.out.println(subscribedIdentifiers.toString());

        consumer.ack();

        BetGroupMessage groupMessage = BeanstalkData.parse(msg, BetGroupMessage.class);

        if (groupMessage != null && groupMessage.getBetGroup() != null && groupMessage.getBetGroup().getId() != null) {
            for (String id : subscribedIdentifiers.get(groupMessage.getBetGroup().getId())) {
                broadcaster.broadcastAsync(msg, isValid(groupMessage.getBetGroup().getId(), id));
            }
        }
    }

    private Predicate<WebSocketSession> isValid(UUID groupId, String id) {
        return s -> subscribedIdentifiers.get(groupId).contains(id) && s.getId().equals(id);
    }

    public static void createPullSubscriptionExample(String projectId, String subscriptionId, String topicId) throws IOException {
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            TopicName topicName = TopicName.of(projectId, topicId);
            ProjectSubscriptionName subscriptionName =
                    ProjectSubscriptionName.of(projectId, subscriptionId);
            // Create a pull subscription with default acknowledgement deadline of 10 seconds.
            // Messages not successfully acknowledged within 10 seconds will get resent by the server.
            Subscription subscription =
                    subscriptionAdminClient.createSubscription(
                            subscriptionName, topicName, PushConfig.getDefaultInstance(), 10);
            System.out.println("Created pull subscription: " + subscription.getName());
        }
    }

    public static void deleteSubscriptionExample(String projectId, String subscriptionId)
            throws IOException {
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            ProjectSubscriptionName subscriptionName =
                    ProjectSubscriptionName.of(projectId, subscriptionId);
            try {
                subscriptionAdminClient.deleteSubscription(subscriptionName);
                System.out.println("Deleted subscription.");
            } catch (NotFoundException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public void toggleSubscription(UUID groupId, String id) {
        if (!subscribedIdentifiers.containsKey(groupId)) {
            subscribedIdentifiers.put(groupId, new ArrayList<>());
        }

        if (!subscribedIdentifiers.get(groupId).contains(id)) {
            System.out.println("Adding " + id + " to " + groupId);
            subscribedIdentifiers.get(groupId).add(id);
        } else {
            System.out.println("Removing " + id + " from " + groupId);
            subscribedIdentifiers.get(groupId).remove(id);
        }
    }

    public void removeId(String id) {
        for (List<String> ids : subscribedIdentifiers.values()) {
            ids.remove(id);
        }
    }

}
