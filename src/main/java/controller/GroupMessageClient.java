package controller;

import beanstalk.bigtable.Beanstalk;
import beanstalk.bigtable.CreateIfNotExists;
import beanstalk.data.BeanstalkData;
import beanstalk.data.types.GroupMember;
import beanstalk.data.types.GroupMessage;
import beanstalk.data.types.Identifier;
import beanstalk.values.GatewayHeader;
import beanstalk.values.Project;
import beanstalk.values.Table;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.micronaut.context.event.ShutdownEvent;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.http.annotation.Header;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.websocket.WebSocketBroadcaster;
import io.micronaut.websocket.WebSocketSession;
import io.micronaut.websocket.annotation.OnClose;
import io.micronaut.websocket.annotation.OnMessage;
import io.micronaut.websocket.annotation.OnOpen;
import io.micronaut.websocket.annotation.ServerWebSocket;
import pubsub.PubSubSubscription;

import java.io.IOException;
import java.util.function.Predicate;

@ServerWebSocket("/")
public class GroupMessageClient {

    private final WebSocketBroadcaster broadcaster;

    private final PubSubSubscription pubSubSubscription;

    private BigtableDataClient dataClient;

    public GroupMessageClient(WebSocketBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
        pubSubSubscription = new PubSubSubscription(broadcaster);

        // Creates the settings to configure a bigtable data client.
        BigtableDataSettings settings =
                BigtableDataSettings.newBuilder().setProjectId(Project.PROJECT).setInstanceId(Table.INSTANCE).build();

        // Creates a bigtable data client.
        try {
            dataClient = BigtableDataClient.create(settings);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @EventListener
    public void onStartupEvent(StartupEvent event) {

        CreateIfNotExists.tables(Project.PROJECT, Table.INSTANCE, Table.GROUP_MESSAGE, GroupMessage.class);

        System.out.println("Starting up");
        pubSubSubscription.start();
    }

    @EventListener
    public void onShutdownEvent(ShutdownEvent event) {
        System.out.println("Shutting down");
        pubSubSubscription.shutdown();
    }

    @OnOpen
    public void onOpen(WebSocketSession session, @Header(GatewayHeader.account) String accountID) {
        System.out.println("Opened session");
    }

    @OnMessage
    public void onMessage(String message, WebSocketSession session, @Header(GatewayHeader.account) String accountID) {
        System.out.println(message);

        GroupMember groupMember = BeanstalkData.parse(message, GroupMember.class);

        if (Beanstalk.isMember(dataClient, accountID, groupMember.getGroupId())) {
            pubSubSubscription.toggleSubscription(groupMember, session.getId());

            broadcaster.broadcastAsync(groupMember, isValid(session));
        }

    }

    private Predicate<WebSocketSession> isValid(WebSocketSession session) {
        return s -> s == session;
    }

    @OnClose
    public void onClose(WebSocketSession session) {
        pubSubSubscription.removeId(session.getId());
        String msg = "Disconnected!";
        System.out.println(msg);
    }

}
