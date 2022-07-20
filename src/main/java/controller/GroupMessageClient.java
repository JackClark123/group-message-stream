package controller;


import com.beanstalk.core.bigtable.BeanstalkData;
import com.beanstalk.core.spanner.entities.account.PublicAccount;
import com.beanstalk.core.spanner.entities.group.BetGroup;
import com.beanstalk.core.spanner.entities.group.BetGroupMember;
import com.beanstalk.core.spanner.entities.group.BetGroupMessage;
import com.beanstalk.core.spanner.entities.group.id.GroupMemberId;
import com.beanstalk.core.spanner.repositories.BetGroupMemberRepository;
import com.beanstalk.core.values.GatewayHeader;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.micronaut.context.event.ShutdownEvent;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Header;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.websocket.WebSocketBroadcaster;
import io.micronaut.websocket.WebSocketSession;
import io.micronaut.websocket.annotation.OnClose;
import io.micronaut.websocket.annotation.OnMessage;
import io.micronaut.websocket.annotation.OnOpen;
import io.micronaut.websocket.annotation.ServerWebSocket;
import jakarta.inject.Inject;
import pubsub.PubSubSubscription;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Predicate;

@ServerWebSocket("/")
public class GroupMessageClient {

    @Inject
    BetGroupMemberRepository betGroupMemberRepository;

    private final WebSocketBroadcaster broadcaster;

    private final PubSubSubscription pubSubSubscription;

    public GroupMessageClient(WebSocketBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
        pubSubSubscription = new PubSubSubscription(broadcaster);
    }

    @EventListener
    public void onStartupEvent(StartupEvent event) {
        System.out.println("Starting up");
        pubSubSubscription.start();
    }

    @EventListener
    public void onShutdownEvent(ShutdownEvent event) {
        System.out.println("Shutting down");
        pubSubSubscription.shutdown();
    }

    @OnOpen
    public void onOpen(WebSocketSession session, @Header(GatewayHeader.account) UUID accountID) {
        System.out.println("Opened session");
    }

    @OnMessage
    public void onMessage(WebSocketSession session, @Header(GatewayHeader.account) UUID accountID, @Body @NotNull UUID groupId) {
        System.out.println(groupId);

        GroupMemberId groupMemberId = GroupMemberId.builder()
                .publicAccount(PublicAccount.builder().id(accountID).build())
                .betGroup(BetGroup.builder().id(groupId).build())
                .build();

        if (betGroupMemberRepository.existsById(groupMemberId)) {
            pubSubSubscription.toggleSubscription(groupId, session.getId());

            broadcaster.broadcastAsync("Listening to messages for bet group: " + groupId, isValid(session));
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
