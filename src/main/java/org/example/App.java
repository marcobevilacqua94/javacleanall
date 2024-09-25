package org.example;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.atr.ActiveTransactionRecordIds;
import com.couchbase.client.core.transaction.cleanup.CleanupRequest;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecord;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordEntry;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecords;
import com.couchbase.client.java.*;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupAttemptEvent;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupEndRunEvent;

import java.time.Duration;
import java.util.Optional;

/**
 * Hello world!
 */
public class App {

    public static void main(String[] args) {
        try (Cluster cluster = Cluster.connect(
                args[0],
                ClusterOptions.clusterOptions(args[1], args[2])
        )

        ) {
            System.out.println("Looking for hanging transaction...");

            ReactiveBucket bucket = cluster.bucket("test").reactive();
            bucket.waitUntilReady(Duration.ofSeconds(10)).block();
            for (String atr : ActiveTransactionRecordIds.allAtrs(1024)) {
                Optional<ActiveTransactionRecords> optActs = ActiveTransactionRecord.getAtr(cluster.core(), CollectionIdentifier.fromDefault("test"), atr, Duration.ofMillis(2500), null).onErrorComplete().block();
                if (optActs == null) continue;
                optActs.ifPresent(act -> {
                    System.out.println("Cleaning transaction record " + act.id());
                    for (ActiveTransactionRecordEntry entry : act.entries()) {
                        cluster.core().transactionsCleanup().getCleaner()
                                .performCleanup(CleanupRequest
                                                .fromAtrEntry(
                                                        CollectionIdentifier.fromDefault("test"),
                                                        entry
                                                )
                                        , false, null).block();
                    }
                });
            }
            cluster.environment().eventBus().subscribe(event -> {
                if (event instanceof TransactionCleanupAttemptEvent || event instanceof TransactionCleanupEndRunEvent) {
                    System.out.println(event.description());
                }
            });

            System.out.println("Finished");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }


}
