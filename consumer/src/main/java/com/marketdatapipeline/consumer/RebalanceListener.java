package com.marketdatapipeline.consumer;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

// Keeps the in-memory cache from outliving partition ownership (invariant #4). Revoked
// and lost partitions are treated the same way — both mean this instance no longer owns
// them, and a stale cache entry surviving either would feed EmaCalculator a wrong basis
// on the next tick, silently writing a corrupted value into Redis. See DECISIONS.md.
public class RebalanceListener implements ConsumerAwareRebalanceListener {

  private static final Logger log = LoggerFactory.getLogger(RebalanceListener.class);

  private final TickStateStore stateStore;

  public RebalanceListener(TickStateStore stateStore) {
    this.stateStore = stateStore;
  }

  @Override
  public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
    clear("revoked", partitions);
  }

  @Override
  public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
    // By the time this fires, another instance may already own these partitions and have
    // written newer state for them — this clear is hygiene, not the thing preventing
    // damage. That's the offset CAS in apply_tick.lua: the new owner's writes already
    // advanced lastOffset, so anything this (now zombie) instance still has in flight gets
    // skipped, not applied, regardless of whether this listener ever fires. See DECISIONS.md.
    clear("lost", partitions);
  }

  @Override
  public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
    log.atInfo().addKeyValue("partitions", partitionNumbers(partitions)).log("partitions assigned");
  }

  private void clear(String event, Collection<TopicPartition> partitions) {
    Set<Integer> partitionNumbers = partitionNumbers(partitions);
    log.atInfo().addKeyValue("partitions", partitionNumbers).log("partitions " + event);
    stateStore.clearForPartitions(partitionNumbers);
  }

  private static Set<Integer> partitionNumbers(Collection<TopicPartition> partitions) {
    return partitions.stream().map(TopicPartition::partition).collect(Collectors.toSet());
  }
}
