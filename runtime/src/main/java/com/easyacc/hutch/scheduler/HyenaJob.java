package com.easyacc.hutch.scheduler;

import com.easyacc.hutch.Hutch;
import com.easyacc.hutch.core.HutchConsumer;
import com.easyacc.hutch.core.Threshold;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/** 提供基于 redis 主动式 hutch scheduler job. ScheduleExecutor 会对相同的任务进行约束, 不会并发执行 */
@Slf4j
public class HyenaJob implements Runnable {

  /** 根据 Threshold.key 计算出来的同一个 Consumer 的不同的 threshold.keys */
  private List<String> consumerThresholdKeys = new ArrayList<>();

  /** 缓存最后更新时间 */
  private Instant updatedAt = Instant.now().minusSeconds(11);

  private final HutchConsumer hc;

  public HyenaJob(HutchConsumer hutchConsumer) {
    // 如果初始化了 HyenaJob 那么就一定需要拥有 threshold
    Objects.requireNonNull(hutchConsumer);
    Objects.requireNonNull(Hutch.threshold(hutchConsumer));

    this.hc = hutchConsumer;
  }

  /** 便利获取 Threshold 方法 */
  public Threshold threshold() {
    return Hutch.threshold(this.hc);
  }

  @Override
  public void run() {
    // 检查 Hutch 是否启动
    if (Hutch.current() == null) {
      log.warn("Hutch 还未初始化完成. {} {}s 后再运行", hc.name(), threshold().interval());
      return;
    }
    if (!Hutch.current().isStarted()) {
      return;
    }

    // 1. 尝试刷新一次 redis keys
    this.reloadRedisKeys(this.hc.queue());
    // 2. 从 redis 中获取 task
    for (var key : this.consumerThresholdKeys) {
      this.fetchAndPublish(key);
    }
  }

  /** 刷新一次 redis keys */
  private void reloadRedisKeys(String prefix) {
    var intervals = Duration.between(this.updatedAt, Instant.now()).toSeconds();
    if (intervals < TimeUnit.MINUTES.toSeconds(1)) {
      log.debug("Reload skipped. The interval must > 1m, right now is: {}s", intervals);
    }

    this.consumerThresholdKeys = Hutch.redis().keys(String.format("%s.*", prefix));
    this.updatedAt = Instant.now();
  }

  public void fetchAndPublish(String key) {
    // TODO: 如果真的需要限速, 那么还需要知道 RabbitMQ 中对应的队列中拥有的队列数量. 只有当 RabbitMQ 中拥有的等待任务数量少于一定值的时候, 才能够从
    //  redis 向 rabbitmq 中推送. 不然 redis 单方面推送, 还是会让任务挤压在 RabbitMQ 当中从而失去 limit 的作用
    var tasks = Hutch.redis().zrange(key, 0, threshold().rate() - 1);
    if (tasks.isEmpty()) {
      log.debug("从 redis 中未找到任务数据! key: {}", key);
      return;
    }

    // 默认应该按照原来的 message body 原封不动的重新发送出去. 如果有自定义的 Publish 方法则使用他
    var batch = threshold().batch();
    if (batch == null) {
      tasks.forEach(hc::enqueue);
      log.debug("没有自定义 batch 方法, 使用默认的 HutchConsumer.enqueue");
    } else {
      batch.accept(tasks);
      log.debug("使用自定义的 batch 方法, 对 {} 条消息进行批处理", tasks.size());
    }
    // 从 redis 队列中移除 tasks
    Hutch.redis().zrem(key, tasks.toArray(String[]::new));
  }
}
