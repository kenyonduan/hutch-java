# Hutch

Hutch 是一个利用 quarkus extension 的方式实现的与 https://github.com/wppurking/hutch-schedule
兼容的后端消息任务处理库.

其设计为利用 RabbitMQ 的 topic exchange 进行消息交互, 利用 classic queue 的 DLX + message ttl
机制进行消息重投的类似后端任务系统.
消息定时采取的是梯度式的延时, 因为 RabbitMQ 的限制无法保证秒级别的精准, 只拥有固定梯度的定时延迟.

## 安装

1. 在 quarkus 项目的 build.gradle 中添加 github 上的 packages

```groovy
repositories {
    mavenCentral()
    maven {
        name 'github packages'
        url 'https://maven.pkg.github.com/wppurking/hutch-java'
    }
    mavenLocal()
}
```

2. 添加依赖

```groovy
implementation 'com.easyacc:hutch:3.9.0'
```

## 使用

1. 添加需要的 rabbitmq 的配置, 这里使用 yaml 格式方便阅读

```yaml
quarkus:
  log:
    level: INFO

  # 使用 idea 会进行自动补全
  hutch:
    name: lake_web
    virtual-host: /
    username: guest
    password: guest
    hostname: 127.0.0.1
    # 是否使用 quorum queue
    quorum: false
    port: 5672
```

2. 添加 Consumer 实现 HutchConsumer 接口

```java
public class AbcTest implements HutchConsumer {

  private static final Logger log = Logger.getLogger(AbcTest.class);

  public void onMessage(Message message) {
    log.info(
        "Thraed: {}, msg: {}, prps: {}",
        Thread.currentThread().getName(),
        message.getBodyContentAsString(),
        message.getMessageProperties());
  }

  // 设置最大的重试次数
  @Override
  public int maxRetry() {
    return 5;
  }

  // 设置当前 AbcTest 的 queue 的最大并发 consumers
  @Override
  public int concurrency() {
    return 100;
  }
}
```

4. 自己在代码中进行启动 (后期可能会改为让 quarkus 自行合成好 Hutch bean 实例自行启动与停止)

```java

@ApplicationScoped
public class App {

  @Inject
  HutchConfig config;
  Hutch hutch;

  void onStart(@Observes StartupEvent ev) throws IOException {
    this.hutch = new Hutch(this.config).start();
  }

  void onStop(@Observes ShutdownEvent ev) {
    this.hutch.stop();
  }
}
```

5. 使用 quarkus cli 执行 `quarkus dev` 自动检查依赖并启动.

## Quorum Queue

在 hutch-java 中, 所有涉及到的 queue 只能同时使用一种类型的 queue type, 要么全部都是默认的 classic
queue, 要么就全部切换到集群方式的
quorum queue. 不在 HutchConsumer 级别做 quorum queue 申明的原因有:

1. 对 rabbitmq 版本到 3.10 以后, quorum queue 与 classic queue
   的区别已经[非常小](https://www.rabbitmq.com/quorum-queues.html)
2. quorum queue 可以在单节点也可以在多节点上使用
3. quorum queue 的高可用性非常用于代替 classic 是非常大的吸引力
4. 如果是产品环境, 大多数情况都应该部署高可用环境, 同时使用 quorum queue

所以, 在全局做了一个 quorum queue 开启关闭的开关.

## Maven

maven 3.9 的 部署问题: https://github.com/community/community/discussions/49001

1. 测试: `mvn test`. 每次构建一定要确保测试通过.
2. 构建到本地: `mvn install -DskipTests`
3. 推送远程的 github repo: `mvn deploy -DskipTests`

### 推送给 Github

注意在 `~/.m2/settings.xml` 中配置好 github 的 token 等信息.

如果出现 `401 Unauthorized`, 先检查两件事:

1. `pom.xml` 中的发布目标是否和你要发布的仓库一致. 当前项目默认发布到 `eardata/hutch-java`, 如果你是发布到自己的 fork, 用下面的方式覆盖:

  `mvn deploy -DskipTests -Dmaven.resolver.transport=wagon -Dgithub.owner=你的 GitHub 用户名 -Dgithub.repo=hutch-java`

2. `~/.m2/settings.xml` 里的 `<server><id>github</id></server>` 是否使用了对目标仓库有权限的 token. GitHub Packages 至少需要 `write:packages`, 私有仓库通常还需要 `repo`, 如果组织启用了 SSO 还要给 token 授权.

```xml

<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                      http://maven.apache.org/xsd/settings-1.0.0.xsd">

  <activeProfiles>
    <activeProfile>github</activeProfile>
  </activeProfiles>

  <profiles>
    <profile>
      <id>github</id>
      <repositories>
        <repository>
          <id>central</id>
          <url>https://repo1.maven.org/maven2</url>
        </repository>
        <repository>
          <id>github-repo</id>
          <url>https://maven.pkg.github.com/OWNER/REPOSITORY</url>
          <snapshots>
            <enabled>true</enabled>
          </snapshots>
        </repository>
      </repositories>
    </profile>
  </profiles>

  <servers>
    <server>
      <id>github</id>
      <username>wppurking</username>
      <password>xxxxx</password>
    </server>
  </servers>
</settings>
```
