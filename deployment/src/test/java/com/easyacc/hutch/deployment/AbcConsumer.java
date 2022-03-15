package com.easyacc.hutch.deployment;

import com.easyacc.hutch.core.AbstractHutchConsumer;
import com.easyacc.hutch.core.Message;

/** Created by IntelliJ IDEA. User: wyatt Date: 2022/3/15 Time: 16:46 */
public class AbcConsumer extends AbstractHutchConsumer {
  @Override
  public void onMessage(Message message) {
    System.out.println("AbcConsumer received message: " + message.getBodyContentAsString());
  }
}
