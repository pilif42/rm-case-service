package uk.gov.ons.ctp.response.casesvc.utility;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import uk.gov.ons.ctp.response.casesvc.service.CaseService;

import static uk.gov.ons.ctp.response.casesvc.message.impl.CaseNotificationPublisherImpl.HEADER_USED_TO_ROLLBACK;
import static uk.gov.ons.ctp.response.casesvc.utility.BespokeConfirmCallback.DELIVERY_FAILURE_MSG;

@Slf4j
@Component
public class BespokeReturnCallback implements RabbitTemplate.ReturnCallback {

  @Autowired
  private CaseService caseService;

  @Override
  public void returnedMessage(Message message, int replyCode, String replyText, String exchange, String routingKey) {
    log.info("returning message {} with replyCode {} - replyText {} - exchange {} - routingKey {}", message, replyCode,
        replyText, exchange, routingKey);

    String errorMsg = String.format(DELIVERY_FAILURE_MSG, replyText);
    log.error(errorMsg);

    MessageProperties messageProps = message.getMessageProperties();
    String correlationIdString = (String)messageProps.getHeaders().get(HEADER_USED_TO_ROLLBACK);
    log.info("correlationIdString is {}", correlationIdString);

    caseService.rollbackForNotificationPublisher(correlationIdString);
  }
}