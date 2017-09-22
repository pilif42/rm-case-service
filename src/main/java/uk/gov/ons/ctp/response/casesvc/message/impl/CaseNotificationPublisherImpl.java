package uk.gov.ons.ctp.response.casesvc.message.impl;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.annotation.MessageEndpoint;
import uk.gov.ons.ctp.response.casesvc.message.CaseNotificationPublisher;
import uk.gov.ons.ctp.response.casesvc.message.notification.CaseNotification;

/**
 * Service implementation responsible for publishing case lifecycle events to
 * notification channel
 *
 */
@MessageEndpoint
@Slf4j
public class CaseNotificationPublisherImpl implements CaseNotificationPublisher {

  public static final String HEADER_USED_TO_ROLLBACK = "correlationDataId";
  private static final String LIFECYCLE_EVENTS_ROUTING_KEY = "Case.LifecycleEvents.binding";

  @Qualifier("caseNotificationRabbitTemplate")
  @Autowired
  private RabbitTemplate rabbitTemplate;

  @Override
  public void sendNotification(CaseNotification caseNotification) {
    log.debug("Entering sendNotification with CaseNotification {}", caseNotification);
    CorrelationData correlationData = new CorrelationData();
    String caseId = caseNotification.getCaseId();
    correlationData.setId(caseId);
    rabbitTemplate.convertAndSend(LIFECYCLE_EVENTS_ROUTING_KEY, caseNotification, correlationData);
    log.info("caseNotification published");
  }

  @Override
  public void sendNotification(CaseNotification caseNotification, String correlationDataId) {
    log.info("Entering sendNotification with CaseNotification {} and correlationDataId {}", caseNotification,
        correlationDataId);

    // Required for correlating publisher returns to sent messages.
    MessagePostProcessor messagePostProcessor = new MessagePostProcessor() {
      public Message postProcessMessage(Message message) throws AmqpException {
        log.info("correlationDataId is {}", correlationDataId);
        message.getMessageProperties().setHeader(HEADER_USED_TO_ROLLBACK, correlationDataId);
        return message;
      }
    };

    // Required for correlating publisher confirms to sent messages.
    CorrelationData correlationData = new CorrelationData();
    correlationData.setId(correlationDataId);

    rabbitTemplate.convertAndSend(LIFECYCLE_EVENTS_ROUTING_KEY, caseNotification, messagePostProcessor,
        correlationData);
    log.info("caseNotification published");
  }
}
