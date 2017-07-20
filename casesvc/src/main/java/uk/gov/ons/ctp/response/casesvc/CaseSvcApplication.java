package uk.gov.ons.ctp.response.casesvc;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import net.sourceforge.cobertura.CoverageIgnore;
import uk.gov.ons.ctp.common.distributed.*;
import uk.gov.ons.ctp.common.error.RestExceptionHandler;
import uk.gov.ons.ctp.common.jackson.CustomObjectMapper;
import uk.gov.ons.ctp.common.rest.RestClient;
import uk.gov.ons.ctp.common.state.StateTransitionManager;
import uk.gov.ons.ctp.common.state.StateTransitionManagerFactory;
import uk.gov.ons.ctp.response.casesvc.config.AppConfig;
import uk.gov.ons.ctp.response.casesvc.representation.CaseDTO;
import uk.gov.ons.ctp.response.casesvc.state.CaseSvcStateTransitionManagerFactory;

/**
 * The 'main' entry point for the CaseSvc SpringBoot Application.
 */
@CoverageIgnore
@SpringBootApplication
@EnableTransactionManagement
@IntegrationComponentScan
@ComponentScan(basePackages = {"uk.gov.ons.ctp.response"})
@EnableJpaRepositories(basePackages = {"uk.gov.ons.ctp.response"})
@EntityScan("uk.gov.ons.ctp.response")
@EnableAsync
@ImportResource("springintegration/main.xml")
public class CaseSvcApplication {

  public static final String CASE_DISTRIBUTION_LIST = "casesvc.case.distribution";
  public static final String REPORT_EXECUTION_LOCK = "casesvc.report.execution";

  @Autowired
  private AppConfig appConfig;

  @Autowired
  private StateTransitionManagerFactory caseSvcStateTransitionManagerFactory;

  /**
   * Bean to allow application to make controlled state transitions of Actions
   * @return the state transition manager specifically for Actions
   */
  @Bean
  public StateTransitionManager<CaseDTO.CaseState, CaseDTO.CaseEvent> caseSvcStateTransitionManager() {
    return caseSvcStateTransitionManagerFactory.getStateTransitionManager(
            CaseSvcStateTransitionManagerFactory.CASE_ENTITY);
  }

  /**
   * The DistributedListManager for CaseDistribution
   * @param redissonClient the redissonClient
   * @return the DistributedListManager
   */
  @Bean
  public DistributedListManager<Integer> caseDistributionListManager(RedissonClient redissonClient) {
    return new DistributedListManagerRedissonImpl<Integer>(
            CASE_DISTRIBUTION_LIST, redissonClient,
        appConfig.getDataGrid().getListTimeToWaitSeconds(),
        appConfig.getDataGrid().getListTimeToLiveSeconds());
  }

  /**
   * The RedissonClient
   * @return the RedissonClient
   */
  @Bean
  public RedissonClient redissonClient() {
    Config config = new Config();
    config.useSingleServer()
        .setAddress(appConfig.getDataGrid().getAddress())
        .setPassword(appConfig.getDataGrid().getPassword());
    return Redisson.create(config);
  }

  /**
   * The IAC service client bean
   * @return the RestClient for the IAC service
   */
  @Bean
  @Qualifier("internetAccessCodeServiceClient")
  public RestClient internetAccessCodeServiceClient() {
    RestClient restHelper = new RestClient(appConfig.getInternetAccessCodeSvc().getConnectionConfig());
    return restHelper;
  }

  /**
   * The action service client bean
   * @return the RestClient for the action service
   */
  @Bean
  @Qualifier("actionServiceClient")
  public RestClient actionServiceClient() {
    RestClient restHelper = new RestClient(appConfig.getActionSvc().getConnectionConfig());
    return restHelper;
  }

  /**
   * The collectionExercise service client bean
   * @return the RestClient for the collectionExercise service
   */
  @Bean
  @Qualifier("collectionExerciseSvcClient")
  public RestClient collectionExerciseServiceClient() {
    RestClient restHelper = new RestClient(appConfig.getCollectionExerciseSvc().getConnectionConfig());
    return restHelper;
  }

  /**
   * The RestExceptionHandler to handle exceptions thrown in our endpoints
   * @return the RestExceptionHandler
   */
  @Bean
  public RestExceptionHandler restExceptionHandler() {
    return new RestExceptionHandler();
  }

  /**
   * The CustomObjectMapper to output dates in the json in our agreed format
   * @return the CustomObjectMapper
   */
  @Bean @Primary
  public CustomObjectMapper customObjectMapper() {
    return new CustomObjectMapper();
  }

  /**
   * Bean used to access Distributed Lock Manager
   *
   * @param redissonClient Redisson Client
   * @return the Distributed Lock Manager
   */
  @Bean
  public DistributedInstanceManager reportDistributedInstanceManager(RedissonClient redissonClient) {
    return new DistributedInstanceManagerRedissonImpl(REPORT_EXECUTION_LOCK, redissonClient);
  }

  /**
   * Bean used to access Distributed Latch Manager
   *
   * @param redissonClient Redisson Client
   * @return the Distributed Lock Manager
   */
  @Bean
  public DistributedLatchManager reportDistributedLatchManager(RedissonClient redissonClient) {
    return new DistributedLatchManagerRedissonImpl(REPORT_EXECUTION_LOCK, redissonClient,
            appConfig.getDataGrid().getReportLockTimeToLiveSeconds());
  }

  /**
   * Bean used to access Distributed Execution Lock Manager
   *
   * @param redissonClient Redisson Client
   * @return the Distributed Lock Manager
   */
  @Bean
  public DistributedLockManager reportDistributedLockManager(RedissonClient redissonClient) {
    return new DistributedLockManagerRedissonImpl(REPORT_EXECUTION_LOCK, redissonClient,
            appConfig.getDataGrid().getReportLockTimeToLiveSeconds());
  }


  /**
   * The main entry point for this applicaion.
   *
   * @param args runtime command line args
   */
  public static void main(final String[] args) {
    SpringApplication.run(CaseSvcApplication.class, args);
  }
}
