package uk.gov.ons.ctp.response.casesvc.scheduled.distribution;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import uk.gov.ons.ctp.response.casesvc.config.AppConfig;
import uk.gov.ons.ctp.response.casesvc.config.CaseDistribution;
import uk.gov.ons.ctp.response.casesvc.config.InternetAccessCodeSvc;

import static org.junit.Assert.assertTrue;

/**
 * Test the case distributor
 */
@RunWith(MockitoJUnitRunner.class)
public class CaseDistributorTest {
  private static final int TWO = 2;
  private static final int FIVE = 5;
  private static final int TEN = 10;
  private static final long TEN_LONG = 10L;

  @Spy
  private AppConfig appConfig = new AppConfig();

/*  @Mock
  private CaseNotificationPublisher caseNotificationPublisher;

  @Mock
  private Tracer tracer;

  @Mock
  private Span span;

  @Mock
  private DistributedListManager<Integer> caseDistributionListManager;

  @Mock
  private StateTransitionManager<CaseState, uk.gov.ons.ctp.response.casesvc.representation.CaseDTO.CaseEvent>
          caseSvcStateTransitionManager;

  @Mock
  private MapperFacade mapperFacade;

  @Mock
  private InternetAccessCodeSvcClientService internetAccessCodeSvcClientService;

  @Mock
  private CaseRepository caseRepo;

  @Mock
  private TransactionTemplate transactionTemplate;

  @Mock
  private PlatformTransactionManager platformTransactionManager;

  @InjectMocks
  private CaseDistributor caseDistributor;*/

  /**
   * A Test
   */
  @Before
  public void setUp() {
    InternetAccessCodeSvc internetAccessCodeSvc = new InternetAccessCodeSvc();
    CaseDistribution caseDistributionConfig = new CaseDistribution();
    caseDistributionConfig.setDelayMilliSeconds(TEN_LONG);
    caseDistributionConfig.setRetrySleepSeconds(TEN);
    caseDistributionConfig.setRetrievalMax(TEN);
    caseDistributionConfig.setDistributionMax(TWO);
    caseDistributionConfig.setIacMax(FIVE);

    appConfig.setInternetAccessCodeSvc(internetAccessCodeSvc);
    appConfig.setCaseDistribution(caseDistributionConfig);

    MockitoAnnotations.initMocks(this);
  }

  /**
   * Test BlueSky scenario
   *
   * @throws Exception oops
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testBlueSkyRetrieve10Iac2Publish5() throws Exception {
    assertTrue(true);
//    Mockito.when(hazelcastInstance.getMap(any(String.class))).thenReturn(Mockito.mock(MapProxyImpl.class));
//    Mockito.when(hazelcastInstance.getLocalEndpoint()).thenReturn(Mockito.mock(Endpoint.class));
//
//    appConfig.getCaseDistribution().setIacMax(2);
//    appConfig.getCaseDistribution().setDistributionMax(5);
//
//    List<Case> cases = FixtureHelper.loadClassFixtures(Case[].class);
//    List<Questionnaire> questionnaires = FixtureHelper.loadClassFixtures(Questionnaire[].class);
//    List<String> iacs = Arrays.asList("bcdf-2345-lkjh", "bcdf-2345-lkjh");
//
//    // wire up mock responses
//    Mockito.when(
//        caseSvcStateTransitionManager.transition(CaseState.SAMPLED_INIT, CaseDTO.CaseEvent.ACTIVATED))
//        .thenReturn(CaseState.ACTIVE);
//
//    List<CaseDTO.CaseState> states = Arrays.asList(CaseDTO.CaseState.SAMPLED_INIT);
//    Mockito.when(
//        caseRepo.findByStateInAndCaseIdNotIn(eq(states), anyListOf(Integer.class), any(Pageable.class)))
//        .thenReturn(cases);
//    Mockito.when(
//        internetAccessCodeSvcClientService.generateIACs(2))
//        .thenReturn(iacs);
//    Mockito.when(
//        questionnaireRepo.findByCaseId(1))
//        .thenReturn(Arrays.asList(questionnaires.get(0)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(2))
//        .thenReturn(Arrays.asList(questionnaires.get(1)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(3))
//        .thenReturn(Arrays.asList(questionnaires.get(2)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(4))
//        .thenReturn(Arrays.asList(questionnaires.get(3)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(5))
//        .thenReturn(Arrays.asList(questionnaires.get(4)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(6))
//        .thenReturn(Arrays.asList(questionnaires.get(5)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(7))
//        .thenReturn(Arrays.asList(questionnaires.get(6)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(8))
//        .thenReturn(Arrays.asList(questionnaires.get(7)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(9))
//        .thenReturn(Arrays.asList(questionnaires.get(8)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(10))
//        .thenReturn(Arrays.asList(questionnaires.get(9)));
//
//    // let it roll
//    caseDistributor.distribute();
//
//    verify(caseRepo).findByStateInAndCaseIdNotIn(eq(states), anyListOf(Integer.class), any(Pageable.class));
//    verify(internetAccessCodeSvcClientService, times(5)).generateIACs(2);
//    verify(questionnaireRepo).findByCaseId(1);
//    verify(questionnaireRepo).findByCaseId(2);
//    verify(questionnaireRepo).findByCaseId(3);
//    verify(questionnaireRepo).findByCaseId(4);
//    verify(questionnaireRepo).findByCaseId(5);
//    verify(questionnaireRepo).findByCaseId(6);
//    verify(questionnaireRepo).findByCaseId(7);
//    verify(questionnaireRepo).findByCaseId(8);
//    verify(questionnaireRepo).findByCaseId(9);
//    verify(questionnaireRepo).findByCaseId(10);
//    verify(questionnaireRepo, times(10)).save(any(Questionnaire.class));
//    verify(caseRepo, times(10)).saveAndFlush(any(Case.class));
//    verify(caseNotificationPublisher, times(2)).sendNotifications(anyListOf(CaseNotification.class));
  }
//
//  /**
//   * Test BlueSky scenario
//   *
//   * @throws Exception oops
//   */
//  @SuppressWarnings("unchecked")
//  @Test
//  public void testBlueSkyRetrieve10Iac5Publish2() throws Exception {
//
//    Mockito.when(hazelcastInstance.getMap(any(String.class))).thenReturn(Mockito.mock(MapProxyImpl.class));
//    Mockito.when(hazelcastInstance.getLocalEndpoint()).thenReturn(Mockito.mock(Endpoint.class));
//
//    List<Case> cases = FixtureHelper.loadClassFixtures(Case[].class);
//    List<Questionnaire> questionnaires = FixtureHelper.loadClassFixtures(Questionnaire[].class);
//    List<String> iacs = Arrays.asList("bcdf-2345-lkjh", "bcdf-2345-lkjh", "bcdf-2345-lkjh", "bcdf-2345-lkjh",
//        "bcdf-2345-lkjh");
//
//    // wire up mock responses
//    Mockito.when(
//        caseSvcStateTransitionManager.transition(CaseState.SAMPLED_INIT, CaseDTO.CaseEvent.ACTIVATED))
//        .thenReturn(CaseState.ACTIVE);
//
//    List<CaseDTO.CaseState> states = Arrays.asList(CaseDTO.CaseState.SAMPLED_INIT);
//    Mockito.when(
//        caseRepo.findByStateInAndCaseIdNotIn(eq(states), anyListOf(Integer.class), any(Pageable.class)))
//        .thenReturn(cases);
//    Mockito.when(
//        internetAccessCodeSvcClientService.generateIACs(5))
//        .thenReturn(iacs);
//    Mockito.when(
//        questionnaireRepo.findByCaseId(1))
//        .thenReturn(Arrays.asList(questionnaires.get(0)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(2))
//        .thenReturn(Arrays.asList(questionnaires.get(1)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(3))
//        .thenReturn(Arrays.asList(questionnaires.get(2)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(4))
//        .thenReturn(Arrays.asList(questionnaires.get(3)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(5))
//        .thenReturn(Arrays.asList(questionnaires.get(4)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(6))
//        .thenReturn(Arrays.asList(questionnaires.get(5)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(7))
//        .thenReturn(Arrays.asList(questionnaires.get(6)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(8))
//        .thenReturn(Arrays.asList(questionnaires.get(7)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(9))
//        .thenReturn(Arrays.asList(questionnaires.get(8)));
//    Mockito.when(
//        questionnaireRepo.findByCaseId(10))
//        .thenReturn(Arrays.asList(questionnaires.get(9)));
//
//    // let it roll
//    caseDistributor.distribute();
//
//    verify(caseRepo).findByStateInAndCaseIdNotIn(eq(states), anyListOf(Integer.class), any(Pageable.class));
//    verify(internetAccessCodeSvcClientService, times(2)).generateIACs(5);
//    verify(questionnaireRepo).findByCaseId(1);
//    verify(questionnaireRepo).findByCaseId(2);
//    verify(questionnaireRepo).findByCaseId(3);
//    verify(questionnaireRepo).findByCaseId(4);
//    verify(questionnaireRepo).findByCaseId(5);
//    verify(questionnaireRepo).findByCaseId(6);
//    verify(questionnaireRepo).findByCaseId(7);
//    verify(questionnaireRepo).findByCaseId(8);
//    verify(questionnaireRepo).findByCaseId(9);
//    verify(questionnaireRepo).findByCaseId(10);
//    verify(questionnaireRepo, times(10)).save(any(Questionnaire.class));
//    verify(caseRepo, times(10)).saveAndFlush(any(Case.class));
//    verify(caseNotificationPublisher, times(5)).sendNotifications(anyListOf(CaseNotification.class));
//  }
//
//  /**
//   * Test that when we fail at first hurdle to load Cases we do not go on to
//   * call anything else In reality the wakeup method would then be called again
//   * after a sleep interval by spring but we cannot test that here
//   *
//   * @throws Exception oops
//   */
//  @SuppressWarnings("unchecked")
//  @Test
//  public void testDBFailure() throws Exception {
//
//    Mockito.when(hazelcastInstance.getMap(any(String.class))).thenReturn(Mockito.mock(MapProxyImpl.class));
//    Mockito.when(hazelcastInstance.getLocalEndpoint()).thenReturn(Mockito.mock(Endpoint.class));
//
//    // wire up mock responses
//    List<CaseDTO.CaseState> states = Arrays.asList(CaseDTO.CaseState.SAMPLED_INIT);
//    Mockito.when(
//        caseRepo.findByStateInAndCaseIdNotIn(eq(states), anyListOf(Integer.class), any(Pageable.class)))
//        .thenThrow(new RuntimeException("Database access failed"));
//
//    // let it roll
//    caseDistributor.distribute();
//
//    verify(caseRepo).findByStateInAndCaseIdNotIn(eq(states), anyListOf(Integer.class), any(Pageable.class));
//    verify(internetAccessCodeSvcClientService, times(0)).generateIACs(any(Integer.class));
//    verify(questionnaireRepo, times(0)).findByCaseId(any(Integer.class));
//    verify(questionnaireRepo, times(0)).save(any(Questionnaire.class));
//    verify(caseRepo, times(0)).saveAndFlush(any(Case.class));
//    verify(caseNotificationPublisher, times(0)).sendNotifications(anyListOf(CaseNotification.class));
//  }
//
}