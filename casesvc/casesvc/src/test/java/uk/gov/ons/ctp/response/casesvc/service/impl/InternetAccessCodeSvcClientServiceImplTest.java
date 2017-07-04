package uk.gov.ons.ctp.response.casesvc.service.impl;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import uk.gov.ons.ctp.common.rest.RestClient;
import uk.gov.ons.ctp.response.casesvc.config.AppConfig;
import uk.gov.ons.ctp.response.casesvc.config.InternetAccessCodeSvc;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/**
 * A test of the case frame service client service
 */
@RunWith(MockitoJUnitRunner.class)
public class InternetAccessCodeSvcClientServiceImplTest {

  @Mock
  private Tracer tracer;
  @Mock
  private Span span;

  @Mock
  private AppConfig appConfig;

  @Spy
  private RestClient restClient = new RestClient();

  @InjectMocks
  private InternetAccessCodeSvcClientServiceImpl iacSvcClientService;

  /**
   * Set up Mockito for tests
   */
  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    Mockito.when(tracer.getCurrentSpan()).thenReturn(span);
    Mockito.when(tracer.createSpan(any(String.class))).thenReturn(span);
    restClient.setTracer(tracer);
  }

  /**
   * Guess what? - a test!
   */
  @Test
  public void testCreateCodes() {
    InternetAccessCodeSvc iacSvcConfig = new InternetAccessCodeSvc();
    iacSvcConfig.setIacPostPath("/iacs");
    Mockito.when(appConfig.getInternetAccessCodeSvc()).thenReturn(iacSvcConfig);
    RestTemplate restTemplate = this.restClient.getRestTemplate();

    MockRestServiceServer mockServer = MockRestServiceServer.createServer(restTemplate);
    mockServer.expect(requestTo("http://localhost:8080/iacs"))
        .andExpect(method(HttpMethod.POST))
        .andExpect(content().string(containsString("\"count\":" + 3 + ",")))
        .andExpect(content().string(containsString("\"createdBy\":" + "\"SYSTEM\"")))
        .andRespond(withSuccess("[1,2,3]", MediaType.APPLICATION_JSON));

    List<String> codes = iacSvcClientService.generateIACs(3);
    assertTrue(codes != null);
    assertTrue(codes.containsAll(Arrays.asList(new String[] {"1", "2", "3"})));
    mockServer.verify();
  }

}