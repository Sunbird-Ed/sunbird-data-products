import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.SparkSpec
class TestEsUtil extends SparkSpec(null) with MockFactory {
  "CourseUtils" should "execute and give live courses from compositesearch" in {
    new MockServerClient("localhost", 1080)
      .when(
        request()
          .withMethod("POST")
          .withPath("/login")
          .withBody("{username: 'foo', password: 'bar'}")
      )
      .respond(
        response()
          .withStatusCode(302)
          .withCookie(
            "sessionId", "2By8LOhBmaW5nZXJwcmludCIlMDAzMW"
          )
          .withHeader(
            "Location", "https://www.mock-server.com"
          )
      );
  }

}