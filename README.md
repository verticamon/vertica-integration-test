Vertica integration test
========================

Support for integration tests that run against real Vertica database.

The library will make sure there is one schema per one test method run. It creates user and schema for each test method run.

How to write a test with Spring
===============================

Create class that will provide connection credentials to Vertica server.

    public class TestVerticaTestContext extends VerticaTestContext {

        @Override
        public String getApplicationName() {
            return "vertica_integration_test";
        }

        @Override
        public Credentials getDbAdminCredentials() {
            Credentials credentials = new Credentials();
            credentials.setHost("TODO: your server");
            credentials.setPort("5433");
            credentials.setDatabase("TODO: your database");
            credentials.setUsername("TODO: admin username");
            credentials.setPassword("TODO: admin password");

            return credentials;
        }

    }

Create test context located in test/resources/test-context.xml file.

    <?xml version="1.0" encoding="UTF-8"?>
         <beans xmlns="http://www.springframework.org/schema/beans"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:context="http://www.springframework.org/schema/context"
                xmlns:aop="http://www.springframework.org/schema/aop"
                xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd http://www.springframework.org/schema/aop http://www.springframework.org/schema/aop/spring-aop.xsd http://www.springframework.org/schema/context http://www.springframework.org/schema/context/spring-context-2.5.xsd">

        <bean id="verticaTestContext" class="com.mycompany.vertica.TestVerticaTestContext" init-method="setup" destroy-method="cleanup" scope="prototype"/>
    </beans>

Create test, e.g. in Spock.

    import org.springframework.beans.factory.annotation.Autowired
    import org.springframework.test.annotation.DirtiesContext
    import org.springframework.test.context.ContextConfiguration

    @ContextConfiguration(locations = ['classpath:test-context.xml'])
    @DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
    class VerticaIntSpec extends Specification {

        @Autowired
        TransformerVerticaTestContext context

        void setup() {
        }

        void insertsRowIntoTable() {
            when:
            context.execute("CREATE TABLE test_table (column_name INT)")
            context.execute("INSERT INTO test_table VALUES (1)")

            then:
            ResultSet result = context.createStatement().executeQuery("SELECT * FROM test_table")

            result.next()
        }
    }