package wa.timeseries.joda;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import wa.timeseries.core.TimeSeriesID;
import wa.timeseries.core.persistence.InMemoryTimeSeriesPersistenceHandler;

/**
 * Created by wagner on 08/12/15.
 */
public class BugTest {

    private InMemoryTimeSeriesPersistenceHandler<Integer> persistenceHandler;

    private TimeSeriesID TS_ID = new TimeSeriesID("ts", "1");

    @Before
    public void before() {
        persistenceHandler = new InMemoryTimeSeriesPersistenceHandler<>();
    }

    @Test
    public void testAdd() {

        DateTime startDate = new DateTime(0);

        TimeSeriesHandlerJodaWrapper<Integer>
                ts = new TimeSeriesHandlerJodaWrapper<>(
                120, Duration.standardDays(1), new DateTime(0), persistenceHandler, DateTimeZone.UTC);

        ts.add(TS_ID, startDate.plusDays(1), 1);
        ts.add(TS_ID, startDate.plusDays(119), 119);
        ts.add(TS_ID, startDate.plusDays(120), 120);
    }

    public void test() {

    }
}
