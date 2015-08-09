package wa.timeseries.joda;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import wa.timeseries.core.TimeSeriesID;
import wa.timeseries.core.persistence.InMemoryTimeSeriesPersistenceHandler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class TimeSeriesHandlerJodaWrapperTest {

    private InMemoryTimeSeriesPersistenceHandler<Integer> persistenceHandler;

    private TimeSeriesID TS_ID = new TimeSeriesID("ts", "1");

    @Before
    public void before() {
        persistenceHandler = new InMemoryTimeSeriesPersistenceHandler<>();
    }

    @Test
    public void testAdd() {

        DateTime startDate = new DateTime().minusYears(1);

        TimeSeriesHandlerJodaWrapper<Integer>
                ts = new TimeSeriesHandlerJodaWrapper<>(
                Duration.millis(1000), startDate, persistenceHandler,
                DateTimeZone.getDefault());

        ts.add(TS_ID, startDate.plusSeconds(1), 1);
    }


    @Test
    public void testLargeSeriesQueryNotFromBeginning() {
        DateTime startDate = new DateTime().minusYears(1);

        List<JodaDateTimeValue<Integer>> batch =
                createBatch(10000, 1, startDate);
        TimeSeriesHandlerJodaWrapper<Integer>
                ts = new TimeSeriesHandlerJodaWrapper<>(10,
                Duration.millis(1000),
                startDate, persistenceHandler, DateTimeZone.getDefault());
        ts.batchAdd(TS_ID, batch);

        assertEquals(1000, persistenceHandler.getPersistCount());

        Iterator<JodaDateTimeValue<Integer>> ite =
                ts.get(TS_ID, startDate.plusSeconds(1000), startDate.plusSeconds(20000));

        assertTrue(ite.hasNext());

        int i = 1000;
        while(ite.hasNext())
        {
            JodaDateTimeValue<Integer> tsValue = ite.next();
            JodaDateTimeValue<Integer> oriValue = batch.get(i++);
            assertEquals(oriValue, tsValue);
        }
    }

    @Test
    public void testLargeSeries() {
        DateTime startDate = new DateTime(0); //new LocalDateTime().minusYears(1);

        List<JodaDateTimeValue<Integer>> batch =
                createBatch(10000, 1, startDate);
        TimeSeriesHandlerJodaWrapper<Integer>
                ts = new TimeSeriesHandlerJodaWrapper<>(10,
                Duration.millis(1000),
                startDate, persistenceHandler, DateTimeZone.getDefault());
        ts.batchAdd(TS_ID, batch);

        assertEquals(1000, persistenceHandler.getPersistCount());

        Iterator<JodaDateTimeValue<Integer>> ite =
                ts.get(TS_ID, startDate, startDate.plusSeconds(20000));

        assertTrue(ite.hasNext());

        int i = 0;
        while(ite.hasNext())
        {
            JodaDateTimeValue<Integer> tsValue = ite.next();
            JodaDateTimeValue<Integer> oriValue = batch.get(i++);
            assertEquals(oriValue, tsValue);
        }
    }

    private List<JodaDateTimeValue<Integer>> createBatch(int size,
            int resolution,
            DateTime startDate) {
        List<JodaDateTimeValue<Integer>> batch = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            batch.add(new JodaDateTimeValue<>(startDate.plusSeconds(i*resolution),
                    (i * resolution)));
        }
        return batch;
    }

}