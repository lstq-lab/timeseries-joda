package wa.timeseries.joda;

import com.google.common.base.Optional;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.LocalDateTime;
import wa.timeseries.core.DateValue;
import wa.timeseries.core.TimeSeries;
import wa.timeseries.core.persistence.TimeSeriesPersistenceHandler;

import java.util.Collection;
import java.util.Iterator;

import static wa.timeseries.joda.JodaUtil.convertToOffset;

/**
 * Provides a JodaWrapper for TimeSeries
 * @param <T>
 */
public class TimeSeriesJodaWrapper<T> {

    private final TimeSeries<T> timeSeries;

    private final DateTimeZone zone;

    public TimeSeriesJodaWrapper(Duration maxResolution,
            DateTime startDate,
            TimeSeriesPersistenceHandler persistenceHandler,
            DateTimeZone zone) {
        this(256, maxResolution, startDate, persistenceHandler, zone);
    }

    public TimeSeriesJodaWrapper(int sliceSize, Duration maxResolution,
            DateTime startDate,
            TimeSeriesPersistenceHandler persistenceHandler,
            DateTimeZone zone) {
        timeSeries = new TimeSeries<>(sliceSize, (int)maxResolution.getMillis(),
                convertToOffset(startDate),
                persistenceHandler);
        this.zone = zone;
    }

    public void add(DateTime date, T value) {
        timeSeries.add(convertToOffset(date), value);
    }

    public void batchAdd(Collection<JodaDateTimeValue<T>> values) {
        timeSeries.batchAdd(values);
    }

    public Optional<T> get(DateTime date) {
        return timeSeries.get(convertToOffset(date));
    }

    public Iterator<JodaDateTimeValue<T>> get(DateTime qStartDate,
            DateTime qEndDate) {
        return convertToJodaDateValue(
                timeSeries.get(convertToOffset(qStartDate),
                        convertToOffset(qEndDate)));
    }

    private Iterator<JodaDateTimeValue<T>> convertToJodaDateValue(
            Iterator<DateValue<T>> iterator) {
        return new JodaDateValueIterator(iterator);
    }

    private class JodaDateValueIterator implements Iterator<JodaDateTimeValue<T>> {

        private final Iterator<DateValue<T>> iterator;

        private JodaDateValueIterator(Iterator<DateValue<T>> iterator) {
            this.iterator = iterator;
        }

        @Override public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override public JodaDateTimeValue<T> next() {
            DateValue<T> value = iterator.next();
            return new JodaDateTimeValue<>(value.getDate(),
                    value.getValue(), zone);
        }

        @Override public void remove() {
            iterator.remove();
        }
    }

}
