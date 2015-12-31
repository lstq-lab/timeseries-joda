package wa.timeseries.joda;

import com.google.common.base.Optional;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import wa.timeseries.core.DateValue;
import wa.timeseries.core.TimeSeries;
import wa.timeseries.core.TimeSeriesHandler;
import wa.timeseries.core.TimeSeriesID;
import wa.timeseries.core.persistence.TimeSeriesPersistenceHandler;

import java.util.Collection;
import java.util.Iterator;

import static wa.timeseries.joda.JodaUtil.convertToOffset;

/**
 * Provides a JodaWrapper for TimeSeriesHandler
 * @param <T>
 */
public class TimeSeriesHandlerJodaWrapper<T> {

    private final TimeSeriesHandler<T> timeSeriesHandler;

    private final DateTimeZone zone;

    public TimeSeriesHandlerJodaWrapper(Duration maxResolution,
            DateTime startDate,
            TimeSeriesPersistenceHandler persistenceHandler,
            DateTimeZone zone) {
        this(256, maxResolution, startDate, persistenceHandler, zone);
    }

    public TimeSeriesHandlerJodaWrapper(int sliceSize, Duration maxResolution,
            DateTime startDate,
            TimeSeriesPersistenceHandler persistenceHandler,
            DateTimeZone zone) {
        timeSeriesHandler = new TimeSeriesHandler<>(sliceSize, (int)maxResolution.getMillis(),
                convertToOffset(startDate),
                persistenceHandler);
        this.zone = zone;
    }

    public void add(TimeSeriesID tsId, DateTime date, T value) {
        timeSeriesHandler.add(tsId, convertToOffset(date), value);
    }

    public void batchAdd(TimeSeriesID tsId, Collection<JodaDateTimeValue<T>> values) {
        timeSeriesHandler.batchAdd(tsId, values);
    }

    public Optional<T> get(TimeSeriesID tsId, DateTime date) {
        return timeSeriesHandler.get(tsId, convertToOffset(date));
    }

    public Iterator<JodaDateTimeValue<T>> get(TimeSeriesID tsId, DateTime qStartDate,
            DateTime qEndDate) {
        return convertToJodaDateValue(
                timeSeriesHandler.get(tsId, convertToOffset(qStartDate),
                        convertToOffset(qEndDate)));
    }

    public Iterator<TimeSeriesJodaWrapper<T>> getUpdates(String family, DateTime date) {
        return convertToWrapper(
                timeSeriesHandler.getUpdates(family, convertToOffset(date)));
    }

    private Iterator<TimeSeriesJodaWrapper<T>> convertToWrapper(
            final Iterator<TimeSeries<T>> iterator) {

        return new Iterator<TimeSeriesJodaWrapper<T>>() {
            @Override public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override public TimeSeriesJodaWrapper<T> next() {
                return new TimeSeriesJodaWrapper<>(iterator.next(), zone);
            }

            @Override public void remove() {
                iterator.remove();
            }
        };
    }

    private Iterator<JodaDateTimeValue<T>> convertToJodaDateValue(
            Iterator<DateValue<T>> iterator) {
        return new JodaDateValueIterator(iterator);
    }

    public Optional<TimeSeriesJodaWrapper<T>> get(TimeSeriesID tsid) {
        Optional<TimeSeries<T>> series = timeSeriesHandler.get(tsid);
        if (series.isPresent()) {
            return Optional.of(new TimeSeriesJodaWrapper<>(series.get(), zone));
        }
        return Optional.absent();
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

    public TimeSeriesHandler<T> getInternalHandler() {
        return timeSeriesHandler;
    }
}
