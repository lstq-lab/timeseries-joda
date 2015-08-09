package wa.timeseries.joda;

import org.joda.time.DateTimeZone;
import wa.timeseries.core.DateValue;
import wa.timeseries.core.TimeSeries;
import wa.timeseries.core.TimeSeriesID;

public class TimeSeriesJodaWrapper<T> {

    private final TimeSeries<T> timeSeries;

    private final DateTimeZone zone;

    TimeSeriesJodaWrapper(TimeSeries<T> timeSeries, DateTimeZone zone) {
        this.timeSeries = timeSeries;
        this.zone = zone;
    }

    public DateValue<T> getLastValue() {
        return new JodaDateTimeValue(timeSeries.getLastValue().getDate(),
                timeSeries.getLastValue().getValue(), zone);
    }

    public TimeSeriesID getId() {
        return timeSeries.getId();
    }


}
