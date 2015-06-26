package wa.timeseries.joda;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import wa.timeseries.core.DateValue;

public class JodaDateTimeValue<T> extends DateValue<T> {

    private final DateTimeZone zone;

    public JodaDateTimeValue(DateTime date, T value) {
        super(JodaUtil.convertToOffset(date), value);
        zone = date.getZone();
    }

    JodaDateTimeValue(long date, T value, DateTimeZone zone) {
        super(date, value);
        this.zone = zone;
    }


    public DateTime getDateTime() {
        return JodaUtil.convertToDate(getDate(), zone);
    }
}