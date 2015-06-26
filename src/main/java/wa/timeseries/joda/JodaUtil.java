package wa.timeseries.joda;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;

class JodaUtil
{
  static DateTime convertToDate(long offset, DateTimeZone zone)
  {
    return new DateTime(offset, DateTimeZone.UTC).toDateTime(zone);
  }

  static long convertToOffset(DateTime date)
  {
    if (date == null)
    {
      throw new RuntimeException("Null date");
    }
    return date.toDateTime(DateTimeZone.UTC).getMillis();
  }
}
