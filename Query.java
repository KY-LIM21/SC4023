import java.util.ArrayList;
import java.util.Objects;

/**
 * Holds the fixed query parameters derived from a matriculation number.
 * The variable parameters (x, y) are iterated externally in Main and supplied
 * to each query call, so they are not stored here.
 *
 * Fixed parameters:
 *   - queryYear   : 4-digit year derived from the last digit of the matric number
 *   - startMonth  : starting month (1-12); second-last digit of matric, with 0 mapping to October
 *   - targetTowns : all towns corresponding to digits found in the matric number
 */
public class Query {
    public int queryYear;
    public int startMonth;
    public ArrayList<String> targetTowns;

    /**
     * @param queryYear   Full 4-digit year (e.g. 2017)
     * @param startMonth  Starting month 1-12 (second-last digit of matric; 0 maps to October)
     * @param targetTowns Towns mapped from all distinct digits in the matric number
     */
    public Query(int queryYear, int startMonth, ArrayList<String> targetTowns) {
        this.queryYear   = queryYear;
        this.startMonth  = startMonth;
        this.targetTowns = targetTowns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Query)) return false;
        Query that = (Query) o;
        return queryYear == that.queryYear
                && startMonth == that.startMonth
                && Objects.equals(targetTowns, that.targetTowns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryYear, startMonth, targetTowns);
    }

    @Override
    public String toString() {
        return String.format("QuerySpec{year=%d, startMonth=%d, towns=%s}",
                queryYear, startMonth, targetTowns);
    }
}