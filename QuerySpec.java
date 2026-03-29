import java.util.ArrayList;
import java.util.Objects;

/**
 * Holds the invariant query parameters derived from a matriculation number.
 * The variable parameters (x, y) are iterated externally in Main and passed
 * into each query call, so they are NOT stored here.
 *
 * Invariant parameters:
 *   - targetYear    : 4-digit year string derived from last digit of matric number
 *   - startMonth    : commencing month (1-12) derived from second-last digit (0 -> 10)
 *   - targetTowns   : all towns corresponding to digits appearing in the matric number
 */
public class QuerySpec {
    public int targetYear;
    public int startMonth;
    public ArrayList<String> targetTowns;

    /**
     * @param targetYear  Full 4-digit year (e.g. 2017)
     * @param startMonth  Commencing month 1-12 (second-last digit of matric, 0 -> October)
     * @param targetTowns All towns mapped from digits in the matric number
     */
    public QuerySpec(int targetYear, int startMonth, ArrayList<String> targetTowns) {
        this.targetYear   = targetYear;
        this.startMonth   = startMonth;
        this.targetTowns  = targetTowns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QuerySpec)) return false;
        QuerySpec that = (QuerySpec) o;
        return targetYear == that.targetYear
                && startMonth == that.startMonth
                && Objects.equals(targetTowns, that.targetTowns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetYear, startMonth, targetTowns);
    }

    @Override
    public String toString() {
        return String.format("QuerySpec{year=%d, startMonth=%d, towns=%s}",
                targetYear, startMonth, targetTowns);
    }
}
