import java.util.ArrayList;

/**
 * A three-dimensional index for fast lookup of property records.
 * The three axes are: year last digit (0-9), month (1-12), and town code (0-based).
 * Queries spanning multiple towns are handled by the caller, which calls queryIndex() once per town and merges the results.
 */
public class Index {
    private final int yearSize;
    private final int monthSize;
    private final int locationSize;

    /**
     * 3D array of row-index lists, keyed by [yearDigit][mIdx][townCode].
     * Buckets remain null until the first record is inserted.
     */
    private ArrayList<Integer>[][][] buckets;

    /**
     * Allocates the index array for the given dimension sizes.
     * All buckets start null and are populated on demand in addValue().
     *
     * @param yearSize     Number of distinct year-digit values (typically 10, for digits 0-9)
     * @param monthSize    Number of months (typically 12)
     * @param locationSize Number of distinct town codes
     */
    @SuppressWarnings("unchecked")
    public Index(int yearSize, int monthSize, int locationSize) {
        this.yearSize     = yearSize;
        this.monthSize    = monthSize;
        this.locationSize = locationSize;

        this.buckets = new ArrayList[yearSize][monthSize][locationSize];
    }

    /**
     * Returns the total number of indexed records across all buckets.
     */
    public int size() {
        int total = 0;
        for (int y = 0; y < yearSize; y++)
            for (int m = 0; m < monthSize; m++)
                for (int l = 0; l < locationSize; l++)
                    if (buckets[y][m][l] != null)
                        total += buckets[y][m][l].size();
        return total;
    }

    /**
     * Inserts a record into the appropriate bucket
     *
     * @param year   Last digit of the year (0-9)
     * @param month  Month number (1-12)
     * @param location 0-based town index; records with unknown towns are skipped
     * @param rowIdx Row index of the record in the column store
     */
    public void addValue(int year, int month, int location, int rowIdx) {
        if (location < 0 || location >= locationSize) return;

        int mIdx = month - 1; // convert to 0-based

        if (buckets[year][mIdx][location] == null)
            buckets[year][mIdx][location] = new ArrayList<>();

        buckets[year][mIdx][location].add(rowIdx);
    }

    /**
     * Returns a copy of the row indices in the specified bucket, or an empty
     * list if no records exist for that combination. The returned list may be
     * modified freely without affecting the index.
     *
     * Note: because only the year's last digit is indexed, callers must still
     * verify col_year[i] == targetYear for each returned index.
     *
     * @param year     Last digit of the year (0-9)
     * @param month    Month number (1-12)
     * @param location Town code
     * @return List of row indices matching the given bucket
     */
    public ArrayList<Integer> queryIndex(int year, int month, int location) {
        int mIdx = month - 1;

        if (buckets[year][mIdx][location] == null)
            return new ArrayList<>();

        return new ArrayList<>(buckets[year][mIdx][location]);
    }
}