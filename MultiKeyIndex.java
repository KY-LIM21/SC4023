import java.util.ArrayList;

/**
 * Multi-dimensional index for efficient querying of property records.
 * Dimensions: year (last digit), month, location code.
 *
 * Multi-town queries are handled by the caller, which invokes queryIndex()
 * once per town and unions the results.
 */
public class MultiKeyIndex {
    private final int yearDimension;
    private final int monthDimension;
    private final int locationDimension;

    private ArrayList<Integer>[][][] indexStructure;

    /**
     * Constructor to initialize the index with specified dimensions.
     */
    @SuppressWarnings("unchecked")
    public MultiKeyIndex(int yearDim, int monthDim, int locationDim) {
        this.yearDimension     = yearDim;
        this.monthDimension    = monthDim;
        this.locationDimension = locationDim;

        this.indexStructure = new ArrayList[yearDim][monthDim][locationDim];
    }

    /**
     * Returns the total number of indexed records.
     */
    public int size() {
        int totalEntries = 0;
        for (int y = 0; y < yearDimension; y++)
            for (int m = 0; m < monthDimension; m++)
                for (int l = 0; l < locationDimension; l++)
                    if (indexStructure[y][m][l] != null)
                        totalEntries += indexStructure[y][m][l].size();
        return totalEntries;
    }

    /**
     * Adds a record to the index.
     *
     * @param year        Last digit of the year (0-9)
     * @param month       Month number (1-12)
     * @param location    Location code (0-based index into the towns array)
     * @param recordIndex Row index of the record in the column store
     */
    public void addValue(int year, int month, int location, int recordIndex) {
        if (location < 0 || location >= locationDimension) return;

        int monthIndex = month - 1; // convert to 0-based

        if (indexStructure[year][monthIndex][location] == null)
            indexStructure[year][monthIndex][location] = new ArrayList<>();

        indexStructure[year][monthIndex][location].add(recordIndex);
    }

    /**
     * Queries the index for records matching the specified criteria.
     *
     * @param year     Last digit of the year (0-9)
     * @param month    Month number (1-12)
     * @param location Location code
     * @return List of row indices matching the criteria
     */
    public ArrayList<Integer> queryIndex(int year, int month, int location) {
        int monthIndex = month - 1;

        if (indexStructure[year][monthIndex][location] == null)
            return new ArrayList<>();

        return new ArrayList<>(indexStructure[year][monthIndex][location]);
    }
}
