import java.util.ArrayList;

/**
 * Spatial partitioning system for efficient range queries on encoded data.
 * Works similarly to a zone map, dividing sorted encoded data into contiguous
 * partitions. Each partition stores its maximum encoded value and its end index.
 *
 * Used by queryCompressedDB() in PropertyDataStore to prune the scan range
 * before doing fine-grained sequential verification. For multi-town queries,
 * the caller invokes getZone() once per town and unions results.
 */
public class ZoneMap {
    private ArrayList<Short> partitionMaxValues;
    private ArrayList<Integer> partitionEndIndices;

    /**
     * Constructor initializes partition lists.
     */
    public ZoneMap() {
        partitionMaxValues   = new ArrayList<>();
        partitionEndIndices  = new ArrayList<>();
    }

    /**
     * Adds a partition with its maximum encoded value and its end index.
     */
    public void addZone(short maxValue, int endIndex) {
        partitionMaxValues.add(maxValue);
        partitionEndIndices.add(endIndex);
    }

    /**
     * Displays partition information for debugging.
     */
    public void printZones() {
        System.out.println("Zone Largest Arr:");
        for (int i = 0; i < partitionMaxValues.size(); i++)
            System.out.printf("%d,", partitionMaxValues.get(i));
        System.out.println();
    }

    /**
     * Gets physical index boundaries for a value range query.
     * Returns [startPartitionLower, startPartitionUpper,
     *          endPartitionLower,   endPartitionUpper].
     */
    public int[] getZone(short startValue, short endValue) {
        int[] boundaries = new int[]{-1, -1, -1, -1};

        int startPartition = findZone(0, partitionMaxValues.size() - 1, startValue);
        int endPartition   = findZone(startPartition, partitionMaxValues.size() - 1, endValue);

        boundaries[0] = (startPartition > 0)
                ? partitionEndIndices.get(startPartition - 1) + 1 : 0;
        boundaries[1] = partitionEndIndices.get(startPartition);

        boundaries[2] = (endPartition > 0)
                ? partitionEndIndices.get(endPartition - 1) + 1 : 0;
        boundaries[3] = partitionEndIndices.get(endPartition);

        return boundaries;
    }

    /**
     * Finds the partition whose maximum value is >= searchValue.
     * Uses linear search for small ranges and binary search for larger ones.
     */
    private int findZone(int startIndex, int endIndex, short searchValue) {
        int current = startIndex;
        int last    = endIndex;

        while (true) {
            int rangeSize = last - current;

            if (rangeSize <= 5) {
                for (int i = current; i <= endIndex; i++)
                    if (searchValue <= partitionMaxValues.get(i))
                        return i;
                return current;
            } else {
                int mid = current + (rangeSize / 2);
                if (searchValue < partitionEndIndices.get(mid)) {
                    if (searchValue > partitionEndIndices.get(mid - 1))
                        return mid;
                    last = mid;
                } else {
                    current = mid;
                }
            }
        }
    }
}
