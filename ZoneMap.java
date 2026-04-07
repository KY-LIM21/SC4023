import java.util.ArrayList;

/**
 * Partition-based structure for pruning range scans over sorted encoded data.
 * Divides the sorted encoded array into contiguous partitions, each storing
 * its maximum encoded value and its end index.
 *
 * Used by queryCompressedDB() in PropertyDataStore to narrow the scan window
 * before fine-grained sequential verification. For multi-town queries, the
 * caller invokes queryZone() once per town and merges the results.
 */
public class ZoneMap {
    private ArrayList<Short>   zoneMaxVals;
    private ArrayList<Integer> zoneEndIdx;

    /**
     * Initialises empty partition lists.
     */
    public ZoneMap() {
        zoneMaxVals = new ArrayList<>();
        zoneEndIdx  = new ArrayList<>();
    }

    /**
     * Registers a partition with its maximum encoded value and physical end index.
     */
    public void addPartition(short maxValue, int endIndex) {
        zoneMaxVals.add(maxValue);
        zoneEndIdx.add(endIndex);
    }

    /**
     * Prints partition details for debugging purposes.
     */
    public void printPartitions() {
        System.out.println("Zone Largest Arr:");
        for (int i = 0; i < zoneMaxVals.size(); i++)
            System.out.printf("%d,", zoneMaxVals.get(i));
        System.out.println();
    }

    /**
     * Returns physical index boundaries for an encoded value range query.
     * Result is [loZoneLower, loZoneUpper, hiZoneLower, hiZoneUpper].
     */
    public int[] queryZone(short loEnc, short hiEnc) {
        int[] bounds = new int[]{-1, -1, -1, -1};

        int loZone = locateZone(0, zoneMaxVals.size() - 1, loEnc);
        int hiZone = locateZone(loZone, zoneMaxVals.size() - 1, hiEnc);

        bounds[0] = (loZone > 0) ? zoneEndIdx.get(loZone - 1) + 1 : 0;
        bounds[1] = zoneEndIdx.get(loZone);

        bounds[2] = (hiZone > 0) ? zoneEndIdx.get(hiZone - 1) + 1 : 0;
        bounds[3] = zoneEndIdx.get(hiZone);

        return bounds;
    }

    /**
     * Finds the partition whose maximum value is >= target.
     * Uses linear search for small ranges and binary search for larger ones.
     */
    private int locateZone(int startIndex, int endIndex, short target) {
        int current = startIndex;
        int last    = endIndex;

        while (true) {
            int span = last - current;

            if (span <= 5) {
                for (int i = current; i <= endIndex; i++)
                    if (target <= zoneMaxVals.get(i))
                        return i;
                return current;
            } else {
                int mid = current + (span / 2);
                if (target < zoneEndIdx.get(mid)) {
                    if (target > zoneEndIdx.get(mid - 1))
                        return mid;
                    last = mid;
                } else {
                    current = mid;
                }
            }
        }
    }
}