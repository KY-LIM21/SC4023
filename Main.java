import java.util.ArrayList;
import java.util.Map;

/**
 * Entry point for the HDB Resale Property Analysis System.
 *
 * Workflow:
 *   1. Parse the matriculation number into a QuerySpec (fixed year, startMonth, towns).
 *   2. Build the column store and all auxiliary structures (index, zone map, etc.).
 *   3. Iterate over all (x, y) pairs: x in [1,8], y in [80,150].
 *   4. For each pair, find the record with the minimum price/sqm that is <= 4725.
 *   5. Write results to ScanResult_<MatricNum>.csv, ordered by x then y.
 *
 * The shared-scan method pre-computes results for all (x,y) pairs in one pass
 * and is used as the default query strategy for output.  The other three methods
 * (normal, index, compressed) are also demonstrated for comparison.
 */
public class Main {

    // Change this to the chosen group member's matriculation number.
    private static final String MATRIC_NUMBER = "A6626226B";

    public static void main(String[] args) {
        System.out.println("=======================================================");
        System.out.println("         HDB RESALE PROPERTY ANALYSIS SYSTEM           ");
        System.out.println("=======================================================");

        // ------------------------------------------------------------------
        // 1. Parse query specification from matriculation number
        // ------------------------------------------------------------------
        System.out.println("\n--- Parsing query specification ---");
        QuerySpec spec = PropertyDataStore.buildQuerySpec(MATRIC_NUMBER);

        // ------------------------------------------------------------------
        // 2. Initialise column store
        // ------------------------------------------------------------------
        System.out.println("\n--- Loading data ---");
        PropertyDataStore db = new PropertyDataStore();

        // ------------------------------------------------------------------
        // 3. Build auxiliary structures
        // ------------------------------------------------------------------
        System.out.println("\n--- Building auxiliary structures ---");
        System.out.println("Compressing town and date data...");
        db.compressTownDate();
        System.out.println("Sorting by compressed data...");
        db.sortByCompressedData();
        System.out.println("Building MultiKeyIndex...");
        db.buildIndex();
        System.out.println("Creating ZoneMap...");
        db.createZoneMap();

        // ------------------------------------------------------------------
        // 4. Shared scan — compute all (x, y) results in one pass
        // ------------------------------------------------------------------
        System.out.println("\n--- Running shared scan (all x, y pairs) ---");
        long t0 = System.nanoTime();
        Map<Integer, ArrayList<Integer>> sharedResults = db.sharedScanQueryDB(spec);
        long t1 = System.nanoTime();
        System.out.printf("Shared scan completed in %,d ns%n", (t1 - t0));

        // ------------------------------------------------------------------
        // 5. Write output file
        // ------------------------------------------------------------------
        String outputFile = "ScanResult_" + MATRIC_NUMBER;
        System.out.println("\n--- Writing output: " + outputFile + ".csv ---");
        db.createOutputFile(outputFile);

        int validCount = 0;
        int noResultCount = 0;

        // Results must be ordered: x ascending, then y ascending
        for (int x = 1; x <= 8; x++) {
            for (int y = 80; y <= 150; y++) {
                ArrayList<Integer> posArray = sharedResults.get(x * 1000 + y);
                PropertyDataStore.QueryResult result = db.findMinPricePerSqm(posArray, x, y);
                db.writeResult(x, y, result);
                if (result != null) validCount++; else noResultCount++;
            }
        }

        db.closeOutputFile();
        System.out.printf("Output written: %d valid pairs, %d no-result pairs.%n",
                validCount, noResultCount);

        // ------------------------------------------------------------------
        // 6. Optional: benchmark comparison of all four query methods
        //    Uses (x=3, y=80) as a representative sample query.
        // ------------------------------------------------------------------
        System.out.println("\n=======================================================");
        System.out.println("   BENCHMARK: four query methods for (x=3, y=80)");
        System.out.println("=======================================================");

        int sampleX = 3, sampleY = 80;

        // Normal (sequential scan)
        long s = System.nanoTime();
        ArrayList<Integer> normalRes = db.queryDB(spec, sampleX, sampleY);
        long e = System.nanoTime();
        System.out.printf("Normal scan     : %5d records  %,12d ns%n",
                normalRes.size(), (e - s));

        // Index-based
        s = System.nanoTime();
        ArrayList<Integer> indexRes = db.queryDBIndex(spec, sampleX, sampleY);
        e = System.nanoTime();
        System.out.printf("Index scan      : %5d records  %,12d ns%n",
                indexRes.size(), (e - s));

        // Compressed + ZoneMap + Sorted
        s = System.nanoTime();
        ArrayList<Integer> compRes = db.queryCompressedDB(spec, sampleX, sampleY);
        e = System.nanoTime();
        System.out.printf("Compressed scan : %5d records  %,12d ns%n",
                compRes.size(), (e - s));

        // Shared scan (already computed above — just retrieve from map)
        s = System.nanoTime();
        ArrayList<Integer> sharedSample = sharedResults.get(sampleX * 1000 + sampleY);
        e = System.nanoTime();
        System.out.printf("Shared scan*    : %5d records  %,12d ns  (* pre-computed)%n",
                sharedSample.size(), (e - s));

        System.out.println("\n=======================================================");
        System.out.println("                  Processing complete                  ");
        System.out.println("=======================================================");
    }
}
