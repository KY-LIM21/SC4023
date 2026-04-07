import java.util.ArrayList;
import java.util.Map;

/**
 * Entry point for the HDB Resale Property Analysis System.
 *
 * Workflow:
 *   Phase 1 — Column file build (first run only):
 *     ColumnStoreWriter reads ResalePricesSingapore.csv and writes one CSV
 *     file per column under the columns/ directory. A completion flag is written
 *     on success. Subsequent runs detect this flag and skip the build phase.
 *
 *   Phase 2 — Load and index:
 *     PropertyDataStore reads the column files into typed primitive arrays and
 *     constructs auxiliary structures (encoding, sort, MultiKeyIndex, ZoneMap).
 *
 *   Phase 3 — Query and output:
 *     A shared scan pre-computes results for all (x, y) pairs in one pass.
 *     Results are written to ScanResult_<MatricNum>.csv ordered by x then y.
 *
 *   Phase 4 — Benchmark (optional):
 *     All four query methods are timed on a sample (x=3, y=80) for comparison.
 *
 * Timing notes:
 *   - Phase 1 time reflects CSV parsing and column file writing on the first run,
 *     vs. a near-zero flag existence check on subsequent runs.
 *   - Phase 2 load time reflects reading from the pre-built column files.
 *   - Comparing Phase 1 + Phase 2 across runs illustrates the benefit of
 *     persistent column storage.
 */
public class Main {
    private static final String MATRIC_NUMBER = "U2322225H";
    private static final String INPUT_CSV = "ResalePricesSingapore.csv";
    private static final String COL_DIR   = "columns/";

    public static void main(String[] args) {
        System.out.println("=======================================================");
        System.out.println("         HDB RESALE PROPERTY ANALYSIS SYSTEM           ");
        System.out.println("=======================================================");

        System.out.println("\n--- Phase 1: Column store build ---");
        ColumnStoreBuild writer = new ColumnStoreBuild(INPUT_CSV, COL_DIR);
        boolean alreadyBuilt = writer.isReady();

        long phase1Start = System.nanoTime();
        if (alreadyBuilt) {
            System.out.println("Column files already exist — skipping CSV parse.");
        } else {
            System.out.println("Column files not found — building from CSV...");
            writer.run();
            System.out.println("Column files written successfully.");
        }
        long phase1End = System.nanoTime();

        System.out.printf("Phase 1 time : %,d ns (%.3f s)  [%s]%n",
                (phase1End - phase1Start),
                (phase1End - phase1Start) / 1_000_000_000.0,
                alreadyBuilt ? "skipped — column files already present"
                        : "CSV parsed and column files written");

        System.out.println("\n--- Phase 2: Load and index ---");

        System.out.println("Parsing query specification...");
        Query spec = DataStore.parseMatric(MATRIC_NUMBER);

        System.out.println("\nLoading column files...");
        long loadStart = System.nanoTime();
        DataStore db = new DataStore(COL_DIR);
        long loadEnd = System.nanoTime();
        System.out.printf("Column file load time         : %,d ns (%.3f s)%n",
                (loadEnd - loadStart),
                (loadEnd - loadStart) / 1_000_000_000.0);

        System.out.println("\nBuilding auxiliary structures...");
        long indexStart = System.nanoTime();

        System.out.println("  Encoding town and date data...");
        db.encodeRecords();
        System.out.println("  Sorting by encoded values...");
        db.sortEncoded();
        System.out.println("  Building MultiKeyIndex...");
        db.buildKeyIndex();
        System.out.println("  Constructing ZoneMap...");
        db.buildZoneMap();

        long indexEnd = System.nanoTime();
        System.out.printf("Auxiliary structure build time : %,d ns (%.3f s)%n",
                (indexEnd - indexStart),
                (indexEnd - indexStart) / 1_000_000_000.0);

        System.out.printf("Phase 2 total time            : %,d ns (%.3f s)%n",
                (indexEnd - loadStart),
                (indexEnd - loadStart) / 1_000_000_000.0);

        System.out.println("\n-------------------------------------------------------");
        System.out.printf("TOTAL startup time (Phase 1 + Phase 2) : %,d ns (%.3f s)%n",
                (phase1End - phase1Start) + (indexEnd - loadStart),
                ((phase1End - phase1Start) + (indexEnd - loadStart)) / 1_000_000_000.0);
        System.out.println("  (First run:       CSV parse + column write + load + index build)");
        System.out.println("  (Subsequent runs: column file load + index build only)");
        System.out.println("-------------------------------------------------------");

        System.out.println("\n--- Phase 3: Shared scan (all x, y pairs) ---");
        long t0 = System.nanoTime();
        Map<Integer, ArrayList<Integer>> sharedResults = db.sharedScanQueryDB(spec);
        long t1 = System.nanoTime();
        System.out.printf("Shared scan completed in %,d ns (%.3f s)%n",
                (t1 - t0), (t1 - t0) / 1_000_000_000.0);

        String outputFile = "ScanResult_" + MATRIC_NUMBER;
        System.out.println("\nWriting output: " + outputFile + ".csv");
        db.createOutputFile(outputFile);

        int validCount    = 0;
        int noResultCount = 0;

        // Results ordered: x ascending, then y ascending
        for (int x = 1; x <= 8; x++) {
            for (int y = 80; y <= 150; y++) {
                ArrayList<Integer> matchedRows = sharedResults.get(x * 1000 + y);
                DataStore.QueryResult result = db.findMinPricePerSqm(matchedRows, x, y);
                db.writeResult(x, y, result);
                if (result != null) validCount++; else noResultCount++;
            }
        }

        db.closeOutputFile();
        System.out.printf("Output written: %d valid pairs, %d no-result pairs.%n",
                validCount, noResultCount);

        System.out.println("\n=======================================================");
        System.out.println("   BENCHMARK: four query methods for (x=3, y=80)");
        System.out.println("=======================================================");

        int sampleX = 3, sampleY = 80;
        long s, e;

        s = System.nanoTime();
        ArrayList<Integer> normalRes = db.queryDB(spec, sampleX, sampleY);
        e = System.nanoTime();
        System.out.printf("Normal scan     : %5d records  %,12d ns%n", normalRes.size(), (e - s));

        s = System.nanoTime();
        ArrayList<Integer> indexRes = db.queryDBIndex(spec, sampleX, sampleY);
        e = System.nanoTime();
        System.out.printf("Index scan      : %5d records  %,12d ns%n", indexRes.size(), (e - s));

        s = System.nanoTime();
        ArrayList<Integer> compRes = db.queryCompressedDB(spec, sampleX, sampleY);
        e = System.nanoTime();
        System.out.printf("Compressed scan : %5d records  %,12d ns%n", compRes.size(), (e - s));

        s = System.nanoTime();
        ArrayList<Integer> sharedSample = sharedResults.get(sampleX * 1000 + sampleY);
        e = System.nanoTime();
        System.out.printf("Shared scan*    : %5d records  %,12d ns  (* pre-computed)%n",
                sharedSample.size(), (e - s));

        System.out.println("Done");
    }
}