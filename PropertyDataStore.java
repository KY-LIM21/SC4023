import java.io.*;
import java.util.*;

/**
 * A column-oriented data store for HDB resale transaction records.
 *
 * Stores each attribute as a separate Column (column-store layout).
 * Supports four query strategies that all share the same filter logic:
 *   year == targetYear, month in [startMonth, startMonth+x-1], town in targetTowns, area >= y
 *
 * The variable parameters x (months window, 1-8) and y (area threshold, 80-150)
 * are passed into each query call rather than stored on the object, because the
 * caller (Main) iterates over all (x, y) pairs using the same fixed QuerySpec.
 */
public class PropertyDataStore {

    // -------------------------------------------------------------------------
    // Column indices
    // -------------------------------------------------------------------------
    private static final int COL_DATE        = 0;
    private static final int COL_LOCATION    = 1;
    private static final int COL_FLAT_TYPE   = 2;
    private static final int COL_BLOCK       = 3;
    private static final int COL_STREET      = 4;
    private static final int COL_LEVEL_RANGE = 5;
    private static final int COL_AREA        = 6;
    private static final int COL_MODEL       = 7;
    private static final int COL_LEASE_START = 8;
    private static final int COL_PRICE       = 9;

    // -------------------------------------------------------------------------
    // Column store
    // -------------------------------------------------------------------------
    private ArrayList<Column> columns;

    // -------------------------------------------------------------------------
    // Compression / index structures
    // -------------------------------------------------------------------------
    private ArrayList<Short>   encodedRecords;
    private ArrayList<String>  townCompressList;
    private ArrayList<String>  dateCompressList;
    private MultiKeyIndex      mki;
    private ZoneMap            zoneMap;

    // Year range discovered during load — used to build the date dictionary
    private int smallestYear = 9999;
    private int largestYear  = 0;

    // -------------------------------------------------------------------------
    // Known towns (fixed mapping used for index and compression)
    // -------------------------------------------------------------------------
    private static final String[] TOWN_LIST = {
            "BEDOK", "BUKIT PANJANG", "CLEMENTI", "CHOA CHU KANG", "HOUGANG",
            "JURONG WEST", "PASIR RIS", "TAMPINES", "WOODLANDS", "YISHUN"
    };

    // Month abbreviations matching the 'Jan-15' date format
    private static final String[] MONTH_ABBR = {
            "Jan","Feb","Mar","Apr","May","Jun",
            "Jul","Aug","Sep","Oct","Nov","Dec"
    };

    // Maximum price/sqm for a result to be considered valid
    private static final double MAX_PRICE_PER_SQM = 4725.0;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------
    public PropertyDataStore() {
        columns = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) columns.add(new Column());

        townCompressList = new ArrayList<>();
        dateCompressList = new ArrayList<>();

        try {
            loadDB();
        } catch (IOException e) {
            System.err.println("Failed to load data: " + e.getMessage());
            throw new RuntimeException("Data loading failed", e);
        }
    }

    // =========================================================================
    // QUERY SPEC INITIALISATION
    // =========================================================================

    /**
     * Parses a matriculation number and returns a QuerySpec containing the
     * invariant query parameters (year, startMonth, list of towns).
     *
     * Rules (per assignment brief):
     *   - Last digit of matric number -> target year (via yearMapping[])
     *   - Second-last digit           -> startMonth (0 -> October = 10)
     *   - All digits present          -> towns list (duplicates ignored)
     *
     * The data for 2025 is provided only for query, not as a target year.
     * If the mapping would produce 2025, an exception is thrown.
     *
     * @param matricNumber e.g. "U2120814C"
     */
    public static QuerySpec buildQuerySpec(String matricNumber) {
        // Extract only the digit characters from the matric string
        String digitsOnly = matricNumber.replaceAll("[^0-9]", "");

        if (digitsOnly.length() < 2) {
            throw new IllegalArgumentException(
                    "Matriculation number must contain at least 2 digits: " + matricNumber);
        }

        // --- Target year (last digit) ---
        int[] yearMapping = {2020, 2021, 2022, 2023, 2014, 2015, 2016, 2017, 2018, 2019};
        int lastDigit = Character.getNumericValue(digitsOnly.charAt(digitsOnly.length() - 1));
        int targetYear = yearMapping[lastDigit];

        if (targetYear == 2025) {
            throw new IllegalArgumentException(
                    "2025 is not a valid target year per the assignment brief.");
        }

        // --- Start month (second-last digit, 0 -> October) ---
        int secondLastDigit = Character.getNumericValue(digitsOnly.charAt(digitsOnly.length() - 2));
        int startMonth = (secondLastDigit == 0) ? 10 : secondLastDigit;

        // --- Town list (all distinct digits) ---
        Set<Integer> seenDigits = new LinkedHashSet<>();
        for (char c : digitsOnly.toCharArray()) seenDigits.add(Character.getNumericValue(c));

        ArrayList<String> targetTowns = new ArrayList<>();
        for (int digit : seenDigits) {
            if (digit >= 0 && digit < TOWN_LIST.length) {
                String town = TOWN_LIST[digit];
                if (!targetTowns.contains(town)) targetTowns.add(town);
            }
        }

        System.out.println("Matric: " + matricNumber);
        System.out.println("Target year  : " + targetYear);
        System.out.println("Start month  : " + startMonth);
        System.out.println("Target towns : " + targetTowns);

        return new QuerySpec(targetYear, startMonth, targetTowns);
    }

    // =========================================================================
    // DATA LOADING
    // =========================================================================

    /**
     * Loads ResalePricesSingapore.csv into the column store.
     * Date format expected: 'Jan-15' (MMM-yy).
     */
    private void loadDB() throws IOException {
        String filePath = "ResalePricesSingapore.csv";
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String line;
        int lineCount = 0;

        while ((line = reader.readLine()) != null) {
            lineCount++;
            if (lineCount == 1) continue; // skip header

            String[] fields = line.split(",");
            if (hasEmptyFields(fields)) {
                System.out.println("Warning: skipping row with empty fields: "
                        + Arrays.toString(fields));
                continue;
            }

            for (int i = 0; i < 10; i++) columns.get(i).add(fields[i].trim());

            // Track unique towns
            String location = fields[COL_LOCATION].trim();
            if (!townCompressList.contains(location)) townCompressList.add(location);

            // Track year range for date encoding dictionary
            int year = parseYear(fields[COL_DATE].trim());
            if (year > largestYear)  largestYear  = year;
            if (year < smallestYear) smallestYear = year;
        }
        reader.close();

        System.out.println("Data loaded: " + (lineCount - 1) + " records");

        if (!allColumnsEqualLength()) {
            System.err.println("Error: columns have different lengths — aborting.");
            System.exit(1);
        }
        System.out.println("Column store created successfully.");
    }

    // =========================================================================
    // DATE PARSING HELPERS  (format: 'Jan-15')
    // =========================================================================

    /** 'Jan-15' -> 2015 */
    private static int parseYear(String date) {
        return 2000 + Integer.parseInt(date.split("-")[1]);
    }

    /** 'Jan-15' -> 1 (January) */
    private static int parseMonth(String date) {
        String abbr = date.split("-")[0];
        return Arrays.asList(MONTH_ABBR).indexOf(abbr) + 1; // 1-based
    }

    /** (month 1-12, full year) -> 'Jan-15' */
    private static String formatDate(int month, int year) {
        return String.format("%s-%02d", MONTH_ABBR[month - 1], year % 100);
    }

    // =========================================================================
    // DATA PREPARATION
    // =========================================================================

    /**
     * Encodes each record's (town, date) pair into a single short value.
     * Encoding: townIndex * 1000 + dateIndex
     * Date dictionary covers every month from smallestYear to largestYear.
     */
    public void compressTownDate() {
        if (dateCompressList.isEmpty()) {
            for (int y = smallestYear; y <= largestYear; y++)
                for (int m = 1; m <= 12; m++)
                    dateCompressList.add(formatDate(m, y));
        }

        encodedRecords = new ArrayList<>(columns.get(0).size());
        for (int i = 0; i < columns.get(0).size(); i++) {
            String date     = columns.get(COL_DATE).get(i);
            String location = columns.get(COL_LOCATION).get(i);
            encodedRecords.add(getCompressValue(location, date));
        }
    }

    /** Encodes one (town, date) pair into a short. */
    private short getCompressValue(String town, String dateString) {
        int townIndex = townCompressList.indexOf(town);
        int dateIndex = dateCompressList.indexOf(dateString);
        return (short) (townIndex * 1000 + dateIndex);
    }

    /**
     * Heap-sorts all columns simultaneously by the encoded value.
     * After this, encodedRecords is sorted ascending and all columns stay aligned.
     */
    public void sortByCompressedData() {
        int n = encodedRecords.size() - 1;
        for (int i = (n / 2) - 1; i >= 0; i--) heapify(n, i);
        for (int i = n - 1; i > 0; i--) { swapAll(0, i); heapify(i, 0); }
    }

    private void heapify(int heapSize, int root) {
        int largest   = root;
        int left      = 2 * root + 1;
        int right     = 2 * root + 2;
        short rootVal = encodedRecords.get(largest);

        if (left  < heapSize && encodedRecords.get(left)  > rootVal) { largest = left;  rootVal = encodedRecords.get(left);  }
        if (right < heapSize && encodedRecords.get(right) > rootVal) { largest = right; }

        if (largest != root) { swapAll(root, largest); heapify(heapSize, largest); }
    }

    /** Swaps row i and row j across encodedRecords and all columns. */
    private void swapAll(int i, int j) {
        Collections.swap(encodedRecords, i, j);
        for (int k = 0; k < columns.size(); k++) columns.get(k).swap(i, j);
    }

    /**
     * Builds the MultiKeyIndex over (year last digit, month, town code).
     * Must be called after loadDB().
     */
    public void buildIndex() {
        mki = new MultiKeyIndex(10, 12, TOWN_LIST.length);
        for (int i = 0; i < columns.get(0).size(); i++) {
            String date  = columns.get(COL_DATE).get(i);
            String town  = columns.get(COL_LOCATION).get(i);
            int yearDigit = parseYear(date) % 10;
            int month     = parseMonth(date);
            int locCode   = mapTownToIndex(town);
            mki.addValue(yearDigit, month, locCode, i);
        }
        System.out.println("MultiKeyIndex built, size: " + mki.size());
    }

    /**
     * Builds the ZoneMap over the sorted encoded array.
     * Must be called after sortByCompressedData().
     */
    public void createZoneMap() {
        zoneMap = new ZoneMap();
        short largest = Short.MIN_VALUE;
        int zoneSize  = 18;
        int count     = 0;

        for (int i = 0; i < encodedRecords.size(); i++) {
            if (encodedRecords.get(i) > largest) {
                count++;
                if (count > zoneSize) {
                    count = 1;
                    zoneMap.addZone(largest, i - 1);
                }
                largest = encodedRecords.get(i);
            }
        }
        if (count > 0) zoneMap.addZone(largest, encodedRecords.size() - 1);
    }

    /** Returns the 0-based index of a town in TOWN_LIST, or -1 if unknown. */
    public int mapTownToIndex(String town) {
        return Arrays.asList(TOWN_LIST).indexOf(town);
    }

    // =========================================================================
    // QUERY METHODS
    // All four methods share the same filter logic:
    //   year == spec.targetYear
    //   month in [spec.startMonth, spec.startMonth + x - 1]
    //   town  in spec.targetTowns
    //   area  >= y
    // They differ only in their access strategy.
    // =========================================================================

    /**
     * Builds the inclusive list of target months for a given (spec, x) combination.
     * Months are capped at 12 — queries must not cross into 2025 data.
     */
    private ArrayList<Integer> buildTargetMonths(QuerySpec spec, int x) {
        ArrayList<Integer> months = new ArrayList<>();
        for (int m = spec.startMonth; m < spec.startMonth + x && m <= 12; m++)
            months.add(m);
        return months;
    }

    // -------------------------------------------------------------------------
    // 1. Normal (sequential scan)
    // -------------------------------------------------------------------------

    /**
     * Sequential scan over every record.
     *
     * @param spec Invariant query parameters
     * @param x    Month window size (1-8)
     * @param y    Minimum floor area in sqm (80-150)
     * @return List of row indices that satisfy all filter conditions
     */
    public ArrayList<Integer> queryDB(QuerySpec spec, int x, int y) {
        ArrayList<Integer> posArray      = new ArrayList<>();
        ArrayList<Integer> finalPosArray = new ArrayList<>();
        ArrayList<Integer> targetMonths  = buildTargetMonths(spec, x);
        String             yearStr       = String.valueOf(spec.targetYear);

        // Pass 1: filter by year and month
        for (int i = 0; i < columns.get(0).size(); i++) {
            String date    = columns.get(COL_DATE).get(i);
            int    recYear = parseYear(date);
            int    recMon  = parseMonth(date);
            if (recYear == spec.targetYear && targetMonths.contains(recMon))
                posArray.add(i);
        }

        // Pass 2: filter by town and area
        for (int i : posArray) {
            String town      = columns.get(COL_LOCATION).get(i);
            double floorArea = Double.parseDouble(columns.get(COL_AREA).get(i));
            if (spec.targetTowns.contains(town) && floorArea >= y)
                finalPosArray.add(i);
        }

        return finalPosArray;
    }

    // -------------------------------------------------------------------------
    // 2. Index-based query
    // -------------------------------------------------------------------------

    /**
     * Uses the MultiKeyIndex to jump directly to candidates.
     * The index is keyed on (year last digit, month, town code), so it is
     * queried once per (month, town) combination; full-year verification is
     * applied afterwards because the year last digit is not unique across years.
     *
     * @param spec Invariant query parameters
     * @param x    Month window size (1-8)
     * @param y    Minimum floor area in sqm (80-150)
     * @return List of row indices that satisfy all filter conditions
     */
    public ArrayList<Integer> queryDBIndex(QuerySpec spec, int x, int y) {
        ArrayList<Integer> finalPosArray = new ArrayList<>();
        ArrayList<Integer> targetMonths  = buildTargetMonths(spec, x);
        int yearDigit = spec.targetYear % 10;

        for (String town : spec.targetTowns) {
            int townCode = mapTownToIndex(town);
            if (townCode < 0) continue; // unknown town

            for (int month : targetMonths) {
                ArrayList<Integer> candidates = mki.queryIndex(yearDigit, month, townCode);
                for (int i : candidates) {
                    // Verify full year (year last digit is not unique across decades)
                    int recYear = parseYear(columns.get(COL_DATE).get(i));
                    if (recYear != spec.targetYear) continue;

                    double floorArea = Double.parseDouble(columns.get(COL_AREA).get(i));
                    if (floorArea >= y) finalPosArray.add(i);
                }
            }
        }

        return finalPosArray;
    }

    // -------------------------------------------------------------------------
    // 3. Compressed + ZoneMap + Sorted query
    // -------------------------------------------------------------------------

    /**
     * Uses the sorted encoded array and ZoneMap to narrow the scan range,
     * then verifies matches within the pruned window.
     * Because multiple towns produce non-contiguous ranges in the encoded
     * space, this method runs once per town and unions the results.
     *
     * @param spec Invariant query parameters
     * @param x    Month window size (1-8)
     * @param y    Minimum floor area in sqm (80-150)
     * @return List of row indices that satisfy all filter conditions
     */
    public ArrayList<Integer> queryCompressedDB(QuerySpec spec, int x, int y) {
        ArrayList<Integer> finalPosArray = new ArrayList<>();
        ArrayList<Integer> targetMonths  = buildTargetMonths(spec, x);

        for (String town : spec.targetTowns) {
            // Build encoded range boundaries for this town
            int  startM    = targetMonths.get(0);
            int  endM      = targetMonths.get(targetMonths.size() - 1);
            String startDate = formatDate(startM, spec.targetYear);
            String endDate   = formatDate(endM,   spec.targetYear);

            short encStart = getCompressValue(town, startDate);
            short encEnd   = getCompressValue(town, endDate);

            // Use ZoneMap to get physical index boundaries
            int[] bounds     = zoneMap.getZone(encStart, encEnd);
            int   startIndex = bounds[0];
            int   endIndex   = bounds[3];

            // Optionally narrow further with binary search for large windows
            if (endIndex - startIndex > 1000) {
                startIndex = findBoundary(startIndex, endIndex, encStart, true);
                endIndex   = findBoundary(startIndex, endIndex, encEnd,   false);
            }

            // Sequential scan within the pruned window
            for (int i = startIndex; i <= endIndex && i < encodedRecords.size(); i++) {
                short enc = encodedRecords.get(i);
                if (enc < encStart || enc > encEnd) continue;

                // Verify month is actually in the target set (encoding compresses
                // month into the date index, so we re-check against targetMonths)
                int recMon = parseMonth(columns.get(COL_DATE).get(i));
                if (!targetMonths.contains(recMon)) continue;

                double floorArea = Double.parseDouble(columns.get(COL_AREA).get(i));
                if (floorArea >= y) finalPosArray.add(i);
            }
        }

        return finalPosArray;
    }

    // -------------------------------------------------------------------------
    // 4. Shared scan
    // -------------------------------------------------------------------------

    /**
     * Single pass over all records, populating results for every (x, y) pair
     * simultaneously. This avoids re-scanning the dataset for each combination.
     *
     * Strategy:
     *   Pass 1 — for each record, determine the maximum x window it qualifies for
     *             (i.e., how many months from startMonth the record's month falls in)
     *             and the minimum y it satisfies (its floor area rounded down to the
     *             nearest integer >= 80).  Records that pass town + year filtering are
     *             stored once, keyed by their (monthOffset, area).
     *   Pass 2 — for each (x, y) pair, gather all records where monthOffset <= x and
     *             area >= y.
     *
     * This is equivalent to the naive double loop but avoids repeated full scans.
     *
     * @param spec Invariant query parameters
     * @return Map from (x, y) pair encoded as x*1000+y -> list of matching row indices
     */
    public Map<Integer, ArrayList<Integer>> sharedScanQueryDB(QuerySpec spec) {
        // Pre-filter: collect all records matching year + targetTowns
        // Also record each row's month and area for fast (x,y) assignment
        int totalRecords = columns.get(0).size();

        // Temporary storage: for each qualifying row store (rowIndex, monthOffset, area)
        // monthOffset = recMonth - startMonth  (0-based within the x window)
        ArrayList<int[]> candidates = new ArrayList<>(); // [rowIndex, monthOffset, floorAreaInt]

        for (int i = 0; i < totalRecords; i++) {
            String date    = columns.get(COL_DATE).get(i);
            int    recYear = parseYear(date);
            if (recYear != spec.targetYear) continue;

            int recMon = parseMonth(date);
            int offset = recMon - spec.startMonth; // offset 0 = startMonth, 1 = startMonth+1 ...
            if (offset < 0 || offset >= 8) continue; // outside the maximum x=8 window

            String town = columns.get(COL_LOCATION).get(i);
            if (!spec.targetTowns.contains(town)) continue;

            int areaInt = (int) Double.parseDouble(columns.get(COL_AREA).get(i));
            candidates.add(new int[]{i, offset, areaInt});
        }

        // Build result map for all (x, y) combinations
        Map<Integer, ArrayList<Integer>> resultMap = new HashMap<>();
        for (int x = 1; x <= 8; x++) {
            for (int yy = 80; yy <= 150; yy++) {
                ArrayList<Integer> list = new ArrayList<>();
                for (int[] c : candidates) {
                    // c[1] is 0-based offset; record qualifies for x if offset < x
                    if (c[1] < x && c[2] >= yy) list.add(c[0]);
                }
                resultMap.put(x * 1000 + yy, list);
            }
        }

        return resultMap;
    }

    // =========================================================================
    // RESULT CALCULATION
    // =========================================================================

    /**
     * Represents one output row: the record achieving the minimum price/sqm
     * for a given (x, y) pair.
     */
    public static class QueryResult {
        public final int    x;
        public final int    y;
        public final String year;
        public final String month;
        public final String town;
        public final String block;
        public final String floorArea;
        public final String flatModel;
        public final String leaseCommenceDate;
        public final int    pricePerSqm;

        public QueryResult(int x, int y, String year, String month, String town,
                           String block, String floorArea, String flatModel,
                           String leaseCommenceDate, int pricePerSqm) {
            this.x                 = x;
            this.y                 = y;
            this.year              = year;
            this.month             = month;
            this.town              = town;
            this.block             = block;
            this.floorArea         = floorArea;
            this.flatModel         = flatModel;
            this.leaseCommenceDate = leaseCommenceDate;
            this.pricePerSqm       = pricePerSqm;
        }

        /** CSV row in the required output format. */
        public String toCsvRow() {
            return String.format("\"(%d, %d)\",%s,%s,%s,%s,%s,%s,%s,%d",
                    x, y, year, month, town, block, floorArea,
                    flatModel, leaseCommenceDate, pricePerSqm);
        }
    }

    /**
     * Given a list of candidate row indices, finds the record with the minimum
     * price per square meter. Returns null if the list is empty or if the
     * minimum price/sqm exceeds MAX_PRICE_PER_SQM (4725).
     *
     * @param posArray Row indices of candidate records
     * @param x        Current x value (stored in result for output)
     * @param y        Current y value (stored in result for output)
     */
    public QueryResult findMinPricePerSqm(ArrayList<Integer> posArray, int x, int y) {
        if (posArray == null || posArray.isEmpty()) return null;

        int    bestRow          = -1;
        double bestPricePerSqm  = Double.MAX_VALUE;

        for (int i : posArray) {
            double price = Double.parseDouble(columns.get(COL_PRICE).get(i));
            double area  = Double.parseDouble(columns.get(COL_AREA).get(i));
            double ppsm  = price / area;
            if (ppsm < bestPricePerSqm) {
                bestPricePerSqm = ppsm;
                bestRow         = i;
            }
        }

        // Validity check: must be <= 4725
        if (bestPricePerSqm > MAX_PRICE_PER_SQM) return null;

        // Extract record fields
        String dateStr = columns.get(COL_DATE).get(bestRow);
        int    recYear = parseYear(dateStr);
        int    recMon  = parseMonth(dateStr);

        return new QueryResult(
                x, y,
                String.valueOf(recYear),
                String.format("%02d", recMon),
                columns.get(COL_LOCATION).get(bestRow),
                columns.get(COL_BLOCK).get(bestRow),
                columns.get(COL_AREA).get(bestRow),
                columns.get(COL_MODEL).get(bestRow),
                columns.get(COL_LEASE_START).get(bestRow),
                (int) Math.round(bestPricePerSqm)
        );
    }

    // =========================================================================
    // OUTPUT FILE
    // =========================================================================

    private FileWriter fw;

    /** Creates the output CSV file with its header row. */
    public void createOutputFile(String filename) {
        filename = filename + ".csv";
        try {
            File f = new File(filename);
            f.createNewFile();
            fw = new FileWriter(filename);
            fw.write("\"(x, y)\",Year,Month,Town,Block,Floor_Area,Flat_Model,"
                    + "Lease_Commence_Date,Price_Per_Square_Meter\n");
        } catch (IOException e) {
            throw new RuntimeException("Output file creation failed", e);
        }
    }

    /**
     * Writes one result row. If result is null (no qualifying records or
     * price/sqm > 4725), writes a "No result" line for that (x, y) pair.
     */
    public void writeResult(int x, int y, QueryResult result) {
        try {
            if (result == null) {
                fw.write(String.format("\"(%d, %d)\",No result\n", x, y));
            } else {
                fw.write(result.toCsvRow() + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write result", e);
        }
    }

    /** Closes the output file. */
    public void closeOutputFile() {
        try {
            if (fw != null) fw.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close output file", e);
        }
    }

    // =========================================================================
    // UTILITY
    // =========================================================================

    private boolean hasEmptyFields(String[] fields) {
        if (fields.length < 10) return true;
        for (String f : fields) if (f == null || f.trim().isEmpty()) return true;
        return false;
    }

    private boolean allColumnsEqualLength() {
        int size = columns.get(0).size();
        for (int i = 1; i < columns.size(); i++)
            if (columns.get(i).size() != size) return false;
        return true;
    }

    /**
     * Binary search for the boundary of a value in the sorted encodedRecords.
     *
     * @param start     Start index of the search range
     * @param end       End index of the search range
     * @param target    Target encoded value
     * @param findFirst If true, returns the first occurrence; else the last
     */
    private int findBoundary(int start, int end, short target, boolean findFirst) {
        int left = start, right = end;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            if (mid >= encodedRecords.size()) return encodedRecords.size() - 1;
            short midVal = encodedRecords.get(mid);
            if (midVal < target)       { left  = mid + 1; }
            else if (midVal > target)  { right = mid - 1; }
            else {
                if (findFirst) {
                    if (mid == 0 || encodedRecords.get(mid - 1) < target) return mid;
                    right = mid - 1;
                } else {
                    if (mid == encodedRecords.size() - 1 || encodedRecords.get(mid + 1) > target) return mid;
                    left = mid + 1;
                }
            }
        }
        return findFirst ? left : right;
    }

    // =========================================================================
    // INNER CLASS: Column
    // =========================================================================

    private class Column {
        private ArrayList<String> values = new ArrayList<>();

        public void   add(String value)        { values.add(value); }
        public String get(int index)           { return values.get(index); }
        public int    size()                   { return values.size(); }
        public void   swap(int i, int j)       {
            String tmp = values.get(i);
            values.set(i, values.get(j));
            values.set(j, tmp);
        }
    }
}