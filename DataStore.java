import java.io.*;
import java.util.*;

/**
 * A column-oriented store for HDB resale flat transaction records.
 *
 * Data is read from per-column CSV files (produced once by ColumnStoreWriter)
 * into typed primitive arrays. Numeric fields (year, month, area, price, lease)
 * are parsed at load time so no string conversion is needed during queries.
 *
 * Four query strategies are supported, all enforcing the same filter:
 *   year == queryYear, month in [startMonth, startMonth+x-1],
 *   town in targetTowns, area >= y
 *
 * The variable parameters x (month window, 1-8) and y (area threshold, 80-150)
 * are supplied per query call; Main iterates over all (x, y) pairs.
 */
public class DataStore {

    private int[]    yearCol;
    private int[]    monthCol;
    private String[] townCol;
    private String[] blockCol;
    private double[] areaCol;
    private String[] flatModelCol;
    private int[]    leaseCol;
    private double[] priceCol;

    private int rowCount;

    private short[]           compressedVals;
    private ArrayList<String> townDict;
    private ArrayList<String> dateDict;
    private Index     keyIndex;
    private ZoneMap           zoneMap;

    // Year range found during load — used to build the date dictionary
    private int minYear = 9999;
    private int maxYear = 0;

    static final String[] TOWN_LIST = {
            "BEDOK", "BUKIT PANJANG", "CLEMENTI", "CHOA CHU KANG", "HOUGANG",
            "JURONG WEST", "PASIR RIS", "TAMPINES", "WOODLANDS", "YISHUN"
    };

    private static final String[] MONTH_LABELS = {
            "Jan","Feb","Mar","Apr","May","Jun",
            "Jul","Aug","Sep","Oct","Nov","Dec"
    };

    private static final double PRICE_CAP = 4725.0;

    /**
     * Reads column files from the given directory into typed arrays.
     * ColumnStoreWriter must have completed successfully (completion flag present).
     *
     * @param outputDir directory holding the column CSV files (e.g. "columns/")
     */
    public DataStore(String outputDir) {
        townDict = new ArrayList<>();
        dateDict = new ArrayList<>();
        loadColumns(outputDir);
    }

    /**
     * Derives fixed query parameters from a matriculation number and returns
     * them as a QuerySpec.
     *
     * Derivation rules (per assignment brief):
     *   - Last digit        -> query year (via yearTable[])
     *   - Second-last digit -> startMonth (0 maps to October = 10)
     *   - All distinct digits -> town list
     *
     * Throws if the resolved year is 2025 (data-only year, not a valid query target).
     */
    public static Query parseMatric(String matricNumber) {
        String numericStr = matricNumber.replaceAll("[^0-9]", "");

        if (numericStr.length() < 2)
            throw new IllegalArgumentException(
                    "Matriculation number must contain at least 2 digits: " + matricNumber);

        int[] yearTable = {2020, 2021, 2022, 2023, 2024, 2015, 2016, 2017, 2018, 2019};
        int finalDigit = Character.getNumericValue(numericStr.charAt(numericStr.length() - 1));
        int queryYear  = yearTable[finalDigit];

        if (queryYear == 2025)
            throw new IllegalArgumentException("2025 is not a valid target year.");

        int penultDigit = Character.getNumericValue(numericStr.charAt(numericStr.length() - 2));
        int startMonth  = (penultDigit == 0) ? 10 : penultDigit;

        Set<Integer> digitSet = new LinkedHashSet<>();
        for (char c : numericStr.toCharArray()) digitSet.add(Character.getNumericValue(c));

        ArrayList<String> targetTowns = new ArrayList<>();
        for (int digit : digitSet) {
            if (digit >= 0 && digit < TOWN_LIST.length) {
                String town = TOWN_LIST[digit];
                if (!targetTowns.contains(town)) targetTowns.add(town);
            }
        }

        System.out.println("Matric       : " + matricNumber);
        System.out.println("Target year  : " + queryYear);
        System.out.println("Start month  : " + startMonth);
        System.out.println("Target towns : " + targetTowns);

        return new Query(queryYear, startMonth, targetTowns);
    }

    /**
     * Determines the row count, allocates typed arrays, loads each column file,
     * then builds the town dictionary and year range used by auxiliary structures.
     */
    private void loadColumns(String outputDir) {
        System.out.println("Loading column files from: " + outputDir);

        rowCount = countLines(outputDir + "col_year.csv");
        System.out.println("Total rows: " + rowCount);

        yearCol      = new int[rowCount];
        monthCol     = new int[rowCount];
        townCol      = new String[rowCount];
        blockCol     = new String[rowCount];
        areaCol      = new double[rowCount];
        flatModelCol = new String[rowCount];
        leaseCol     = new int[rowCount];
        priceCol     = new double[rowCount];

        loadIntColumn   (outputDir + "col_year.csv",       yearCol);
        loadIntColumn   (outputDir + "col_month.csv",      monthCol);
        loadStringColumn(outputDir + "col_town.csv",       townCol);
        loadStringColumn(outputDir + "col_block.csv",      blockCol);
        loadDoubleColumn(outputDir + "col_area.csv",       areaCol);
        loadStringColumn(outputDir + "col_flat_model.csv", flatModelCol);
        loadIntColumn   (outputDir + "col_lease.csv",      leaseCol);
        loadDoubleColumn(outputDir + "col_price.csv",      priceCol);

        for (int i = 0; i < rowCount; i++) {
            String town = townCol[i];
            if (!townDict.contains(town)) townDict.add(town);
            if (yearCol[i] > maxYear) maxYear = yearCol[i];
            if (yearCol[i] < minYear) minYear = yearCol[i];
        }

        if (!columnsAligned()) {
            System.err.println("Error: column files have different row counts — aborting.");
            System.exit(1);
        }

        System.out.println("All column files loaded successfully. (" + rowCount + " rows)");
    }

    /** Returns the number of lines in a file (equals the number of data rows; no header). */
    private int countLines(String filepath) {
        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            while (br.readLine() != null) count++;
        } catch (IOException e) {
            System.err.println("Error counting lines in " + filepath + ": " + e.getMessage());
        }
        return count;
    }

    private void loadIntColumn(String filepath, int[] arr) {
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            int i = 0;
            String line;
            while ((line = br.readLine()) != null && i < arr.length)
                arr[i++] = Integer.parseInt(line.trim());
            System.out.println("  Loaded: " + filepath + " (" + i + " rows)");
        } catch (IOException e) {
            throw new RuntimeException("Failed to load " + filepath, e);
        }
    }

    private void loadDoubleColumn(String filepath, double[] arr) {
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            int i = 0;
            String line;
            while ((line = br.readLine()) != null && i < arr.length)
                arr[i++] = Double.parseDouble(line.trim());
            System.out.println("  Loaded: " + filepath + " (" + i + " rows)");
        } catch (IOException e) {
            throw new RuntimeException("Failed to load " + filepath, e);
        }
    }

    private void loadStringColumn(String filepath, String[] arr) {
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            int i = 0;
            String line;
            while ((line = br.readLine()) != null && i < arr.length)
                arr[i++] = line.trim();
            System.out.println("  Loaded: " + filepath + " (" + i + " rows)");
        } catch (IOException e) {
            throw new RuntimeException("Failed to load " + filepath, e);
        }
    }

    private boolean columnsAligned() {
        return yearCol.length == monthCol.length
                && yearCol.length == townCol.length
                && yearCol.length == blockCol.length
                && yearCol.length == areaCol.length
                && yearCol.length == flatModelCol.length
                && yearCol.length == leaseCol.length
                && yearCol.length == priceCol.length;
    }

    /** Converts (month 1-12, full year) to "Jan-15" format — used to build the date dictionary. */
    private static String toDateKey(int month, int year) {
        return String.format("%s-%02d", MONTH_LABELS[month - 1], year % 100);
    }

    /**
     * Encodes each record's (town, date) pair into a single short value.
     * Formula: townIdx * 1000 + dateIdx.
     * The date dictionary spans every month from minYear to maxYear.
     * Values are read directly from yearCol[] and monthCol[] — no string parsing.
     */
    public void encodeRecords() {
        if (dateDict.isEmpty()) {
            for (int y = minYear; y <= maxYear; y++)
                for (int m = 1; m <= 12; m++)
                    dateDict.add(toDateKey(m, y));
        }

        compressedVals = new short[rowCount];
        for (int i = 0; i < rowCount; i++) {
            String dateKey = toDateKey(monthCol[i], yearCol[i]);
            compressedVals[i] = encodeValue(townCol[i], dateKey);
        }
    }

    /** Encodes a single (town, dateKey) pair into a short. */
    private short encodeValue(String town, String dateKey) {
        int tIdx = townDict.indexOf(town);
        int dIdx = dateDict.indexOf(dateKey);
        return (short) (tIdx * 1000 + dIdx);
    }

    /**
     * Heap-sorts all column arrays together using the encoded value as the sort key.
     * After sorting, compressedVals is in ascending order and all arrays remain aligned.
     */
    public void sortEncoded() {
        int n = rowCount - 1;
        for (int i = (n / 2) - 1; i >= 0; i--) siftDown(n, i);
        for (int i = n - 1; i > 0; i--) { swapRows(0, i); siftDown(i, 0); }
    }

    private void siftDown(int heapSize, int root) {
        int largest  = root;
        short pivotVal = compressedVals[largest];
        int left     = 2 * root + 1;
        int right    = 2 * root + 2;

        if (left  < heapSize && compressedVals[left]  > pivotVal) { largest = left;  pivotVal = compressedVals[left];  }
        if (right < heapSize && compressedVals[right] > pivotVal) { largest = right; }

        if (largest != root) { swapRows(root, largest); siftDown(heapSize, largest); }
    }

    /** Exchanges rows i and j across compressedVals and all column arrays. */
    private void swapRows(int i, int j) {
        short tmpS;
        tmpS = compressedVals[i]; compressedVals[i] = compressedVals[j]; compressedVals[j] = tmpS;

        int tmpI;
        tmpI = yearCol[i];  yearCol[i]  = yearCol[j];  yearCol[j]  = tmpI;
        tmpI = monthCol[i]; monthCol[i] = monthCol[j]; monthCol[j] = tmpI;
        tmpI = leaseCol[i]; leaseCol[i] = leaseCol[j]; leaseCol[j] = tmpI;

        double tmpD;
        tmpD = areaCol[i];  areaCol[i]  = areaCol[j];  areaCol[j]  = tmpD;
        tmpD = priceCol[i]; priceCol[i] = priceCol[j]; priceCol[j] = tmpD;

        String tmpStr;
        tmpStr = townCol[i];      townCol[i]      = townCol[j];      townCol[j]      = tmpStr;
        tmpStr = blockCol[i];     blockCol[i]      = blockCol[j];     blockCol[j]     = tmpStr;
        tmpStr = flatModelCol[i]; flatModelCol[i] = flatModelCol[j]; flatModelCol[j] = tmpStr;
    }

    /**
     * Constructs the MultiKeyIndex over (year last digit, month, town code).
     * Values are read directly from yearCol[], monthCol[], townCol[].
     */
    public void buildKeyIndex() {
        keyIndex = new Index(10, 12, TOWN_LIST.length);
        for (int i = 0; i < rowCount; i++) {
            int yrDigit  = yearCol[i] % 10;
            int month    = monthCol[i];
            int townCode = getTownCode(townCol[i]);
            keyIndex.addValue(yrDigit, month, townCode, i);
        }
        System.out.println("MultiKeyIndex built, size: " + keyIndex.size());
    }

    /**
     * Constructs the ZoneMap over the sorted encoded array.
     * Must be called after sortEncoded().
     */
    public void buildZoneMap() {
        zoneMap = new ZoneMap();
        short largest      = Short.MIN_VALUE;
        int partitionLimit = 18;
        int count          = 0;

        for (int i = 0; i < rowCount; i++) {
            if (compressedVals[i] > largest) {
                count++;
                if (count > partitionLimit) {
                    count = 1;
                    zoneMap.addPartition(largest, i - 1);
                }
                largest = compressedVals[i];
            }
        }
        if (count > 0) zoneMap.addPartition(largest, rowCount - 1);
    }

    /** Returns the 0-based index of a town in TOWN_LIST, or -1 if not found. */
    public int getTownCode(String town) {
        return Arrays.asList(TOWN_LIST).indexOf(town);
    }

    /** Builds the inclusive list of months covered by a given (spec, x) combination. */
    private ArrayList<Integer> getMonthRange(Query spec, int x) {
        ArrayList<Integer> months = new ArrayList<>();
        for (int m = spec.startMonth; m < spec.startMonth + x && m <= 12; m++)
            months.add(m);
        return months;
    }

    /**
     * Full sequential scan across every record.
     * All comparisons operate on typed array values — no string parsing at query time.
     */
    public ArrayList<Integer> queryDB(Query spec, int x, int y) {
        ArrayList<Integer> firstPass  = new ArrayList<>();
        ArrayList<Integer> matchedRows = new ArrayList<>();
        ArrayList<Integer> monthRange  = getMonthRange(spec, x);

        // Pass 1: year and month filter
        for (int i = 0; i < rowCount; i++) {
            if (yearCol[i] == spec.queryYear && monthRange.contains(monthCol[i]))
                firstPass.add(i);
        }

        // Pass 2: town and area filter
        for (int i : firstPass) {
            if (spec.targetTowns.contains(townCol[i]) && areaCol[i] >= y)
                matchedRows.add(i);
        }

        return matchedRows;
    }

    /**
     * Uses the MultiKeyIndex to jump directly to candidate rows.
     * A full-year check is still applied because the index key uses only the
     * last digit of the year, which is not unique across decades.
     */
    public ArrayList<Integer> queryDBIndex(Query spec, int x, int y) {
        ArrayList<Integer> matchedRows = new ArrayList<>();
        ArrayList<Integer> monthRange  = getMonthRange(spec, x);
        int yrDigit = spec.queryYear % 10;

        for (String town : spec.targetTowns) {
            int townCode = getTownCode(town);
            if (townCode < 0) continue;

            for (int month : monthRange) {
                ArrayList<Integer> candidates = keyIndex.queryIndex(yrDigit, month, townCode);
                for (int i : candidates) {
                    if (yearCol[i] != spec.queryYear) continue; // full year check
                    if (areaCol[i] >= y) matchedRows.add(i);
                }
            }
        }

        return matchedRows;
    }

    /**
     * Uses the sorted encoded array and ZoneMap to restrict the scan range.
     * Invoked once per town since towns are non-contiguous in encoded space.
     */
    public ArrayList<Integer> queryCompressedDB(Query spec, int x, int y) {
        ArrayList<Integer> matchedRows = new ArrayList<>();
        ArrayList<Integer> monthRange  = getMonthRange(spec, x);

        for (String town : spec.targetTowns) {
            int   startM   = monthRange.get(0);
            int   endM     = monthRange.get(monthRange.size() - 1);
            short encStart = encodeValue(town, toDateKey(startM, spec.queryYear));
            short encEnd   = encodeValue(town, toDateKey(endM,   spec.queryYear));

            int[] bounds     = zoneMap.queryZone(encStart, encEnd);
            int   startIndex = bounds[0];
            int   endIndex   = bounds[3];

            if (endIndex - startIndex > 1000) {
                startIndex = binaryBound(startIndex, endIndex, encStart, true);
                endIndex   = binaryBound(startIndex, endIndex, encEnd,   false);
            }

            for (int i = startIndex; i <= endIndex && i < rowCount; i++) {
                if (compressedVals[i] < encStart || compressedVals[i] > encEnd) continue;
                if (!monthRange.contains(monthCol[i])) continue;
                if (areaCol[i] >= y) matchedRows.add(i);
            }
        }

        return matchedRows;
    }

    /**
     * Scans all records once and populates results for every (x, y) pair in
     * a single pass. Qualifying rows are captured as (rowIndex, monthOffset, areaInt)
     * tuples, then assigned to each (x, y) bucket by arithmetic instead of re-scanning.
     *
     * @return Map keyed by x*1000+y -> list of matching row indices
     */
    public Map<Integer, ArrayList<Integer>> sharedScanQueryDB(Query spec) {
        ArrayList<int[]> candidates = new ArrayList<>(); // [rowIndex, monthOffset, areaInt]

        for (int i = 0; i < rowCount; i++) {
            if (yearCol[i] != spec.queryYear) continue;

            int offset = monthCol[i] - spec.startMonth;
            if (offset < 0 || offset >= 8) continue;

            if (!spec.targetTowns.contains(townCol[i])) continue;

            candidates.add(new int[]{i, offset, (int) areaCol[i]});
        }

        Map<Integer, ArrayList<Integer>> resultMap = new HashMap<>();
        for (int x = 1; x <= 8; x++) {
            for (int yy = 80; yy <= 150; yy++) {
                ArrayList<Integer> list = new ArrayList<>();
                for (int[] c : candidates) {
                    if (c[1] < x && c[2] >= yy) list.add(c[0]);
                }
                resultMap.put(x * 1000 + yy, list);
            }
        }

        return resultMap;
    }

    /**
     * Represents one output row: the record with the lowest price/sqm
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

        public QueryResult(int x, int y, int year, int month, String town,
                           String block, double floorArea, String flatModel,
                           int leaseCommenceDate, int pricePerSqm) {
            this.x                 = x;
            this.y                 = y;
            this.year              = String.valueOf(year);
            this.month             = String.format("%02d", month);
            this.town              = town;
            this.block             = block;
            this.floorArea         = String.valueOf(floorArea);
            this.flatModel         = flatModel;
            this.leaseCommenceDate = String.valueOf(leaseCommenceDate);
            this.pricePerSqm       = pricePerSqm;
        }

        /** Formats this result as a CSV row for output. */
        public String toCsvRow() {
            return String.format("\"(%d, %d)\",%s,%s,%s,%s,%s,%s,%s,%d",
                    x, y, year, month, town, block, floorArea,
                    flatModel, leaseCommenceDate, pricePerSqm);
        }
    }

    /**
     * Finds the record with the lowest price per sqm among the given candidates.
     * Returns null if the list is empty or the minimum value exceeds PRICE_CAP.
     */
    public QueryResult findMinPricePerSqm(ArrayList<Integer> matchedRows, int x, int y) {
        if (matchedRows == null || matchedRows.isEmpty()) return null;

        int    minRow  = -1;
        double minPpsm = Double.MAX_VALUE;

        for (int i : matchedRows) {
            double unitPrice = priceCol[i] / areaCol[i];
            if (unitPrice < minPpsm) {
                minPpsm = unitPrice;
                minRow  = i;
            }
        }

        if (minPpsm > PRICE_CAP) return null;

        return new QueryResult(
                x, y,
                yearCol[minRow],
                monthCol[minRow],
                townCol[minRow],
                blockCol[minRow],
                areaCol[minRow],
                flatModelCol[minRow],
                leaseCol[minRow],
                (int) Math.round(minPpsm)
        );
    }

    private FileWriter fw;

    /** Creates the output CSV file and writes the header row. */
    public void createOutputFile(String filename) {
        filename = filename + ".csv";
        try {
            new File(filename).createNewFile();
            fw = new FileWriter(filename);
            fw.write("\"(x, y)\",Year,Month,Town,Block,Floor_Area,Flat_Model,"
                    + "Lease_Commence_Date,Price_Per_Square_Meter\n");
        } catch (IOException e) {
            throw new RuntimeException("Output file creation failed", e);
        }
    }

    /** Appends one result row. Writes "No result" if result is null. */
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

    /**
     * Binary search for the boundary position of a target value in compressedVals.
     *
     * @param seekStart If true, returns the first occurrence; otherwise the last.
     */
    private int binaryBound(int start, int end, short target, boolean seekStart) {
        int left = start, right = end;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            if (mid >= rowCount) return rowCount - 1;
            short midEnc = compressedVals[mid];
            if      (midEnc < target) { left  = mid + 1; }
            else if (midEnc > target) { right = mid - 1; }
            else {
                if (seekStart) {
                    if (mid == 0 || compressedVals[mid - 1] < target) return mid;
                    right = mid - 1;
                } else {
                    if (mid == rowCount - 1 || compressedVals[mid + 1] > target) return mid;
                    left = mid + 1;
                }
            }
        }
        return seekStart ? left : right;
    }
}