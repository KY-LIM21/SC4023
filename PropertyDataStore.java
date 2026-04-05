import java.io.*;
import java.util.*;

/**
 * A column-oriented data store for HDB resale transaction records.
 *
 * Data is loaded from per-column CSV files (written once by ColumnStoreWriter)
 * into typed primitive arrays. Numeric fields (year, month, area, price, lease)
 * are parsed at load time so query methods never need to re-parse strings.
 *
 * Supports four query strategies that all share the same filter logic:
 *   year == targetYear, month in [startMonth, startMonth+x-1],
 *   town in targetTowns, area >= y
 *
 * The variable parameters x (month window, 1-8) and y (area threshold, 80-150)
 * are passed into each query call; the caller (Main) iterates over all pairs.
 */
public class PropertyDataStore {

    private int[]    col_year;
    private int[]    col_month;
    private String[] col_town;
    private String[] col_block;
    private double[] col_area;
    private String[] col_flat_model;
    private int[]    col_lease;
    private double[] col_price;

    private int totalRows;

    private short[]           encodedRecords;
    private ArrayList<String> townCompressList;
    private ArrayList<String> dateCompressList;
    private MultiKeyIndex     mki;
    private ZoneMap           zoneMap;

    // Year range discovered during load — used to build the date dictionary
    private int smallestYear = 9999;
    private int largestYear  = 0;

    static final String[] TOWN_LIST = {
        "BEDOK", "BUKIT PANJANG", "CLEMENTI", "CHOA CHU KANG", "HOUGANG",
        "JURONG WEST", "PASIR RIS", "TAMPINES", "WOODLANDS", "YISHUN"
    };

    private static final String[] MONTH_ABBR = {
        "Jan","Feb","Mar","Apr","May","Jun",
        "Jul","Aug","Sep","Oct","Nov","Dec"
    };

    private static final double MAX_PRICE_PER_SQM = 4725.0;

    /**
     * Loads column files from the given directory into typed arrays.
     * ColumnStoreWriter must have already run successfully (READY marker present).
     *
     * @param colDir directory containing the column CSV files (e.g. "columns/")
     */
    public PropertyDataStore(String colDir) {
        townCompressList = new ArrayList<>();
        dateCompressList = new ArrayList<>();
        loadFromColumnFiles(colDir);
    }

    /**
     * Parses a matriculation number and returns a QuerySpec containing the
     * invariant query parameters (year, startMonth, list of towns).
     *
     * Rules (per assignment brief):
     *   - Last digit        -> target year (via yearMapping[])
     *   - Second-last digit -> startMonth (0 -> October = 10)
     *   - All distinct digits -> towns list
     *
     * Throws if the mapped year would be 2025 (data-only year, not a query target).
     */
    public static QuerySpec buildQuerySpec(String matricNumber) {
        String digitsOnly = matricNumber.replaceAll("[^0-9]", "");

        if (digitsOnly.length() < 2)
            throw new IllegalArgumentException(
                "Matriculation number must contain at least 2 digits: " + matricNumber);

        int[] yearMapping = {2020, 2021, 2022, 2023, 2014, 2015, 2016, 2017, 2018, 2019};
        int lastDigit  = Character.getNumericValue(digitsOnly.charAt(digitsOnly.length() - 1));
        int targetYear = yearMapping[lastDigit];

        if (targetYear == 2025)
            throw new IllegalArgumentException("2025 is not a valid target year.");

        int secondLastDigit = Character.getNumericValue(digitsOnly.charAt(digitsOnly.length() - 2));
        int startMonth = (secondLastDigit == 0) ? 10 : secondLastDigit;

        Set<Integer> seenDigits = new LinkedHashSet<>();
        for (char c : digitsOnly.toCharArray()) seenDigits.add(Character.getNumericValue(c));

        ArrayList<String> targetTowns = new ArrayList<>();
        for (int digit : seenDigits) {
            if (digit >= 0 && digit < TOWN_LIST.length) {
                String town = TOWN_LIST[digit];
                if (!targetTowns.contains(town)) targetTowns.add(town);
            }
        }

        System.out.println("Matric       : " + matricNumber);
        System.out.println("Target year  : " + targetYear);
        System.out.println("Start month  : " + startMonth);
        System.out.println("Target towns : " + targetTowns);

        return new QuerySpec(targetYear, startMonth, targetTowns);
    }

    /**
     * Counts rows, allocates typed arrays, loads each column file, then
     * populates the town compress list and year range needed for auxiliary
     * structure building.
     */
    private void loadFromColumnFiles(String colDir) {
        System.out.println("Loading column files from: " + colDir);

        totalRows = countLines(colDir + "col_year.csv");
        System.out.println("Total rows: " + totalRows);

        col_year       = new int[totalRows];
        col_month      = new int[totalRows];
        col_town       = new String[totalRows];
        col_block      = new String[totalRows];
        col_area       = new double[totalRows];
        col_flat_model = new String[totalRows];
        col_lease      = new int[totalRows];
        col_price      = new double[totalRows];

        loadIntColumn   (colDir + "col_year.csv",       col_year);
        loadIntColumn   (colDir + "col_month.csv",      col_month);
        loadStringColumn(colDir + "col_town.csv",       col_town);
        loadStringColumn(colDir + "col_block.csv",      col_block);
        loadDoubleColumn(colDir + "col_area.csv",       col_area);
        loadStringColumn(colDir + "col_flat_model.csv", col_flat_model);
        loadIntColumn   (colDir + "col_lease.csv",      col_lease);
        loadDoubleColumn(colDir + "col_price.csv",      col_price);

        for (int i = 0; i < totalRows; i++) {
            String town = col_town[i];
            if (!townCompressList.contains(town)) townCompressList.add(town);
            if (col_year[i] > largestYear)  largestYear  = col_year[i];
            if (col_year[i] < smallestYear) smallestYear = col_year[i];
        }

        if (!allColumnsEqualLength()) {
            System.err.println("Error: column files have different row counts — aborting.");
            System.exit(1);
        }

        System.out.println("All column files loaded successfully. (" + totalRows + " rows)");
    }

    /** Counts the number of lines in a file (= number of data rows, no header). */
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

    private boolean allColumnsEqualLength() {
        return col_year.length == col_month.length
            && col_year.length == col_town.length
            && col_year.length == col_block.length
            && col_year.length == col_area.length
            && col_year.length == col_flat_model.length
            && col_year.length == col_lease.length
            && col_year.length == col_price.length;
    }

    /** (month 1-12, full year) -> "Jan-15" — used only for building the date dictionary. */
    private static String formatDate(int month, int year) {
        return String.format("%s-%02d", MONTH_ABBR[month - 1], year % 100);
    }

    /**
     * Encodes each record's (town, date) pair into a single short value.
     * Encoding: townIndex * 1000 + dateIndex
     * Date dictionary covers every month from smallestYear to largestYear.
     * Reads directly from col_year[] and col_month[] — no string parsing.
     */
    public void compressTownDate() {
        if (dateCompressList.isEmpty()) {
            for (int y = smallestYear; y <= largestYear; y++)
                for (int m = 1; m <= 12; m++)
                    dateCompressList.add(formatDate(m, y));
        }

        encodedRecords = new short[totalRows];
        for (int i = 0; i < totalRows; i++) {
            String dateStr = formatDate(col_month[i], col_year[i]);
            encodedRecords[i] = getCompressValue(col_town[i], dateStr);
        }
    }

    /** Encodes one (town, dateString) pair into a short. */
    private short getCompressValue(String town, String dateString) {
        int townIndex = townCompressList.indexOf(town);
        int dateIndex = dateCompressList.indexOf(dateString);
        return (short) (townIndex * 1000 + dateIndex);
    }

    /**
     * Heap-sorts all column arrays simultaneously by the encoded value.
     * After this, encodedRecords is sorted ascending and all arrays stay aligned.
     */
    public void sortByCompressedData() {
        int n = totalRows - 1;
        for (int i = (n / 2) - 1; i >= 0; i--) heapify(n, i);
        for (int i = n - 1; i > 0; i--) { swapAll(0, i); heapify(i, 0); }
    }

    private void heapify(int heapSize, int root) {
        int largest   = root;
        short rootVal = encodedRecords[largest];
        int left      = 2 * root + 1;
        int right     = 2 * root + 2;

        if (left  < heapSize && encodedRecords[left]  > rootVal) { largest = left;  rootVal = encodedRecords[left];  }
        if (right < heapSize && encodedRecords[right] > rootVal) { largest = right; }

        if (largest != root) { swapAll(root, largest); heapify(heapSize, largest); }
    }

    /** Swaps row i and row j across encodedRecords and all column arrays. */
    private void swapAll(int i, int j) {
        short tmpS;
        tmpS = encodedRecords[i]; encodedRecords[i] = encodedRecords[j]; encodedRecords[j] = tmpS;

        int tmpI;
        tmpI = col_year[i];  col_year[i]  = col_year[j];  col_year[j]  = tmpI;
        tmpI = col_month[i]; col_month[i] = col_month[j]; col_month[j] = tmpI;
        tmpI = col_lease[i]; col_lease[i] = col_lease[j]; col_lease[j] = tmpI;

        double tmpD;
        tmpD = col_area[i];  col_area[i]  = col_area[j];  col_area[j]  = tmpD;
        tmpD = col_price[i]; col_price[i] = col_price[j]; col_price[j] = tmpD;

        String tmpStr;
        tmpStr = col_town[i];       col_town[i]       = col_town[j];       col_town[j]       = tmpStr;
        tmpStr = col_block[i];      col_block[i]       = col_block[j];      col_block[j]      = tmpStr;
        tmpStr = col_flat_model[i]; col_flat_model[i] = col_flat_model[j]; col_flat_model[j] = tmpStr;
    }

    /**
     * Builds the MultiKeyIndex over (year last digit, month, town code).
     * Reads directly from col_year[], col_month[], col_town[].
     */
    public void buildIndex() {
        mki = new MultiKeyIndex(10, 12, TOWN_LIST.length);
        for (int i = 0; i < totalRows; i++) {
            int yearDigit = col_year[i] % 10;
            int month     = col_month[i];
            int locCode   = mapTownToIndex(col_town[i]);
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

        for (int i = 0; i < totalRows; i++) {
            if (encodedRecords[i] > largest) {
                count++;
                if (count > zoneSize) {
                    count = 1;
                    zoneMap.addZone(largest, i - 1);
                }
                largest = encodedRecords[i];
            }
        }
        if (count > 0) zoneMap.addZone(largest, totalRows - 1);
    }

    /** Returns the 0-based index of a town in TOWN_LIST, or -1 if unknown. */
    public int mapTownToIndex(String town) {
        return Arrays.asList(TOWN_LIST).indexOf(town);
    }

    /** Builds the inclusive list of target months for a given (spec, x). */
    private ArrayList<Integer> buildTargetMonths(QuerySpec spec, int x) {
        ArrayList<Integer> months = new ArrayList<>();
        for (int m = spec.startMonth; m < spec.startMonth + x && m <= 12; m++)
            months.add(m);
        return months;
    }

    /**
     * Sequential scan over every record.
     * All comparisons use typed array values — no string parsing at query time.
     */
    public ArrayList<Integer> queryDB(QuerySpec spec, int x, int y) {
        ArrayList<Integer> posArray      = new ArrayList<>();
        ArrayList<Integer> finalPosArray = new ArrayList<>();
        ArrayList<Integer> targetMonths  = buildTargetMonths(spec, x);

        // Pass 1: year and month filter
        for (int i = 0; i < totalRows; i++) {
            if (col_year[i] == spec.targetYear && targetMonths.contains(col_month[i]))
                posArray.add(i);
        }

        // Pass 2: town and area filter
        for (int i : posArray) {
            if (spec.targetTowns.contains(col_town[i]) && col_area[i] >= y)
                finalPosArray.add(i);
        }

        return finalPosArray;
    }

    /**
     * Uses the MultiKeyIndex to jump directly to candidates.
     * Full-year verification is still applied because the index is keyed on
     * the last digit of the year only, which is not unique across decades.
     */
    public ArrayList<Integer> queryDBIndex(QuerySpec spec, int x, int y) {
        ArrayList<Integer> finalPosArray = new ArrayList<>();
        ArrayList<Integer> targetMonths  = buildTargetMonths(spec, x);
        int yearDigit = spec.targetYear % 10;

        for (String town : spec.targetTowns) {
            int townCode = mapTownToIndex(town);
            if (townCode < 0) continue;

            for (int month : targetMonths) {
                ArrayList<Integer> candidates = mki.queryIndex(yearDigit, month, townCode);
                for (int i : candidates) {
                    if (col_year[i] != spec.targetYear) continue; // full year check
                    if (col_area[i] >= y) finalPosArray.add(i);
                }
            }
        }

        return finalPosArray;
    }

    /**
     * Uses the sorted encoded array and ZoneMap to narrow the scan range.
     * Runs once per town since multiple towns are non-contiguous in encoded space.
     */
    public ArrayList<Integer> queryCompressedDB(QuerySpec spec, int x, int y) {
        ArrayList<Integer> finalPosArray = new ArrayList<>();
        ArrayList<Integer> targetMonths  = buildTargetMonths(spec, x);

        for (String town : spec.targetTowns) {
            int   startM   = targetMonths.get(0);
            int   endM     = targetMonths.get(targetMonths.size() - 1);
            short encStart = getCompressValue(town, formatDate(startM, spec.targetYear));
            short encEnd   = getCompressValue(town, formatDate(endM,   spec.targetYear));

            int[] bounds     = zoneMap.getZone(encStart, encEnd);
            int   startIndex = bounds[0];
            int   endIndex   = bounds[3];

            if (endIndex - startIndex > 1000) {
                startIndex = findBoundary(startIndex, endIndex, encStart, true);
                endIndex   = findBoundary(startIndex, endIndex, encEnd,   false);
            }

            for (int i = startIndex; i <= endIndex && i < totalRows; i++) {
                if (encodedRecords[i] < encStart || encodedRecords[i] > encEnd) continue;
                if (!targetMonths.contains(col_month[i])) continue;
                if (col_area[i] >= y) finalPosArray.add(i);
            }
        }

        return finalPosArray;
    }

    /**
     * Single pass over all records, populating results for every (x, y) pair
     * simultaneously. Records qualifying for year + town are stored once as
     * (rowIndex, monthOffset, areaInt); then assigned to (x,y) pairs by
     * arithmetic rather than re-scanning.
     *
     * @return Map keyed by x*1000+y -> list of matching row indices
     */
    public Map<Integer, ArrayList<Integer>> sharedScanQueryDB(QuerySpec spec) {
        ArrayList<int[]> candidates = new ArrayList<>(); // [rowIndex, monthOffset, areaInt]

        for (int i = 0; i < totalRows; i++) {
            if (col_year[i] != spec.targetYear) continue;

            int offset = col_month[i] - spec.startMonth;
            if (offset < 0 || offset >= 8) continue;

            if (!spec.targetTowns.contains(col_town[i])) continue;

            candidates.add(new int[]{i, offset, (int) col_area[i]});
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

        /** CSV row in the required output format. */
        public String toCsvRow() {
            return String.format("\"(%d, %d)\",%s,%s,%s,%s,%s,%s,%s,%d",
                    x, y, year, month, town, block, floorArea,
                    flatModel, leaseCommenceDate, pricePerSqm);
        }
    }

    /**
     * Finds the record with the minimum price/sqm among the candidates.
     * Returns null if the list is empty or if the minimum exceeds 4725.
     */
    public QueryResult findMinPricePerSqm(ArrayList<Integer> posArray, int x, int y) {
        if (posArray == null || posArray.isEmpty()) return null;

        int    bestRow         = -1;
        double bestPricePerSqm = Double.MAX_VALUE;

        for (int i : posArray) {
            double ppsm = col_price[i] / col_area[i];
            if (ppsm < bestPricePerSqm) {
                bestPricePerSqm = ppsm;
                bestRow         = i;
            }
        }

        if (bestPricePerSqm > MAX_PRICE_PER_SQM) return null;

        return new QueryResult(
            x, y,
            col_year[bestRow],
            col_month[bestRow],
            col_town[bestRow],
            col_block[bestRow],
            col_area[bestRow],
            col_flat_model[bestRow],
            col_lease[bestRow],
            (int) Math.round(bestPricePerSqm)
        );
    }

    private FileWriter fw;

    /** Creates the output CSV file with its header row. */
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

    /** Writes one result row. Writes "No result" if result is null. */
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
     * Binary search for the boundary of a value in the sorted encodedRecords array.
     *
     * @param findFirst If true, returns the first occurrence; else the last.
     */
    private int findBoundary(int start, int end, short target, boolean findFirst) {
        int left = start, right = end;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            if (mid >= totalRows) return totalRows - 1;
            short midVal = encodedRecords[mid];
            if      (midVal < target) { left  = mid + 1; }
            else if (midVal > target) { right = mid - 1; }
            else {
                if (findFirst) {
                    if (mid == 0 || encodedRecords[mid - 1] < target) return mid;
                    right = mid - 1;
                } else {
                    if (mid == totalRows - 1 || encodedRecords[mid + 1] > target) return mid;
                    left = mid + 1;
                }
            }
        }
        return findFirst ? left : right;
    }
}
