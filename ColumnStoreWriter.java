import java.io.*;
import java.util.*;

/**
 * Reads ResalePricesSingapore.csv once and writes each column into a
 * separate CSV file under the specified output directory.
 *
 * Column files written (one value per line, no header):
 *   col_year.csv       — int,    e.g. 2015
 *   col_month.csv      — int,    e.g. 2
 *   col_town.csv       — String, e.g. TAMPINES
 *   col_block.csv      — String, e.g. 123A
 *   col_area.csv       — double, e.g. 92.0
 *   col_flat_model.csv — String, e.g. Improved
 *   col_lease.csv      — int,    e.g. 1995
 *   col_price.csv      — double, e.g. 320000.0
 *
 * A READY marker file is written only after all column files are successfully
 * flushed and closed. Main checks for this marker via isReady() to decide
 * whether to skip the write phase on subsequent runs.
 *
 * Expected CSV date format: MMM-YY (e.g. Jan-15).
 * Years are assumed to be in the 2000s: fullYear = 2000 + YY.
 */
public class ColumnStoreWriter {

    private static final String READY_MARKER = "READY";

    private final String inputCsvPath;
    private final String colDir;

    private static final Map<String, Integer> MONTH_MAP = new HashMap<>();
    static {
        MONTH_MAP.put("Jan", 1);  MONTH_MAP.put("Feb", 2);
        MONTH_MAP.put("Mar", 3);  MONTH_MAP.put("Apr", 4);
        MONTH_MAP.put("May", 5);  MONTH_MAP.put("Jun", 6);
        MONTH_MAP.put("Jul", 7);  MONTH_MAP.put("Aug", 8);
        MONTH_MAP.put("Sep", 9);  MONTH_MAP.put("Oct", 10);
        MONTH_MAP.put("Nov", 11); MONTH_MAP.put("Dec", 12);
    }

    /**
     * @param inputCsvPath path to ResalePricesSingapore.csv
     * @param colDir       directory to write column files into (e.g. "columns/")
     */
    public ColumnStoreWriter(String inputCsvPath, String colDir) {
        this.inputCsvPath = inputCsvPath;
        this.colDir       = colDir;
    }

    /**
     * Returns true if the READY marker file exists, meaning column files were
     * previously written successfully and can be loaded directly.
     */
    public boolean isReady() {
        return new File(colDir + READY_MARKER).exists();
    }

    /**
     * Creates the output directory if absent, parses the CSV, writes all
     * column files, and writes the READY marker on success.
     */
    public void run() {
        createDirectory();
        writeColumnFiles();
    }

    private void createDirectory() {
        File dir = new File(colDir);
        if (!dir.exists()) {
            dir.mkdirs();
            System.out.println("  Created directory: " + colDir);
        }
    }

    private void writeColumnFiles() {
        System.out.println("  Reading CSV: " + inputCsvPath);
        System.out.println("  Writing column files to: " + colDir);

        try {
            BufferedReader br = new BufferedReader(new FileReader(inputCsvPath));

            BufferedWriter w_year       = new BufferedWriter(new FileWriter(colDir + "col_year.csv"));
            BufferedWriter w_month      = new BufferedWriter(new FileWriter(colDir + "col_month.csv"));
            BufferedWriter w_town       = new BufferedWriter(new FileWriter(colDir + "col_town.csv"));
            BufferedWriter w_block      = new BufferedWriter(new FileWriter(colDir + "col_block.csv"));
            BufferedWriter w_area       = new BufferedWriter(new FileWriter(colDir + "col_area.csv"));
            BufferedWriter w_flat_model = new BufferedWriter(new FileWriter(colDir + "col_flat_model.csv"));
            BufferedWriter w_lease      = new BufferedWriter(new FileWriter(colDir + "col_lease.csv"));
            BufferedWriter w_price      = new BufferedWriter(new FileWriter(colDir + "col_price.csv"));

            String line;
            int lineCount  = 0;
            int skipCount  = 0;
            int writeCount = 0;

            while ((line = br.readLine()) != null) {
                lineCount++;
                if (lineCount == 1) continue; // skip header row

                String[] parts = line.split(",");

                if (parts.length < 10) {
                    skipCount++;
                    System.out.println("  Warning: skipped malformed row " + lineCount
                            + " (fields=" + parts.length + "): " + line);
                    continue;
                }

                boolean hasEmpty = false;
                for (int k = 0; k < 10; k++) {
                    if (parts[k] == null || parts[k].trim().isEmpty()) {
                        hasEmpty = true;
                        break;
                    }
                }
                if (hasEmpty) {
                    skipCount++;
                    System.out.println("  Warning: skipped row with empty field at line " + lineCount);
                    continue;
                }

                String[] dateParts = parts[0].trim().split("-");
                if (dateParts.length < 2 || !MONTH_MAP.containsKey(dateParts[0])) {
                    skipCount++;
                    System.out.println("  Warning: skipped row with unrecognised date at line "
                            + lineCount + ": " + parts[0]);
                    continue;
                }
                int fullYear = 2000 + Integer.parseInt(dateParts[1].trim());
                int monthNum = MONTH_MAP.get(dateParts[0]);

                String town      = parts[1].trim();
                // parts[2] = flat_type    (not needed for queries or output — skipped)
                String block     = parts[3].trim();
                // parts[4] = street_name  (not needed — skipped)
                // parts[5] = storey_range (not needed — skipped)
                double area      = Double.parseDouble(parts[6].trim());
                String flatModel = parts[7].trim();
                int    lease     = Integer.parseInt(parts[8].trim());
                double price     = Double.parseDouble(parts[9].trim());

                w_year.write(String.valueOf(fullYear));    w_year.newLine();
                w_month.write(String.valueOf(monthNum));   w_month.newLine();
                w_town.write(town);                        w_town.newLine();
                w_block.write(block);                      w_block.newLine();
                w_area.write(String.valueOf(area));        w_area.newLine();
                w_flat_model.write(flatModel);             w_flat_model.newLine();
                w_lease.write(String.valueOf(lease));      w_lease.newLine();
                w_price.write(String.valueOf(price));      w_price.newLine();

                writeCount++;
            }

            br.close();
            w_year.close();
            w_month.close();
            w_town.close();
            w_block.close();
            w_area.close();
            w_flat_model.close();
            w_lease.close();
            w_price.close();

            System.out.println("  Rows written : " + writeCount);
            System.out.println("  Rows skipped : " + skipCount);

            new FileWriter(colDir + READY_MARKER).close();
            System.out.println("  READY marker written: " + colDir + READY_MARKER);

        } catch (IOException e) {
            System.err.println("Error during column file write: " + e.getMessage());
            throw new RuntimeException("ColumnStoreWriter failed", e);
        }
    }
}
