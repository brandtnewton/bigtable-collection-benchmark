package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/google/uuid"
)

// Configuration constants
const (
	// COLUMN_FAMILY is the column family used for the benchmark.
	// The Blob storage will use a single column in this family.
	// The Column Family storage will use column qualifiers in this family.
	COLUMN_FAMILY = "cf" //

	// BLOB_COLUMN is the single column qualifier used to store the entire
	// serialized JSON blob.
	BLOB_COLUMN = "collection_blob"

	// KEY_TO_READ is a static key we know will exist in the initial map
	// used for the "Read One" benchmark.
	KEY_TO_READ = "initial-key-0"
)

func main() {
	ctx := context.Background()

	// --- 1. Configuration from Environment Variables ---
	// Required Bigtable connection details
	projectID := getEnv("PROJECT_ID", "")
	instanceID := getEnv("INSTANCE_ID", "")
	tableID := getEnv("TABLE_ID", "")

	if projectID == "" || instanceID == "" || tableID == "" {
		log.Fatalf("PROJECT_ID, INSTANCE_ID, and TABLE_ID environment variables must be set.")
	}

	// Benchmark-specific configuration
	numRuns, _ := strconv.Atoi(getEnv("NUM_RUNS", "1000"))
	initialSize, _ := strconv.Atoi(getEnv("INITIAL_SIZE", "200"))

	if initialSize <= 0 {
		log.Fatalf("INITIAL_SIZE must be > 0 to perform the 'Read One' benchmark.")
	}

	log.Printf("Starting benchmark with NUM_RUNS=%d, INITIAL_SIZE=%d", numRuns, initialSize)

	// --- 2. Setup Bigtable Client ---
	client, err := bigtable.NewClient(ctx, projectID, instanceID)
	if err != nil {
		log.Fatalf("Could not create Bigtable client: %v", err)
	}
	defer client.Close()

	tbl := client.Open(tableID)

	// --- 3. Run Benchmark Loop ---
	// Slices to hold timings for all benchmark types
	var (
		blobAppendTimings      []time.Duration
		colFamilyAppendTimings []time.Duration

		blobReadOneTimings      []time.Duration
		blobReadAllTimings      []time.Duration
		colFamilyReadOneTimings []time.Duration
		colFamilyReadAllTimings []time.Duration

		blobSqlReadOneTimings      []time.Duration
		blobSqlReadAllTimings      []time.Duration
		colFamilySqlReadOneTimings []time.Duration
		colFamilySqlReadAllTimings []time.Duration
	)

	for i := 0; i < numRuns; i++ {
		// Use a unique row key for each run to avoid contention
		blobRowKey := fmt.Sprintf("benchmark-blob-%s", uuid.New().String())
		colFamilyRowKey := fmt.Sprintf("benchmark-cf-%s", uuid.New().String())

		// Generate a new key-value pair to append for this run
		keyToAppend := fmt.Sprintf("new-key-%s", uuid.New().String())
		valToAppend := fmt.Sprintf("new-val-%s", uuid.New().String())

		// --- Initialization Step ---
		// This part is NOT timed.
		initialMap := generateMap(initialSize)

		// Initialize collection for Blob method
		if err := initializeBlobCollection(ctx, tbl, blobRowKey, initialMap); err != nil {
			log.Printf("Warning: Failed to initialize blob collection for row %s: %v", blobRowKey, err)
			continue
		}
		// Initialize collection for Column Family method
		if err := initializeColumnFamilyCollection(ctx, tbl, colFamilyRowKey, initialMap); err != nil {
			log.Printf("Warning: Failed to initialize column family collection for row %s: %v", colFamilyRowKey, err)
			continue
		}

		// --- Run Benchmarks ---

		// 1. Append Operation
		durationBlobAppend, err := benchmarkBlobAppend(ctx, tbl, blobRowKey, keyToAppend, valToAppend)
		if err != nil {
			log.Printf("Warning: Blob append failed: %v", err)
		} else {
			blobAppendTimings = append(blobAppendTimings, durationBlobAppend)
		}

		durationColFamilyAppend, err := benchmarkColumnFamilyAppend(ctx, tbl, colFamilyRowKey, keyToAppend, valToAppend)
		if err != nil {
			log.Printf("Warning: Column Family append failed: %v", err)
		} else {
			colFamilyAppendTimings = append(colFamilyAppendTimings, durationColFamilyAppend)
		}

		// 2. Read One Element
		durationBlobReadOne, err := benchmarkBlobReadOne(ctx, tbl, blobRowKey, KEY_TO_READ)
		if err != nil {
			log.Printf("Warning: Blob read one failed: %v", err)
		} else {
			blobReadOneTimings = append(blobReadOneTimings, durationBlobReadOne)
		}

		durationColFamilyReadOne, err := benchmarkColFamilyReadOne(ctx, tbl, colFamilyRowKey, KEY_TO_READ)
		if err != nil {
			log.Printf("Warning: Column Family read one failed: %v", err)
		} else {
			colFamilyReadOneTimings = append(colFamilyReadOneTimings, durationColFamilyReadOne)
		}

		// 3. Read Entire Map
		durationBlobReadAll, err := benchmarkBlobReadAll(ctx, tbl, blobRowKey)
		if err != nil {
			log.Printf("Warning: Blob read all failed: %v", err)
		} else {
			blobReadAllTimings = append(blobReadAllTimings, durationBlobReadAll)
		}

		durationColFamilyReadAll, err := benchmarkColFamilyReadAll(ctx, tbl, colFamilyRowKey)
		if err != nil {
			log.Printf("Warning: Column Family read all failed: %v", err)
		} else {
			colFamilyReadAllTimings = append(colFamilyReadAllTimings, durationColFamilyReadAll)
		}

		// append all
		q := fmt.Sprintf("SELECT %s FROM %s WHERE _key = '%s'", COLUMN_FAMILY, tableID, colFamilyRowKey)
		durationSqlReadOne, err := benchmarkSqlRead(ctx, client, q)
		if err != nil {
			log.Printf("Warning: SQL read one failed: %v", err)
		} else {
			colFamilySqlReadAllTimings = append(colFamilySqlReadAllTimings, durationSqlReadOne)
		}

		// append single
		q = fmt.Sprintf("SELECT %s['%s'] FROM %s WHERE _key = '%s'", COLUMN_FAMILY, KEY_TO_READ, tableID, colFamilyRowKey)
		durationSqlReadOne, err = benchmarkSqlRead(ctx, client, q)
		if err != nil {
			log.Printf("Warning: SQL read one failed: %v", err)
		} else {
			colFamilySqlReadOneTimings = append(colFamilySqlReadOneTimings, durationSqlReadOne)
		}

		// blob SQL
		q = fmt.Sprintf("SELECT %s['%s'] FROM %s WHERE _key = '%s'", COLUMN_FAMILY, BLOB_COLUMN, tableID, blobRowKey)
		durationSqlReadOne, err = benchmarkSqlRead(ctx, client, q)
		if err != nil {
			log.Printf("Warning: SQL read one failed: %v", err)
		} else {
			blobSqlReadOneTimings = append(blobSqlReadOneTimings, durationSqlReadOne)
			blobSqlReadAllTimings = append(blobSqlReadAllTimings, durationSqlReadOne)
		}

		if (i+1)%100 == 0 {
			log.Printf("Completed %d/%d runs...", i+1, numRuns)
		}
	}

	// --- 4. Output Results ---
	log.Println("Benchmark complete. Calculating percentiles...")

	// Sort all slices
	sort.Slice(blobAppendTimings, func(i, j int) bool { return blobAppendTimings[i] < blobAppendTimings[j] })
	sort.Slice(blobReadOneTimings, func(i, j int) bool { return blobReadOneTimings[i] < blobReadOneTimings[j] })
	sort.Slice(blobReadAllTimings, func(i, j int) bool { return blobReadAllTimings[i] < blobReadAllTimings[j] })

	sort.Slice(colFamilyAppendTimings, func(i, j int) bool { return colFamilyAppendTimings[i] < colFamilyAppendTimings[j] })
	sort.Slice(colFamilyReadOneTimings, func(i, j int) bool { return colFamilyReadOneTimings[i] < colFamilyReadOneTimings[j] })
	sort.Slice(colFamilyReadAllTimings, func(i, j int) bool { return colFamilyReadAllTimings[i] < colFamilyReadAllTimings[j] })
	sort.Slice(blobSqlReadOneTimings, func(i, j int) bool { return blobSqlReadOneTimings[i] < blobSqlReadOneTimings[j] })
	sort.Slice(blobSqlReadAllTimings, func(i, j int) bool { return blobSqlReadAllTimings[i] < blobSqlReadAllTimings[j] })
	sort.Slice(colFamilySqlReadOneTimings, func(i, j int) bool { return colFamilySqlReadOneTimings[i] < colFamilySqlReadOneTimings[j] })
	sort.Slice(colFamilySqlReadAllTimings, func(i, j int) bool { return colFamilySqlReadAllTimings[i] < colFamilySqlReadAllTimings[j] })

	// Print all results
	printPercentiles("Blob Storage - Append", blobAppendTimings)
	printPercentiles("Col Family Storage - Append", colFamilyAppendTimings)

	printPercentiles("Blob Storage - Read One", blobReadOneTimings)
	printPercentiles("Col Family Storage - Read One", colFamilyReadOneTimings)

	printPercentiles("Blob Storage - Read All", blobReadAllTimings)
	printPercentiles("Col Family Storage - Read All", colFamilyReadAllTimings)

	// Sql results
	printPercentiles("blobSqlReadOneTimings", blobSqlReadOneTimings)
	printPercentiles("blobSqlReadAllTimings", blobSqlReadAllTimings)
	printPercentiles("colFamilySqlReadOneTimings", colFamilySqlReadOneTimings)
	printPercentiles("colFamilySqlReadAllTimings", colFamilySqlReadAllTimings)
}

// === Benchmark Functions: Append ===

// benchmarkBlobAppend performs the "append" operation by reading the JSON blob,
// deserializing, mutating, reserializing, and performing a conditional mutation.
func benchmarkBlobAppend(ctx context.Context, tbl *bigtable.Table, rowKey, newKey, newValue string) (time.Duration, error) {
	start := time.Now()

	// 1. Read the current value
	row, err := tbl.ReadRow(ctx, rowKey, bigtable.RowFilter(
		bigtable.ChainFilters(
			bigtable.FamilyFilter(COLUMN_FAMILY),
			bigtable.ColumnFilter(BLOB_COLUMN),
			bigtable.LatestNFilter(1),
		),
	))
	if err != nil {
		return 0, fmt.Errorf("ReadRow failed: %w", err)
	}

	var originalJSONBytes []byte
	if items, ok := row[COLUMN_FAMILY]; ok && len(items) > 0 {
		originalJSONBytes = items[0].Value
	} else {
		originalJSONBytes = []byte("{}") // Start with empty map
	}

	// 2. Deserialize, mutate, reserialize
	currentMap := make(map[string]string)
	if err := json.Unmarshal(originalJSONBytes, &currentMap); err != nil {
		return 0, fmt.Errorf("failed to deserialize JSON: %w", err)
	}

	currentMap[newKey] = newValue // Mutate

	newJSONBytes, err := json.Marshal(currentMap)
	if err != nil {
		return 0, fmt.Errorf("failed to reserialize JSON: %w", err)
	}

	// 3. Conditionally mutate the row
	filter := bigtable.ChainFilters(
		bigtable.FamilyFilter(COLUMN_FAMILY),
		bigtable.ColumnFilter(BLOB_COLUMN),
		bigtable.ValueFilter(string(originalJSONBytes)),
	)

	mut := bigtable.NewMutation()
	mut.Set(COLUMN_FAMILY, BLOB_COLUMN, bigtable.Now(), newJSONBytes)

	condMut := bigtable.NewCondMutation(filter, mut, nil)

	// 4. Apply the mutation
	if err := tbl.Apply(ctx, rowKey, condMut); err != nil {
		return 0, fmt.Errorf("Apply CondMutation failed: %w", err)
	}

	duration := time.Since(start)
	return duration, nil
}

// benchmarkColumnFamilyAppend performs the "append" operation by "blindly"
// setting the cell for the new key.
func benchmarkColumnFamilyAppend(ctx context.Context, tbl *bigtable.Table, rowKey, newKey, newValue string) (time.Duration, error) {
	start := time.Now()

	mut := bigtable.NewMutation()
	mut.Set(COLUMN_FAMILY, newKey, bigtable.Now(), []byte(newValue))

	if err := tbl.Apply(ctx, rowKey, mut); err != nil {
		return 0, fmt.Errorf("Apply mutation failed: %w", err)
	}

	duration := time.Since(start)
	return duration, nil
}

// === Benchmark Functions: Read One ===

// benchmarkBlobReadOne reads the entire blob, deserializes it, and accesses one key.
func benchmarkBlobReadOne(ctx context.Context, tbl *bigtable.Table, rowKey, keyToRead string) (time.Duration, error) {
	start := time.Now()

	// 1. Read the entire blob
	row, err := tbl.ReadRow(ctx, rowKey, bigtable.RowFilter(
		bigtable.ChainFilters(
			bigtable.FamilyFilter(COLUMN_FAMILY),
			bigtable.ColumnFilter(BLOB_COLUMN),
			bigtable.LatestNFilter(1),
		),
	))
	if err != nil {
		return 0, fmt.Errorf("ReadRow failed: %w", err)
	}

	var jsonBytes []byte
	if items, ok := row[COLUMN_FAMILY]; ok && len(items) > 0 {
		jsonBytes = items[0].Value
	} else {
		return 0, fmt.Errorf("blob not found")
	}

	// 2. Deserialize
	m := make(map[string]string)
	if err := json.Unmarshal(jsonBytes, &m); err != nil {
		return 0, fmt.Errorf("failed to deserialize JSON: %w", err)
	}

	// 3. Access the one element
	_ = m[keyToRead]

	duration := time.Since(start)
	return duration, nil
}

// benchmarkColFamilyReadOne reads exactly one column qualifier from the row.
func benchmarkColFamilyReadOne(ctx context.Context, tbl *bigtable.Table, rowKey, keyToRead string) (time.Duration, error) {
	start := time.Now()

	// 1. Read only the specific column
	filter := bigtable.ChainFilters(
		bigtable.FamilyFilter(COLUMN_FAMILY),
		bigtable.ColumnFilter(keyToRead), // Filter for the exact key
		bigtable.LatestNFilter(1),
	)

	_, err := tbl.ReadRow(ctx, rowKey, bigtable.RowFilter(filter))
	if err != nil {
		return 0, fmt.Errorf("ReadRow failed: %w", err)
	}

	duration := time.Since(start)
	return duration, nil
}

// === Benchmark Functions: Read All ===

// benchmarkBlobReadAll reads the entire blob and deserializes it.
func benchmarkBlobReadAll(ctx context.Context, tbl *bigtable.Table, rowKey string) (time.Duration, error) {
	start := time.Now()

	// 1. Read the entire blob
	row, err := tbl.ReadRow(ctx, rowKey, bigtable.RowFilter(
		bigtable.ChainFilters(
			bigtable.FamilyFilter(COLUMN_FAMILY),
			bigtable.ColumnFilter(BLOB_COLUMN),
			bigtable.LatestNFilter(1),
		),
	))
	if err != nil {
		return 0, fmt.Errorf("ReadRow failed: %w", err)
	}

	var jsonBytes []byte
	if items, ok := row[COLUMN_FAMILY]; ok && len(items) > 0 {
		jsonBytes = items[0].Value
	} else {
		return 0, fmt.Errorf("blob not found")
	}

	// 2. Deserialize
	m := make(map[string]string)
	if err := json.Unmarshal(jsonBytes, &m); err != nil {
		return 0, fmt.Errorf("failed to deserialize JSON: %w", err)
	}
	_ = m // We have the map

	duration := time.Since(start)
	return duration, nil
}

// benchmarkColFamilyReadAll reads all columns in the family for a row.
func benchmarkColFamilyReadAll(ctx context.Context, tbl *bigtable.Table, rowKey string) (time.Duration, error) {
	start := time.Now()

	// 1. Read the entire row (filtered to our family)
	// The resulting row object will contain all columns/values
	_, err := tbl.ReadRow(ctx, rowKey, bigtable.RowFilter(
		bigtable.FamilyFilter(COLUMN_FAMILY),
	))
	if err != nil {
		return 0, fmt.Errorf("ReadRow failed: %w", err)
	}

	duration := time.Since(start)
	return duration, nil
}

// --- Helper Functions ---

// initializeBlobCollection seeds a row with a JSON blob of a given size.
func initializeBlobCollection(ctx context.Context, tbl *bigtable.Table, rowKey string, initialMap map[string]string) error {
	jsonBytes, err := json.Marshal(initialMap)
	if err != nil {
		return err
	}

	mut := bigtable.NewMutation()
	mut.Set(COLUMN_FAMILY, BLOB_COLUMN, bigtable.Now(), jsonBytes)
	return tbl.Apply(ctx, rowKey, mut)
}

// initializeColumnFamilyCollection seeds a row with N columns.
func initializeColumnFamilyCollection(ctx context.Context, tbl *bigtable.Table, rowKey string, initialMap map[string]string) error {
	mut := bigtable.NewMutation()
	for k, v := range initialMap {
		mut.Set(COLUMN_FAMILY, k, bigtable.Now(), []byte(v))
	}
	return tbl.Apply(ctx, rowKey, mut)
}

// generateMap creates a simple map[string]string of a given size.
func generateMap(size int) map[string]string {
	m := make(map[string]string, size)
	for i := 0; i < size; i++ {
		m[fmt.Sprintf("initial-key-%d", i)] = fmt.Sprintf("initial-value-%d", i)
	}
	return m
}

// getEnv reads an environment variable or returns a fallback string.
func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// === Benchmark Functions: SQL (NEW) ===

// benchmarkSqlReadAll reads all columns in the family using a Bigtable SQL query.
func benchmarkSqlRead(ctx context.Context, client *bigtable.Client, query string) (time.Duration, error) {
	stmt, err := client.PrepareStatement(ctx, query, nil)
	if err != nil {
		return 0, fmt.Errorf("ExecuteQuery failed: %w", err)
	}

	start := time.Now()
	b, err := stmt.Bind(nil)
	count := 0
	err = b.Execute(ctx, func(row bigtable.ResultRow) bool {
		count += 1
		return true
	})

	duration := time.Since(start)
	//log.Printf("executing query: got %d from %s", count, query)
	return duration, nil
}

// printPercentiles calculates and prints the p50, p90, p99, and p100 latencies.
func printPercentiles(name string, timings []time.Duration) {
	if len(timings) == 0 {
		log.Printf("--- Results for %s (0 samples) ---", name)
		log.Println("No data captured.")
		return
	}

	p50 := timings[int(float64(len(timings))*0.50)]
	p90 := timings[int(float64(len(timings))*0.90)]
	p99 := timings[int(float64(len(timings))*0.99)]
	p100 := timings[len(timings)-1] // p100 is the max value

	fmt.Println("---")
	log.Printf("--- Results for %s (%d samples) ---", name, len(timings))
	fmt.Printf("p50 (Median): \t%s\n", p50)
	fmt.Printf("p90: \t\t%s\n", p90)
	fmt.Printf("p99: \t\t%s\n", p99)
	fmt.Printf("p100 (Max): \t%s\n", p100)
	fmt.Println("---")
}
