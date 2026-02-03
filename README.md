# TPCH Query 5 Implementation

## 📊 Performance Results
| Threads | Execution Time | Result File |
|---------|----------------|-------------|
| 1       | 302.588 seconds | [results/single.txt](results/single.txt) |
| 4       | 145.561 seconds | [results/four.txt](results/four.txt) |

**Speedup:** 2.08x with 4 threads

## 🖼️ Screenshot
![Performance Output](screenshots/performance.png)

## 🚀 Execution Commands
```bash
# Single thread
.\tpch_query5.exe --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 1 --table_path E:\tpc_db\db_tbl --result_path results/single.txt

# Multi-thread (4 threads)
.\tpch_query5.exe --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path E:\tpc_db\db_tbl --result_path results/four.txt

---

## 📂 Project Structure

```text
tpch-query5/
├── src/                    # Source code
│   ├── main.cpp            # Entry point with argument parsing & timing
│   └── query5.cpp          # Core logic: parsing, data loading, query execution
├── include/                # Header files
│   └── query5.hpp
├── results/                # Output files from runs
│   ├── single.txt          # Single-thread results
│   └── four.txt            # 4-thread results
├── screenshots/            # Performance screenshots
│   ├── single_thread.png
│   ├── four_threads.png
│   └── results_comparison.png
├── CMakeLists.txt          # Build configuration
└── README.md               # This file

Build Instructions

step1 : mkdir build
step2 : cd build
step3 : cmake .. -G "MinGW Makefiles"   # for window like system 
step4 : mingw32-make

## ⚠️ Note on Data Processing

Due to system memory constraints, I limited the data processing to **10 crore (100 million) lines** from the TPC-H data tables. Without this limit:

- System experienced frequent **hanging and crashes**
- **Stack overflow errors** occurred during full dataset processing  
- Memory usage exceeded available system resources
