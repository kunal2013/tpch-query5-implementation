#include "query5.hpp"

#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <unordered_map>
#include <iomanip>   // for fixed  & set presicion 

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) 
{
    // TODO: Implement command line argument parsing
    // Example: --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path /path/to/tables --result_path /path/to/results

    for (int i = 1; i < argc; i += 2) 
    {
        std::string flag = argv[i];
        if (i + 1 >= argc) {
            std::cerr << "Missing value for " << flag << std::endl;
            return false;
        }
        std::string val = argv[i + 1];

        if (flag == "--r_name") 
            r_name = val;
        else 
        if (flag == "--start_date") 
            start_date = val;
        else 
        if (flag == "--end_date") 
            end_date = val;
        else 
        if (flag == "--threads") 
            num_threads = std::stoi(val);
        else 
        if (flag == "--table_path") 
            table_path = val;
        else 
        if (flag == "--result_path") 
            result_path = val;
        else 
        {
            std::cerr << "Unknown flag: " << flag << std::endl;
            return false;
        }
    }

    if (r_name.empty() || start_date.empty() || end_date.empty() || table_path.empty() || result_path.empty() || num_threads <= 0) 
        return false;
    
    return true;
}

std::vector<std::map<std::string, std::string>> readTable(const std::string& file_path, const std::vector<std::string>& columns) 
{
    std::vector<std::map<std::string, std::string>> data;
    std::ifstream file(file_path);
    if (!file.is_open()) 
	{
        std::cerr << "Failed to open " << file_path << std::endl;
        return data;
    }

    std::string line;
	int count = 0;
    while (std::getline(file, line) && count < 1000000) 
	{
        std::map<std::string, std::string> row;
        std::stringstream ss(line);
        std::string field;

        int col_idx = 0;
		
        while (std::getline(ss, field, '|') && col_idx < columns.size()) 
            row[columns[col_idx++]] = field;
        
        data.push_back(row);
        //std::cout << "count = " << count << std::endl ;
		count++ ;
    }
    std::cout << " | Line read = " << count << std::endl ;
    return data;
}

// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path, std::vector<std::map<std::string, std::string>>& customer_data, std::vector<std::map<std::string, std::string>>& orders_data, std::vector<std::map<std::string, std::string>>& lineitem_data, std::vector<std::map<std::string, std::string>>& supplier_data, std::vector<std::map<std::string, std::string>>& nation_data, std::vector<std::map<std::string, std::string>>& region_data) 
{
    // TODO: Implement reading TPCH data from files
    std::string path = table_path + "/";

    // Region
    std::vector<std::string> region_cols = {"r_regionkey", "r_name", "r_comment"};
    std::cout << path << "region.tbl" ;
    region_data = readTable(path + "region.tbl", region_cols);

    // Nation
    std::vector<std::string> nation_cols = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
    std::cout << path << "nation.tbl" ;
    nation_data = readTable(path + "nation.tbl", nation_cols);

    // Supplier
    std::vector<std::string> supplier_cols = {"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"};
    std::cout << path << "supplier.tbl" ;
    supplier_data = readTable(path + "supplier.tbl", supplier_cols);

    // Customer
    std::vector<std::string> customer_cols = {"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"};
    std::cout << path << "customer.tbl" ;
    customer_data = readTable(path + "customer.tbl", customer_cols);

    // Orders
    std::vector<std::string> orders_cols = {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"};
    std::cout << path << "order.tbl" ;
    orders_data = readTable(path + "orders.tbl", orders_cols);

    // Lineitem
    std::vector<std::string> lineitem_cols = {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"};
    std::cout << path << "lineitem.tbl" ;
    lineitem_data = readTable(path + "lineitem.tbl", lineitem_cols);

    if (region_data.empty() || nation_data.empty() || supplier_data.empty() || customer_data.empty() || orders_data.empty() || lineitem_data.empty()) {
        return false;
    }

    return true;
}

int dateToInt(const std::string& date_str)    // String: 2024-03-15, this function is not validating date 
{
    std::stringstream ss(date_str);
    std::string year, month, day;
    std::getline(ss, year, '-');
    std::getline(ss, month, '-');
    std::getline(ss, day, '-');
    return std::stoi(year) * 10000 + std::stoi(month) * 100 + std::stoi(day);    // 20240315
}

void processChunk(
    const std::vector<std::map<std::string, std::string>>& lineitem_chunk,
    const std::unordered_map<std::string, std::map<std::string, std::string>>& orders_map,
    const std::unordered_map<std::string, std::map<std::string, std::string>>& supplier_map,
    const std::unordered_map<std::string, std::map<std::string, std::string>>& customer_map,
    const std::unordered_map<std::string, std::map<std::string, std::string>>& nation_map,
    const std::unordered_map<std::string, std::map<std::string, std::string>>& region_map,
    const std::string& r_name,
    int start_date_int,
    int end_date_int,
    std::map<std::string, double>& results,          // <-- this is the shared results map
    std::mutex& results_mutex
) 
{
    std::map<std::string, double> local;  // thread-local partial results

    for (const auto& lineitem : lineitem_chunk) {
        std::string orderkey = lineitem.at("l_orderkey");
        auto orders_it = orders_map.find(orderkey);
        if (orders_it == orders_map.end()) continue;

        const auto& orders = orders_it->second;
        std::string orderdate = orders.at("o_orderdate");
        int orderdate_int = dateToInt(orderdate);
        if (orderdate_int < start_date_int || orderdate_int >= end_date_int) continue;

        std::string custkey = orders.at("o_custkey");
        auto customer_it = customer_map.find(custkey);
        if (customer_it == customer_map.end()) continue;

        const auto& customer = customer_it->second;
        std::string nationkey_c = customer.at("c_nationkey");

        std::string suppkey = lineitem.at("l_suppkey");
        auto supplier_it = supplier_map.find(suppkey);
        if (supplier_it == supplier_map.end()) continue;

        const auto& supplier = supplier_it->second;
        std::string nationkey_s = supplier.at("s_nationkey");

        if (nationkey_c != nationkey_s) continue;

        auto nation_it = nation_map.find(nationkey_s);
        if (nation_it == nation_map.end()) continue;

        const auto& nation = nation_it->second;
        std::string regionkey = nation.at("n_regionkey");

        auto region_it = region_map.find(regionkey);
        if (region_it == region_map.end()) continue;

        const auto& region = region_it->second;
        if (region.at("r_name") != r_name) continue;

        double extendedprice = std::stod(lineitem.at("l_extendedprice"));
        double discount = std::stod(lineitem.at("l_discount"));
        double revenue = extendedprice * (1.0 - discount);

        std::string n_name = nation.at("n_name");
        local[n_name] += revenue;
    }

    // Merge local results into shared results (protected by mutex)
    {
        std::lock_guard<std::mutex> lock(results_mutex);
        for (const auto& kv : local) {
            results[kv.first] += kv.second;
        }
    }
}

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, const std::vector<std::map<std::string, std::string>>& customer_data, const std::vector<std::map<std::string, std::string>>& orders_data, const std::vector<std::map<std::string, std::string>>& lineitem_data, const std::vector<std::map<std::string, std::string>>& supplier_data, const std::vector<std::map<std::string, std::string>>& nation_data, const std::vector<std::map<std::string, std::string>>& region_data, std::map<std::string, double>& results) 
{
    // TODO: Implement TPCH Query 5 using multithreading

    // Build hash maps for fast lookup
    std::unordered_map<std::string, std::map<std::string, std::string>> orders_map;
    for (const auto& ord : orders_data) {
        orders_map[ord.at("o_orderkey")] = ord;
    }

    std::unordered_map<std::string, std::map<std::string, std::string>> supplier_map;
    for (const auto& sup : supplier_data) {
        supplier_map[sup.at("s_suppkey")] = sup;
    }

    std::unordered_map<std::string, std::map<std::string, std::string>> customer_map;
    for (const auto& cust : customer_data) {
        customer_map[cust.at("c_custkey")] = cust;
    }

    std::unordered_map<std::string, std::map<std::string, std::string>> nation_map;
    for (const auto& nat : nation_data) {
        nation_map[nat.at("n_nationkey")] = nat;
    }

    std::unordered_map<std::string, std::map<std::string, std::string>> region_map;
    for (const auto& reg : region_data) {
        region_map[reg.at("r_regionkey")] = reg;
    }

    int start_date_int = dateToInt(start_date);
    int end_date_int = dateToInt(end_date);

    // Multithreading
    std::vector<std::thread> threads;
    std::mutex results_mutex;

    size_t chunk_size = lineitem_data.size() / num_threads;
    for (int i = 0; i < num_threads; ++i) {
        size_t start = i * chunk_size;
        size_t end = (i == num_threads - 1) ? lineitem_data.size() : (i + 1) * chunk_size;
        std::vector<std::map<std::string, std::string>> chunk(lineitem_data.begin() + start, lineitem_data.begin() + end);
        threads.emplace_back(processChunk, chunk, orders_map, supplier_map, customer_map, nation_map, region_map, r_name, start_date_int, end_date_int, std::ref(results), std::ref(results_mutex));
    }

    for (auto& th : threads) {
        th.join();
    }

    return true;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) 
{
    std::vector<std::pair<std::string, double>> sorted_results(results.begin(), results.end());
    std::sort(sorted_results.begin(), sorted_results.end(), [](const auto& a, const auto& b) {
        return a.second > b.second;
    });

    std::ofstream file(result_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open " << result_path << std::endl;
        return false;
    }

    for (const auto& res : sorted_results) {
        file << res.first << " | " 
             << std::fixed << std::setprecision(4)    // with fixed we will get fixed 4 after decimal 232.4000, with only setprecision we might get only 4 digits like 232.4
             << res.second << "\n";
    }

    return true;
} 