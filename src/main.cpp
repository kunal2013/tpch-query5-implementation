#include "query5.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <map>
#include <chrono>  // for time measurement

// TODO: Include additional headers as needed

int main(int argc, char* argv[]) 
{
    std::string r_name, start_date, end_date, table_path, result_path;  // r_name = region name
    int num_threads;

    if (!parseArgs(argc, argv, r_name, start_date, end_date, num_threads, table_path, result_path)) 
    {
        std::cerr << "Failed to parse command line arguments." << std::endl;
        return 1;
    }   

    std::vector<std::map<std::string, std::string>> customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data;

    auto start_time = std::chrono::steady_clock::now();  // start timer

    if (!readTPCHData(table_path, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data)) 
    {
        std::cerr << "Failed to read TPCH data." << std::endl;
        return 1;
    }

    std::map<std::string, double> results;

    if (!executeQuery5(r_name, start_date, end_date, num_threads, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data, results)) {
        std::cerr << "Failed to execute TPCH Query 5." << std::endl;
        return 1;
    }

    if (!outputResults(result_path, results)) {
        std::cerr << "Failed to output results." << std::endl;
        return 1;
    }

    auto end_time = std::chrono::steady_clock::now();  // end timer
    std::chrono::duration<double> elapsed = end_time - start_time ;

    std::cout << "TPCH Query 5 implementation completed." << std::endl;
    std::cout << "Execution time: " << elapsed.count() << " seconds." << std::endl;
    return 0;
} 