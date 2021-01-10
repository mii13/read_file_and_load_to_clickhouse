#include <iostream>
#include <chrono>
#include "loader.cpp"
#include "sensor_map.h"

using namespace std::chrono;

void only_read_file(const string & filename, sensors_info_t &sensors){
    std::ifstream file(filename);
    std::string row;
    if(!file.is_open()) throw std::runtime_error("Could not open file");
    while(std::getline(file, row)) {

        if (row.length() == 0 || row[0] == '#') {
            continue;
        }
        if (file.bad() || file.fail()) {
            break;;
        }
        parse_row(row, sensors);
    }
}

int main(int argc, char *argv[]) {
    if (argc != 3){
        std::cout << "Usage example: `app sensors_filename metrics_filename`" << std::endl;
        exit(EXIT_FAILURE);
    }
    string sensors_filename(argv[1]);
    string metrics_filename(argv[2]);

    auto sensor_mapping = get_sensor_mapping(sensors_filename);

    std::cout << "Start for: " << metrics_filename << std::endl;
    auto start = high_resolution_clock::now();
    load_in_thread(metrics_filename, sensor_mapping);
    auto stop = high_resolution_clock::now();

    auto duration = duration_cast<microseconds>(stop - start);

    std::cout << "Time taken by function: "
    << duration.count()  / 1e+6 << " seconds" << std::endl;

    return EXIT_SUCCESS;
}
