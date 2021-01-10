#include <fstream>
#include <unordered_map>
#include <sstream>
#include "sensor_map.h"
using namespace std;

sensors_info_t get_sensor_mapping(const string filename){
    sensors_info_t sensors;

    std::ifstream file(filename);
    std::string row, sensor, type;
    if(!file.is_open()) throw std::runtime_error("Could not open file");
    while(std::getline(file, row)) {
        if (file.bad() || file.fail()) {
            break;;
        }
        std::stringstream ss(row);
        std::getline(ss, sensor, ',');
        std::getline(ss, type, ',');
        sensors[sensor] = type;
    }

    return sensors;
}