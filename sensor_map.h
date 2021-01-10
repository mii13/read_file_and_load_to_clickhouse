#ifndef CLICKHOUSE_LOADER_SENSOR_MAP_H
#define CLICKHOUSE_LOADER_SENSOR_MAP_H

#include <string>
#include <unordered_map>


typedef std::unordered_map<std::string, std::string> sensors_info_t;


sensors_info_t get_sensor_mapping(const std::string filename);
#endif //CLICKHOUSE_LOADER_SENSOR_MAP_H
