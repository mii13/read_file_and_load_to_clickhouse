#include <iostream>
#include <fstream>      // std::ifstream
#include <sstream> // std::stringstream
#include <vector>
#include <math.h>
#include "queue.cpp"
#include "sensor_map.h"
#include <thread>

#include <clickhouse/client.h>


using namespace clickhouse;


string get_env(const string& variable_name, const string& default_value)
{
    const char* value = getenv(variable_name.c_str());
    return value ? value : default_value;
}

const string CLICKHOUSE_HOST = get_env("CLICKHOUSE_HOST", "localhost");
const string CLICKHOUSE_PASSWORD = get_env("CLICKHOUSE_PASSWORD", "");
const int CLICKHOUSE_PORT = stoi(get_env("CLICKHOUSE_PORT", "9000"));

struct RecordTelemetry
{
    double ts;
    std::string sensor;
    float value_float;
    std::string value_str;
};


void create_table(Client &client){
    client.Execute(
            "CREATE TABLE IF NOT EXISTS telemetry\n"
            "(\n"
            "  dt          DateTime default toDateTime(ts),\n"
            "  ts          Float64,\n"
            "  sensor      String,\n"
            "  value_float Nullable(Float32),\n"
            "  value_str   Nullable(String),\n"
            "  stock_num   String,\n"
            "  upload_id   String,\n"
            "  head        FixedString(2)\n"
            ")\n"
            "  engine = ReplacingMergeTree() PARTITION BY toMonday(toDateTime(ts)) ORDER BY (stock_num, sensor, ts) "
            " SAMPLE BY ts SETTINGS index_granularity = 8192;"
            );
}

void load_to_clickhouse(RecordTelemetry * records, int n){
    ClientOptions option = ClientOptions();
    option.SetPort(CLICKHOUSE_PORT);
    option.SetPassword(CLICKHOUSE_PASSWORD);
    option.SetHost(CLICKHOUSE_HOST);
    Client client(option);

    /// Insert a few blocks.
    //std::cout << "start load to ch" << std::endl;
    int i(0), insert_count(n);
    RecordTelemetry record;
    while (i < n) {
        Block b;

        auto ts = std::make_shared<ColumnFloat64>();
        auto sensor = std::make_shared<ColumnString>();
        auto value_float = std::make_shared<ColumnFloat32>();
        auto value_float_nulls = std::make_shared<ColumnUInt8>();
        auto value_str = std::make_shared<ColumnString>();
        auto value_str_nulls = std::make_shared<ColumnUInt8>();
        auto stock_num = std::make_shared<ColumnString>();
        auto upload_id = std::make_shared<ColumnString>();
        auto head = std::make_shared<ColumnFixedString>(2);
        int j(0);
        while (j < insert_count || i < n) {
            record = records[i];
            ts->Append(record.ts);
            sensor->Append(record.sensor);
            if (isnan(record.value_float)) {
                value_float->Append(0.0);
                value_float_nulls->Append(1);
                value_str_nulls->Append(0);
                value_str->Append(record.value_str);
            } else {
                value_float->Append(record.value_float);
                value_float_nulls->Append(0);
                value_str_nulls->Append(1);
                value_str->Append("");
            }
            stock_num->Append("1");
            upload_id->Append("345");
            head->Append("10");
            ++i;
            ++j;
        }
        b.AppendColumn("ts", ts);
        b.AppendColumn("sensor", sensor);
        b.AppendColumn("value_float", std::make_shared<ColumnNullable>(value_float, value_float_nulls));
        b.AppendColumn("value_str", std::make_shared<ColumnNullable>(value_str, value_str_nulls));
        b.AppendColumn("stock_num", stock_num);
        b.AppendColumn("upload_id", upload_id);
        b.AppendColumn("head", head);
        client.Insert("telemetry", b);
    }
    //std::cout << "end load to ch" << std::endl;
}

void read_file(QueueThreadSafe<string> &q,  string file_name, int max_size, int th_count)
{
    std::ifstream file(file_name);
    if(!file.is_open()) throw std::runtime_error("Could not open file");

    std::string row;
    std::string parameter, type;
    while(std::getline(file, row)) {

        if (row.length() == 0 || row[0] == '#') {
            continue;
        }
        if(q.size() >= max_size){
            std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  block queue !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << std::endl;
            std::this_thread::sleep_for(5000000us);
        }
        q.push(row);
        if (file.bad() || file.fail()) {
            break;;
        }
    }
    for(int i(0); i < th_count; ++i){
        q.push("");
    }
    std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  Finish read file !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << std::endl;
}


optional<RecordTelemetry> parse_row(string row, const sensors_info_t &sensors){

    std::stringstream ss(row);
    std::string parameter, type;
    RecordTelemetry record;

    std::getline(ss, parameter, '\t');
    try {
        record.ts = std::stod(parameter);
    }
    catch (std::exception &e) {
        return std::nullopt;
    }
    std::getline(ss, parameter, '\t');
    if (parameter.empty()){
        return std::nullopt;
    }
    parameter.erase(0, 1);
    std::replace( parameter.begin(), parameter.end(), '/', '_');
    try {
        type = sensors.at(parameter);
    }
    catch (std::out_of_range &){
        return std::nullopt;
    }

    record.sensor = parameter;
    std::getline(ss, parameter, '\t');
    if (type == "float") {
        try {
            record.value_float = std::stof(parameter);
            record.value_str = "";
        }
        catch (std::exception &e) {
            //std::cout << e << std::endl;
            return std::nullopt;
        }
    }
    else
    {
        record.value_str = parameter;
        record.value_float = std::numeric_limits<float>::quiet_NaN();
    }
    return record;
}

void parse(QueueThreadSafe<string> &q, const sensors_info_t &sensors)
{
    int max_size(500000), i(0);
    RecordTelemetry *records = new RecordTelemetry[max_size];
    while (true)
    {
        std::optional<string> row = q.pop();
        if(not row.has_value()) {
            std::this_thread::sleep_for(100us);
            continue;
        }
        if(row.value().empty()) {
            break;
        }

        optional<RecordTelemetry> record = parse_row(row.value(), sensors);
        //std::cout << row.value();
        if(not record.has_value()) {
            continue;
        }
        records[i] = record.value();
        ++i;
        if (i == max_size){
            //std::this_thread::sleep_for(2000us);
            //std::cout << "load to ch" << std::endl;

            load_to_clickhouse(records, i);
            i = 0;
        }
    }

    if( i > 0){
        load_to_clickhouse(records, i);
    }
    delete [] records;
}

void load_in_thread(string file_name, sensors_info_t &sensors, int thread_count=3, int max_size=5000000){
    std::vector<std::thread> threads;
    QueueThreadSafe<string> q;
    threads.push_back(std::thread(read_file, std::ref(q), file_name,  max_size, thread_count));
    for(int i(0); i < thread_count; ++i) {
        threads.push_back(std::thread(parse, std::ref(q), std::ref(sensors)));
    }
    std::for_each(threads.begin(), threads.end(), std::mem_fn(&std::thread::join));

    optional<string> r = q.pop();
    while (r){
        r = q.pop();
    }
}