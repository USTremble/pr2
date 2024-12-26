#ifndef PR1MAIN_READJSON_H
#define PR1MAIN_READJSON_H

#include "help.h"
#include "json.hpp"

using json = nlohmann::json;

struct Schema {
    string name;
    int tuplesLimit;
    HashTable<Vector<string>> structure;
};

Schema readJson(const string& path) {
    Schema schema;
    json jsonData;
    fstream file(path);

    if (file.is_open()) {
        file >> jsonData;
        file.close();
    } else {
        cerr << "Failed to open file" << endl;
    }


    schema.name = jsonData["name"];
    schema.tuplesLimit = jsonData["tuples_limit"];

    auto structure = jsonData["structure"];
    for (auto it = structure.begin(); it != structure.end(); it++) {
        Vector<string> columns;
        for (const auto& column : it.value()) {
            columns.pushBack(column);
        }
        schema.structure.put(it.key(), columns);
    }

    return schema;
}

void createDir(const Schema& schema) {
    fs::create_directory(schema.name); // основная

    Vector<string> listTables = schema.structure.keys(); // список таблиц
    try {
        for (int i = 0; i < listTables.size(); ++i) {
            string tableName = listTables.get(i);
            Vector<string> columns = schema.structure.get(tableName);

            string tablePath = schema.name + "/" + tableName;
            if (!fs::exists(tablePath)) {
                fs::create_directory(tablePath); // директория для таблиц
            }

            int fileIndex = 1;
            string csvPath = tablePath + "/" + toString(fileIndex) + ".csv";
            if (!fs::exists(csvPath)) {
                ofstream csv(csvPath);
                csv << tableName << "_pk,";
                for (int j = 0; j < columns.size(); ++j) {
                    csv << columns.get(j);
                    if (j < columns.size() - 1) {
                        csv << ",";
                    }
                }
                csv << endl;
                csv.close();
            }

            string pkPath = tablePath + "/" + tableName + "_pk_sequence.txt";
            if (!fs::exists(pkPath)) {
                ofstream pkFile(pkPath);
                pkFile << "1";
                pkFile.close();
            }

            string lockPath = tablePath + "/" + tableName + "_lock.txt";
            if (!fs::exists(lockPath)) {
                ofstream lockFile(lockPath);
                lockFile.close();
            }
        }
        cout << "All directories are created" << endl;
    } catch (const std::exception& e) {
        cerr << e.what() << endl;
    }
}

#endif //PR1MAIN_READJSON_H
