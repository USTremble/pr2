#ifndef PR1MAIN_HELP_H
#define PR1MAIN_HELP_H

#include "vector.h"
#include "hash.h"
#include <fstream>
#include <filesystem>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <thread>
#include <arpa/inet.h>

namespace fs = std::filesystem;

string toString(const int& val) {
    string str;
    int digit = val;

    while (digit > 0) {
        int temp = digit % 10;
        str = static_cast<char>(temp + '0') + str;
        digit /= 10;
    }

    return str;
}

int toInt(const string& str) {
    int result = 0;
    for (int i = 0; i < str.size(); ++i) {
        char ch = str[i];
        if (ch < '0' || ch > '9') {
            throw invalid_argument("Invalid character in string");
        }
        result = result * 10 + (ch - '0');
    }
    return result;
}

void checkArgumentsCount(Vector<string>& tokens, int expected) {
    if (tokens.size() != expected) {
        throw runtime_error("Incorrect count of arguments");
    }
}

bool isLocked(const string& path) {
    string val;

    ifstream file(path);
    file >> val;
    file.close();

    if (val == "1") {
        return true;
    } else {
        return false;
    }
}

void lock(const string& path) {
    if (isLocked(path)) {
        throw runtime_error("Table locked");
    }

    ofstream file(path);
    file << "1";
    file.close();
}


void unlock (const string& path) {
    ofstream file(path);
    file << "0";
    file.close();
}

void lockAll(Vector<string>& tables, const string& name) {
    for (int i = 0; i < tables.size(); i++) {
        string tableName = tables.get(i);
        string path = name + + "/" + tableName + "/" + tableName + "_lock.txt";

        try {
            lock(path);
        } catch (exception& e) {
            throw runtime_error(tableName + " already blocked");
        }
    }
}

void unlockAll(Vector<string>& tables, const string& name) {
    for (int i = 0; i < tables.size(); i++) {
        string tableName = tables.get(i);
        string path = name + + "/" + tableName + "/" + tableName + "_lock.txt";
        unlock(path);
    }
}

int getCsvNum(const string& tablePath) {
    int index = 1;
    int countCsv = 0;
    while (true) {
        string csvPath = tablePath + "/" + toString(index) + ".csv";
        if (fs::exists(csvPath)) {
            index++;
            countCsv++;
        } else {
            break;
        }
    }
    return countCsv;
}

string trim(const string& inputStr, char ch = ' ') {
    int start = 0;
    while (start < inputStr.size() && inputStr[start] == ch) {
        start++;
    }

    if (start == inputStr.size()) {
        return "";
    }

    int end = inputStr.size() - 1;
    while (end > start && inputStr[end] == ch) {
        end--;
    }

    return inputStr.substr(start, end - start + 1);
}


Vector<string> split(const string& str, const string& delimiter) {
    Vector<string> values;
    int start = 0;
    int end = 0;

    string tempStr = str;
    tempStr = trim(tempStr, '\t');

    while ((end = tempStr.find(delimiter, start)) != string::npos) {
        string value = tempStr.substr(start, end - start);
        values.pushBack(trim(value));

        start = end + delimiter.length();
    }

    string lastValue = tempStr.substr(start);
    values.pushBack(trim(lastValue));

    return values;
}

Vector<Vector<string>> readCSV(const string& path) {
    Vector<Vector<string>> result;
    ifstream csvFile(path);

    string line;
    while (getline(csvFile, line)) {
        Vector<string> str;
        stringstream lineStream(line);
        string cell;

        while (getline(lineStream, cell, ',')) {
            str.pushBack(cell);
        }

        result.pushBack(str);
    }
    csvFile.close();

    return result;
}

string getHeader(const string& path) {
    int csvNum = getCsvNum(path);
    Vector<string> csvFiles;
    for (int i = 1; i <= csvNum; i++) {
        string currentCsvPath = path + "/" + toString(i) + ".csv";
        csvFiles.pushBack(currentCsvPath);
    }

    if (csvFiles.size() == 0) {
        throw runtime_error("Problems with csv-file");
    }

    ifstream file(csvFiles.get(0));
    string firstLine;
    getline(file, firstLine);
    file.close();
    return firstLine;
}

string dotCut(string& rawString, int part) {
    int indexDot = rawString.find('.');

    if (indexDot == string::npos) {
        throw runtime_error("Invalid format " + rawString);
    } else {
        if (part == 1) {
            return rawString.substr(0, indexDot);
        } else if (part == 2) {
            return rawString.substr(indexDot + 1);
        }
    }

    return "";
}

int getColumnIndex(string& tableColumn, const string& path) {
    int dotIndex = tableColumn.find('.');
    if (dotIndex == string::npos) {
        throw invalid_argument("Invalid format: " + tableColumn);
    }

    string columnName = dotCut(tableColumn, 2);
    string firstLine = getHeader(path);

    Vector<string> header = split(firstLine, ",");
    for (int i = 0; i < header.size(); i++) {
        if (header.get(i) == columnName) {
            return i;
        }
    }

    throw runtime_error("Something went wrong with " + tableColumn);
}

#endif //PR1MAIN_HELP_H
