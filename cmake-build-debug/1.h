#ifndef PR1MAIN_REQUEST_H
#define PR1MAIN_REQUEST_H

#include "readJson.h"
#include "help.h"

// INSERT INTO table1 VALUES 4,5,6,7
void insert(Vector<string>& values, const string& tableName, Schema& schema) {
    string tablePath = schema.name + "/" + tableName;
    string pkPath = tablePath + "/" + tableName + "_pk_sequence.txt";
    string pathToUnlock = schema.name + "/" + tableName + "/" + tableName + "_lock.txt";

    Vector<string> table = schema.structure.get(tableName);
    if (values.size() != table.size()) {
        unlock(pathToUnlock);
        throw runtime_error("Incorrect count of values");
    }


    int key = 0; // первичный ключ
    ifstream pkFile(pkPath);
    if (pkFile) {
        pkFile >> key;
    }
    pkFile.close();


    string insertFile;
    int csvNum = 1;
    while (true) {
        int numLines = 0; // число строк
        string csvString;
        insertFile = tablePath + "/" + toString(csvNum) + ".csv";

        ifstream csvCurFile(insertFile);
        while (getline(csvCurFile, csvString)){
            numLines++;
        }

        if (numLines < schema.tuplesLimit) {
            break;
        }
        csvNum++; // новый файл если превысили лимит
    }

    ofstream csvFile(insertFile, ios::app);
    if (!csvFile.is_open()) {
        unlock(pathToUnlock);
        throw runtime_error("Error when opening file");
    }

    csvFile << key << ',';
    for (int i = 0; i < values.size(); i++) {
        csvFile << values.get(i); // добавляем

        if (values.size() - 1 != i) { // если это не последний элемент
            csvFile << ',';
        }
    }
    csvFile << endl;
    csvFile.close();

    key++;
    ofstream pkFileSave(pkPath);
    pkFileSave << key;
    pkFileSave.close();

    unlock(pathToUnlock);
}

void preInsert(Vector<string>& tokens, Schema& schema) {
    checkArgumentsCount(tokens, 5);

    string tableName = tokens.get(2); // название таблицы
    string value = tokens.get(4); // элементы
    Vector<string> values; // "чистые" элементы

    if (value[0] == '(' && value[value.size() - 1] == ')') {
        value = value.substr(1, value.size() - 2);

        values = split(value, ',');
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i)[0] == '\'' && values.get(i)[values.get(i).size() - 1] == '\'') {
                values.set(i, values.get(i).substr(1, values.get(i).size() - 2));
            }
        }
    } else {
        values = split(value, ',');
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i)[0] == '\'' && values.get(i)[values.get(i).size() - 1] == '\'') {
                values.set(i, values.get(i).substr(1, values.get(i).size() - 2));
            }
        }
    }

    string pathToLock = schema.name + "/" + tableName + "/" + tableName + "_lock.txt";
    lock(pathToLock);
    insert(values, tableName, schema);
}

void deleteFunc(Vector<string>& directory, string& val, Schema& schema) {
    string tableName = directory.get(0);
    string columnName = directory.get(1);
    string tablePath = schema.name + "/" + tableName;
    string pkPath = tablePath + "/" + tableName + "_pk_sequence.txt";
    string pathToUnlock = schema.name + "/" + tableName + "/" + tableName + "_lock.txt";


    int columnIndex = -1; // получаем индекс столбца
    Vector<string> tableStructure = schema.structure.get(tableName);
    for (int i = 0; i < tableStructure.size(); i++) {
        if (tableStructure.get(i) == columnName) {
            columnIndex = i;
            break;
        }
    }
    if (columnIndex == -1) {
        unlock(pathToUnlock);
        throw runtime_error("Column not found");
    }
    columnIndex++; // тк первым идёт первичный ключ

    int csvNum = getCsvNum(tablePath); // общее количество файлов в директории
    int maxPk = 0; // наибольший ключ после удаления строк

    for (int i = 1; i <= csvNum; i++) {
        string filePath = tablePath + "/" + toString(i) + ".csv";
        ifstream csvFile(filePath);

        Vector<string> buffer;
        string line;
        bool isFirstLine = true;
        while (getline(csvFile, line)) {
            if (isFirstLine) {
                buffer.pushBack(line);
                isFirstLine = false;
                continue;
            }

            Vector<string> lineElements = split(line, ',');
            if (lineElements.get(columnIndex) == val) {
                continue; // если есть такой столбец в строке и там есть значение, то просто пропускаем строку
            }

            int currentPk = toInt(lineElements.get(0)); // первым записан первичный ключ
            if (currentPk > maxPk) { // проверяем если
                maxPk = currentPk;
            }

            buffer.pushBack(line);
        }
        csvFile.close();

        ofstream writeToCsv(filePath, ios::trunc);
        for (int i = 0; i < buffer.size(); i++) {
            writeToCsv << buffer.get(i) << endl;
        }
        writeToCsv.close();
    }

    ofstream writeToPk(pkPath, ios::trunc);
    writeToPk << maxPk + 1;
    writeToPk.close();

    unlock(pathToUnlock);
}

// DELETE FROM table1 WHERE table1.column1 = '123'
void preDelete(Vector<string>& tokens, Schema& schema) {
    checkArgumentsCount(tokens, 7);

    string tableName = tokens.get(2);
    string value = tokens.get(6);
    string fullPath = tokens.get(4);
    Vector<string> dir = split(fullPath, '.');

    if (tableName != dir.get(0)) {
        throw runtime_error("Invalid column position");
    }

    if (value[0] == '(' && value[value.size() - 1] == ')') {
        value = value.substr(1, value.size() - 2);
    }

    if (value[0] == '\'' && value[value.size() - 1] == '\'') {
        value = value.substr(1, value.size() - 2);
    }

    string pathToLock = schema.name + "/" + tableName + "/" + tableName + "_lock.txt";
    lock(pathToLock);
    deleteFunc(dir, value, schema);
}

void generateCombinations(int index,Vector<Vector<Vector<string>>>& tablesData, Vector<Vector<int>>& columnIndexesList,Vector<Vector<string>>& crossProduct,Vector<string>& currentCombination) {
if (index == tablesData.size()) {
crossProduct.pushBack(currentCombination.copy());
}
Vector<Vector<string>> tableData = tablesData.get(index);//������ ������� �������
for (int i = 0; i < tableData.size(); i++) {
Vector<string> row = tableData.get(i);
Vector<int> columnIndexes = columnIndexesList.get(index);

for (int j = 0; j < columnIndexes.size(); j++) {
currentCombination.pushBack(row.get(columnIndexes.get(j)));
}

generateCombinations(index + 1, tablesData, columnIndexesList, crossProduct, currentCombination);

int newLen = currentCombination.size() - columnIndexes.size();
currentCombination.resize(newLen);
}
}

void selcet(Vector<string>& tables, Vector<string>& columns, Schema& schema) {
    if (tables.size() == 1) { // FROM table1
        string tableName = tables.get(0);

        for (int i = 0; i < columns.size(); i++) { // проверка принадлежности колонок к таблице
            string curColumn = columns.get(i);
            Vector<string> schemaCols = schema.structure.get(tableName); // колонки для таблицы
            if (schemaCols.find(curColumn) == -1) {
                //unlock
                throw runtime_error("Invalid column");
            }
        }

        int csvCount = getCsvNum(schema.name + "/" + tableName);

        for (int i = 1; i <= csvCount; i++) {
            string curCsvPath = schema.name + "/" + tableName + "/" + toString(i) + ".csv";
            Vector<Vector<string>> csvData = readCSV(curCsvPath); // всё содержимое из текущего .csv
            Vector<string> firstLine = csvData.get(0); // заголовок

            Vector<int> indexCol; // индексы колонок, т.к может быть table1.column1, table1.column4
            for (int j = 0; j < columns.size(); j++) {
                string curColumn = columns.get(j); // текущая колонка
                int index = firstLine.find(curColumn);
                if (index != -1) { // если нашли в заголовке индекс
                    indexCol.pushBack(index);
                }
            }
            cout << "idex cols: " << indexCol << endl;

            for (int k = 1; k < csvData.size(); k++) { // проходим по всем строкам из файла, кроме заголовка
                Vector<string> currentLine = csvData.get(k);
                cout << "cur: " << currentLine << endl;
                for (int i = 0; i < indexCol.size(); i++) {
                    cout << currentLine.get(indexCol.get(i));
                    if (i < indexCol.size() - 1) { // не последний элемент
                        cout << ",";
                    }
                }
                cout << endl;
            }
        }
    } else {
        Vector<Vector<int>> listOfIndexCol;
        Vector<Vector<Vector<string>>> tablesData;
        for (int i = 0; i < tables.size(); i++) {
            string tableName = tables.get(i);
            for (int j = 0; j < columns.size(); j++) { // проверка принадлежности колонок к таблице
                string curColumn = columns.get(j);
                Vector<string> schemaCols = schema.structure.get(tableName); // колонки для таблицы
                if (schemaCols.find(curColumn) == -1) {
                    //unlock
                    throw runtime_error("Invalid column");
                }
            }

            int csvCount = getCsvNum(schema.name + "/" + tableName);
            Vector<Vector<string>> tableRows; // все строки из csv
            Vector<int> indexCol; // индексы колонок, т.к может быть table1.column1, table1.column4
            for (int i = 1; i <= csvCount; i++) {
                string curCsvPath = schema.name + "/" + tableName + "/" + toString(i) + ".csv";
                Vector<Vector<string>> csvData = readCSV(curCsvPath); // всё содержимое из текущего .csv
                Vector<string> firstLine = csvData.get(0); // заголовок
                for (int j = 0; j < columns.size(); j++) {
                    string curColumn = columns.get(j); // текущая колонка
                    int index = firstLine.find(curColumn);
                    if (index != -1) { // если нашли в заголовке индекс
                        indexCol.pushBack(index);
                    }
                }
                cout << "idex cols: " << indexCol << endl;
                listOfIndexCol.pushBack(indexCol); // индексы колонок для всех таблиц

                for (int k = 1; k < csvData.size(); k++) {
                    Vector<string> es = csvData.get(k);
                    tableRows.pushBack(csvData.get(k));
                }
            }
            tablesData.pushBack(tableRows);
        }

        Vector<Vector<string>> crossProduct;
        Vector<string> currentCombination;
        generateCombinations(0, tablesData, listOfIndexCol, crossProduct, currentCombination);


        // ������� ����������
        cout << columns << endl;
        for (int i = 0; i < crossProduct.size(); i++) {
            Vector<string> combination = crossProduct.get(i);
            for (int j = 0; j < combination.size(); j++) {
                cout << combination.get(j);
                if (j < combination.size() - 1) {
                    cout << ", ";
                }
            }
            cout << endl;
        }
    }
}

void preSelect(Vector<string>& tokens, Schema& schema) {
    int indexFrom = -1;
    for (int i = 0; i < tokens.size(); i++) {
        if (tokens.get(i) == "FROM") {
            indexFrom = i;
            break;
        }
    }

    if (indexFrom == -1) {
        throw runtime_error("Invalid request");
    }

    int indexWhere = -1;
    for (int i = 0; i < tokens.size(); i++) {
        if (tokens.get(i) == "WHERE") {
            indexWhere = i;
            break;
        }
    }

    string tempColumn = "";
    for (int i = 1; i < indexFrom; i++) {
        if (tempColumn.size() != 0) {
            tempColumn += ' ';
        }
        tempColumn += tokens.get(i);
    }
    Vector<string> dataSELECT = split(tempColumn, ',');

    string tempTable = "";
    if (indexWhere != -1) { // SELECTWHERE
        for (int i = indexFrom + 1; i < indexWhere; i++) {
            if (tempTable.size() != 0) {
                tempTable += ' ';
            }
            tempTable += tokens.get(i);
        }
    } else { // обычный SELECT
        for (int i = indexFrom + 1; i < tokens.size(); i++) {
            if (tempTable.size() != 0) {
                tempTable += ' ';
            }
            tempTable += tokens.get(i);
        }
    }

    bool isSingle = false;
    Vector<string> dataFROM = split(tempTable, ',');
    if (dataFROM.size() == 1) {
        isSingle = true;
    }

    for (int i = 0; i < dataSELECT.size(); i++) { // проверка соответствия таблиц после select и from
        string tempStr = trim(dataSELECT.get(i)); // table.column
        Vector<string> tempVec = split(tempStr, '.');
        string tempTableName = trim(tempVec.get(0)); // table

        bool isFound = false;
        for (int j = 0; j < dataFROM.size(); j++) { // есть ли таблица из select в после from
            if (tempTableName == trim(dataFROM.get(j))) {
                isFound = true;
                break;
            }
        }

        if (!isFound) {
            throw runtime_error("Table " + tempTableName + " in SELECT does not match any table in FROM");
        }
    }

    Vector<string> columns;
    Vector<string> tables;
    bool isFirstRound = true;
    for (int i = 0; i < dataSELECT.size(); i++) {
        string tempStr = dataSELECT.get(i); // table.column
        Vector<string> tempVector = split(tempStr, '.');
        if (tempVector.size() != 2) {
            throw runtime_error("Invalid SELECT format");
        }

        if (isSingle && isFirstRound) {
            tables.pushBack(dataFROM.get(0));
            isFirstRound = false;
        } else if (!isSingle){
            tables.pushBack(trim(tempVector.get(0)));
        }
        columns.pushBack(tempVector.get(1));
    }

    for (int i = 0; i < tables.size(); i++) {
        string tableName = tables.get(i);
        if (!schema.structure.conatins(tableName)) {
            throw runtime_error("Invalid table");
        }
    }

    selcet(tables, columns, schema);
}

#endif //PR1MAIN_REQUEST_H
