#ifndef PR1MAIN_REQUEST_H
#define PR1MAIN_REQUEST_H

#include "readJson.h"
#include "help.h"

void insert(Vector<string>& values, const string& tableName, Schema& schema, int clientSocket) {
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
        int numLines = 0;
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
        csvFile << values.get(i);

        if (values.size() - 1 != i) {
            csvFile << ',';
        }
    }
    csvFile << endl;
    csvFile.close();

    key++;
    ofstream pkFileSave(pkPath);
    pkFileSave << key;
    pkFileSave.close();

    string successMessage = "Insert completed!\n";
    send(clientSocket, successMessage.c_str(), successMessage.size(), 0);
    unlock(pathToUnlock);
}

void preInsert(Vector<string>& tokens, Schema& schema, int clientSocket) {
    checkArgumentsCount(tokens, 5);

    string tableName = tokens.get(2);
    string value = tokens.get(4);
    Vector<string> values; // "чистые" элементы

    if (value[0] == '(' && value[value.size() - 1] == ')') {
        value = value.substr(1, value.size() - 2);

        values = split(value, ",");
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i)[0] == '\'' && values.get(i)[values.get(i).size() - 1] == '\'') {
                values.set(i, values.get(i).substr(1, values.get(i).size() - 2));
            }
        }
    } else {
        values = split(value, ",");
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i)[0] == '\'' && values.get(i)[values.get(i).size() - 1] == '\'') {
                values.set(i, values.get(i).substr(1, values.get(i).size() - 2));
            }
        }
    }

    string pathToLock = schema.name + "/" + tableName + "/" + tableName + "_lock.txt";
    lock(pathToLock);
    insert(values, tableName, schema, clientSocket);
}

void deleteFunc(Vector<string>& tableColumn, string& val, Schema& schema, int clientSocket) {
    string tableName = tableColumn.get(0);
    string columnName = tableColumn.get(1);
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
        throw runtime_error("Column " + columnName + " not found");
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

            Vector<string> lineElements = split(line, ",");
            if (lineElements.get(columnIndex) == val) {
                continue; // если в строке есть значение, то просто пропускаем строку
            }

            int currentPk = toInt(lineElements.get(0)); // первым записан первичный ключ
            if (currentPk > maxPk) {
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

    string successMessage = "Delete completed!\n";
    send(clientSocket, successMessage.c_str(), successMessage.size(), 0);
    unlock(pathToUnlock);
}

void preDelete(Vector<string>& tokens, Schema& schema, int clientSocket) {
    checkArgumentsCount(tokens, 7);

    string tableName = tokens.get(2);
    string value = tokens.get(6);
    string rawString = tokens.get(4);
    Vector<string> tableColumn = split(rawString, ".");

    if (tableName != tableColumn.get(0)) {
        throw runtime_error("Invalid table " + tableColumn.get(0) + " for delete from " + tableName);
    }

    if (value[0] == '(' && value[value.size() - 1] == ')') {
        value = value.substr(1, value.size() - 2);
    }

    if (value[0] == '\'' && value[value.size() - 1] == '\'') {
        value = value.substr(1, value.size() - 2);
    }

    string pathToLock = schema.name + "/" + tableName + "/" + tableName + "_lock.txt";
    lock(pathToLock);
    deleteFunc(tableColumn, value, schema, clientSocket);
}

void selcet(Vector<string>& tables, Vector<string>& tabColumns, Schema& schema, int clientSocket) {
    string clientOutput;
    if (tables.size() == 1) {
        string tableName = tables.get(0);
        Vector<string> columns;

        for (int i = 0; i < tabColumns.size(); i++) { // очистка
            string curColumnTab = tabColumns.get(i);
            string curColumn = dotCut(curColumnTab, 2);

            columns.pushBack(curColumn);
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

            clientOutput += "\n";
            for (int k = 1; k < csvData.size(); k++) { // проходим по всем строкам из файла, кроме заголовка
                Vector<string> currentLine = csvData.get(k);
                for (int i = 0; i < indexCol.size(); i++) {
                    clientOutput += currentLine.get(indexCol.get(i));
                    if (i < indexCol.size() - 1) { // не последний элемент
                        clientOutput += ",";
                    }
                }
                clientOutput += "\n";
            }
            send(clientSocket, clientOutput.c_str(), clientOutput.size(), 0);
            clientOutput.clear();
        }
        unlockAll(tables, schema.name);
    } else {
        Vector<Vector<int>> listOfIndexCol;
        Vector<Vector<Vector<string>>> tablesData;

        for (int i = 0; i < tables.size(); i++) {

            string tableName = tables.get(i);
            Vector<string> schemaCols = schema.structure.get(tableName); // колонки для таблицы
            Vector<string> columns;

            for (int j = 0; j < tabColumns.size(); j++) { // очистка + проверка принадлежности колонок к таблице

                string curColumnTab = tabColumns.get(j);
                string curTable = dotCut(curColumnTab, 1);
                string curColumn = dotCut(curColumnTab, 2);

                if (curTable == tableName) {
                    if (schemaCols.find(curColumn) == -1) {
                        unlockAll(tables, schema.name);
                        throw runtime_error("Invalid column " + curColumn);
                    }
                    columns.pushBack(curColumn);
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

                listOfIndexCol.pushBack(indexCol); // индексы колонок для всех таблиц

                for (int k = 1; k < csvData.size(); k++) {
                    tableRows.pushBack(csvData.get(k));
                }
            }
            tablesData.pushBack(tableRows);
        }

        Vector<Vector<string>> result;
        for (int i = 0; i < tablesData.get(0).size(); i++) {
            for (int j = 0; j < tablesData.get(1).size(); j++) {

                Vector<string> newRow;
                Vector<int> indexesTab1 = listOfIndexCol.get(0);
                Vector<int> indexesTab2 = listOfIndexCol.get(1);

                for (int k = 0; k < indexesTab1.size(); k++) {
                    Vector<string> curVec = tablesData.get(0).get(i); // берём из 1 таблицы i-ый вектор
                    int index = indexesTab1.get(k); // берём k-ый индекс
                    newRow.pushBack(curVec.get(index));
                }

                for (int k = 0; k < indexesTab2.size(); k++) {
                    Vector<string> curVec = tablesData.get(1).get(j); // берём из 2 таблицы j-ый вектор
                    int index = indexesTab2.get(k);
                    newRow.pushBack(curVec.get(index));
                }

                result.pushBack(newRow);
            }
        }

        clientOutput += "\n";
        for (int i = 0; i < result.size(); i++) {
            for (int j = 0; j < result.get(i).size(); j++) {
                clientOutput += result.get(i).get(j);
                if (j < result.get(i).size() - 1) {
                    clientOutput += ",";
                }
            }
            clientOutput += "\n";
        }
        send(clientSocket, clientOutput.c_str(), clientOutput.size(), 0);
        clientOutput.clear();
        unlockAll(tables, schema.name);
    }
}

void generateCombinations(int depth, Vector<Vector<Vector<string>>>& tablesData, Vector<string>& currentCombination, Vector<Vector<string>>& crossProduct, int tablesSize) {
    if (depth == tablesSize) {
        crossProduct.pushBack(currentCombination.copy());
        return;
    }

    Vector<Vector<string>> tableData = tablesData.get(depth);
    for (int i = 0; i < tableData.size(); i++) {
        Vector<string> row = tableData.get(i);

        for (int j = 0; j < row.size(); j++) {
            currentCombination.pushBack(row.get(j));
        }

        generateCombinations(depth + 1, tablesData, currentCombination, crossProduct, tablesSize);

        int newLen = currentCombination.size() - row.size();
        currentCombination.resize(newLen);
    }
}

void selectWhere(Vector<string>& tables, Vector<string>& tabColumns, Vector<string> conditions, Schema& schema, int clientSocket) {
    string clientOutput;
    clientOutput += "\n";
    if (tables.size() == 1) {
        string tableName = tables.get(0);
        Vector<string> columns;

        for (int i = 0; i < tabColumns.size(); i++) { // очистка + проверка принадлежности колонок к таблице
            string curColumnTab = tabColumns.get(i);
            string curColumn = dotCut(curColumnTab, 2);

            columns.pushBack(curColumn);
        }

        int csvCount = getCsvNum(schema.name + "/" + tableName);

        for (int i = 1; i <= csvCount; i++) {
            string curCsvPath = schema.name + "/" + tableName + "/" + toString(i) + ".csv";
            Vector<Vector<string>> csvData = readCSV(curCsvPath); // всё содержимое текущего .csv
            Vector<string> firstLine = csvData.get(0); // заголовок

            Vector<int> indexCol;
            for (int j = 0; j < columns.size(); j++) {
                string curColumn = columns.get(j);
                int index = firstLine.find(curColumn);
                if (index != -1) {
                    indexCol.pushBack(index);
                }
            }

            for (int lineIndex = 1; lineIndex < csvData.size(); lineIndex++) { // всё кроме заголовка
                Vector<string> lineElements = csvData.get(lineIndex);
                bool isOrCondition = false;

                for (int indexOr = 0; indexOr < conditions.size(); indexOr++) { // все части OR
                    Vector<string> andPart = split(conditions.get(indexOr), "AND");
                    bool isAndCondition = true;

                    for (auto& condition : andPart) { // для всех условий из AND
                        Vector<string> partsEquivalence = split(condition, "=");

                        if (partsEquivalence.size() != 2) {
                            isAndCondition = false;
                            break;
                        }

                        string leftPart = partsEquivalence.get(0);
                        string rightPart = partsEquivalence.get(1);

                        int indexOfColumn;
                        try {
                            indexOfColumn = getColumnIndex(leftPart, schema.name + "/" + tableName);
                        } catch (exception& e) {
                            unlockAll(tables, schema.name);
                            throw runtime_error("Invalid format: " + leftPart);
                        }

                        // правая часть - значение
                        if (rightPart[0] == '\'' && rightPart[rightPart.size() - 1] == '\'') {
                            string value = rightPart.substr(1, rightPart.size() - 2);
                            if (lineElements.get(indexOfColumn) != value) {
                                isAndCondition = false;
                                break;
                            }
                        } else { // правая часть - tab.col

                            int compareIndex;
                            try {
                                compareIndex = getColumnIndex(rightPart, schema.name + "/" + tableName);
                            } catch (exception& e) {
                                unlockAll(tables, schema.name);
                                throw runtime_error("Invalid format: " + rightPart);
                            }

                            if (lineElements.get(indexOfColumn) != lineElements.get(compareIndex)) {
                                isAndCondition = false;
                                break;
                            }
                        }
                    }

                    if (isAndCondition) {
                        isOrCondition = true;
                        break;
                    }
                }

                if (isOrCondition) {
                    for (int i = 0; i < indexCol.size(); i++) {
                        clientOutput += lineElements.get(indexCol.get(i));
                        if (i < indexCol.size() - 1) {
                            clientOutput += ", ";
                        }
                    }
                    clientOutput += "\n";
                }
            }
        }
        unlockAll(tables, schema.name);
    } else {
        Vector<Vector<int>> listOfIndexCol;
        Vector<Vector<Vector<string>>> tablesData;
        string newHeader;

        for (int i = 0; i < tables.size(); i++) {
            string tableName = tables.get(i);
            Vector<string> schemaCols = schema.structure.get(tableName); // колонки для таблицы
            Vector<string> columns;

            for (int j = 0; j < tabColumns.size(); j++) { // очистка + проверка принадлежности колонок к таблице

                string curColumnTab = tabColumns.get(j);
                string curTable = dotCut(curColumnTab, 1);
                string curColumn = dotCut(curColumnTab, 2);

                if (curTable == tableName) {
                    if (schemaCols.find(curColumn) == -1) {
                        unlockAll(tables, schema.name);
                        throw runtime_error("Invalid column: " + curColumn);
                    }
                    columns.pushBack(curColumn);
                }

            }

            int offset = 0;
            for (int j = 0; j < i; j++) { // Обработка всех предыдущих таблиц
                string prevTableName = tables.get(j);
                int csvCountForOff = getCsvNum(schema.name + "/" + prevTableName);
                for (int k = 1; k <= csvCountForOff; k++) {
                    string curCsvPath = schema.name + "/" + prevTableName + "/" + toString(k) + ".csv";
                    Vector<Vector<string>> prevPages = readCSV(curCsvPath);
                    Vector<string> prevHeader = prevPages.get(0);
                    offset += prevHeader.size();
                }
            }

            int csvCount = getCsvNum(schema.name + "/" + tableName);
            Vector<Vector<string>> tablesRows;

            for (int j = 1; j <= csvCount; j++) {
                string curCsvPath = schema.name + "/" + tableName + "/" + toString(j) + ".csv";
                Vector<Vector<string>> csvData = readCSV(curCsvPath); // всё содержимое текущего .csv
                Vector<string> firstLine = csvData.get(0); // заголовок

                Vector<int> indexCol;
                for (int j = 0; j < columns.size(); j++) {
                    string curColumn = columns.get(j);
                    int index = firstLine.find(curColumn);
                    if (index != -1) {
                        indexCol.pushBack(index + offset);
                    }
                }

                listOfIndexCol.pushBack(indexCol);

                for (int k = 1; k < csvData.size(); k++) {
                    tablesRows.pushBack(csvData.get(k));
                }

            }
            tablesData.pushBack(tablesRows); // добавляем данные текущей таблицы

            for (int j = 0; j < schemaCols.size(); j++) {
                newHeader += schemaCols.get(j);
                if (!(j == schemaCols.size() - 1 && i == tables.size() - 1)) {
                    newHeader += ",";
                }
            }
        }

        Vector<Vector<string>> crossProduct;
        Vector<string> currentCombination;

        generateCombinations(0, tablesData, currentCombination, crossProduct, tablesData.size());

        Vector<Vector<string>> result;
        for (int i = 0; i < crossProduct.size(); i++) {
            Vector<string> row = crossProduct.get(i);
            bool isOrCondition = false;

            for (int indexOr = 0; indexOr < conditions.size(); indexOr++) {
                Vector<string> andParts = split(conditions.get(indexOr), "AND");
                bool isAndCondition = true;

                for (auto condition: andParts) {
                    Vector<string> partsEquivalence = split(condition, "=");

                    if (partsEquivalence.size() != 2) {
                        isAndCondition = false;
                        break;
                    }

                    string leftPart = partsEquivalence.get(0);
                    string rightPart = partsEquivalence.get(1);

                    string tableName;
                    string columnName;
                    try {
                        tableName = dotCut(leftPart, 1);
                        columnName = dotCut(leftPart, 2);
                    } catch (exception& e) {
                        unlockAll(tables, schema.name);
                        throw runtime_error("Invalid format for " + leftPart);
                    }

                    Vector<string> newHeaders = split(newHeader, ",");
                    int columnIndex = -1;

                    for (int k = 0; k < newHeaders.size(); k++) {
                        if (newHeaders.get(k) == columnName) {
                            if (tableName.substr(0, 5) == "table") {
                                int tableNumber = stoi(tableName.substr(5));
                                columnIndex = k + tableNumber;
                            }
                        }
                    }

                    if (columnIndex == -1) {
                        isAndCondition = false;
                        break;
                    }

                    // 'val'
                    if (rightPart[0] == '\'' && rightPart[rightPart.size() - 1] == '\'') {
                        string value = rightPart.substr(1, rightPart.size() - 2);
                        if (row.get(columnIndex) != value) {
                            isAndCondition = false;
                            break;
                        }
                    } else { // tab.col

                        string secTableName;
                        string secColumnName;

                        try {
                            secTableName = dotCut(rightPart, 1);
                            secColumnName = dotCut(rightPart, 2);
                        } catch (exception& e) {
                            unlockAll(tables, schema.name);
                            throw runtime_error("Invalid format for " + rightPart);
                        }

                        int secColumnIndex = -1;
                        for (int tabIndex = 0; tabIndex < tables.size(); tabIndex++) {
                            if (tables.get(tabIndex) == secTableName) {
                                Vector<string> secHeader = split(newHeader, ",");
                                for (int k = 0; k < secHeader.size(); k++) {
                                    if (secHeader.get(k) == secColumnName) {
                                        secColumnIndex = k;
                                        break;
                                    }
                                }
                                break;
                            }
                        }

                        if (secColumnIndex == -1) {
                            isAndCondition = false;
                            break;
                        }

                        if (row.get(columnIndex) != row.get(secColumnIndex)) {
                            isAndCondition = false;
                            break;
                        }
                    }
                }

                if (isAndCondition) {
                    isOrCondition = true;
                    break;
                }
            }

            if (isOrCondition) {
                result.pushBack(row);
            }
        }

        for (int i = 0; i < result.size(); i++) {
            Vector<string> combination = result.get(i);
            Vector<string> selectedColumns;

            for (int tableIndex = 0; tableIndex < tables.size(); ++tableIndex) {
                Vector<int> columnIndexes = listOfIndexCol.get(tableIndex);
                for (int colIdx : columnIndexes) {
                    selectedColumns.pushBack(combination.get(colIdx));
                }
            }

            for (int j = 0; j < selectedColumns.size(); j++) {
                clientOutput += selectedColumns.get(j);
                if (j < selectedColumns.size() - 1) {
                    clientOutput += ", ";
                }
            }
            clientOutput += "\n";
        }
        unlockAll(tables, schema.name);
    }
    send(clientSocket, clientOutput.c_str(), clientOutput.size(), 0);
}

void isCorrectTabCol(Vector<string>& tabCol, Schema& schema) {
    Vector<string> tables;
    Vector<string> columns;

    // разделяем на таблицы и колонки
    for (int i = 0; i < tabCol.size(); i++) {
        Vector<string> temp = split(tabCol.get(i), ".");

        if (temp.size() != 2) {
            throw runtime_error("Invalid input format: " + tabCol.get(i)); // должно быть table.column
        }

        tables.pushBack(temp.get(0));
        columns.pushBack(temp.get(1));
    }

    // существуют ли таблицы
    for (int i = 0; i < tables.size(); i++) {
        string tableName = tables.get(i);
        if (!schema.structure.conatins(tableName)) {
            throw runtime_error("Table " + tableName + " not found");
        }
    }

    // есть ли колонки в каждой таблице
    for (int i = 0; i < tables.size(); i++) {
        string tableName = tables.get(i);
        string columnName = columns.get(i);

        Vector<string> jsonColumns = schema.structure.get(tableName);
        bool isColumnExists = false;

        for (int j = 0; j < jsonColumns.size(); j++) {
            if (jsonColumns.get(j) == columnName) {
                isColumnExists = true;
                break;
            }
        }

        if (!isColumnExists) {
            throw runtime_error("Column " + columnName + " not found");
        }
    }

}

void preSelect(Vector<string>& tokens, Schema& schema, int clientSocket) {
    int indexFrom = -1;
    for (int i = 0; i < tokens.size(); i++) {
        if (tokens.get(i) == "FROM") {
            indexFrom = i;
            break;
        }
    }

    if (indexFrom == -1) {
        throw runtime_error("Invalid request. Maybe you forgot FROM?");
    }

    int indexWhere = -1;
    for (int i = 0; i < tokens.size(); i++) {
        if (tokens.get(i) == "WHERE") {
            indexWhere = i;
            break;
        }
    }

    string tempTabColumn = "";
    for (int i = 1; i < indexFrom; i++) {
        tempTabColumn += tokens.get(i);
    }

    bool isWhere = false;
    string tempTable = "";
    if (indexWhere != -1) { // SELECTWHERE
        for (int i = indexFrom + 1; i < indexWhere; i++) {
            tempTable += tokens.get(i);
        }
        isWhere = true;
    } else { // обычный SELECT
        for (int i = indexFrom + 1; i < tokens.size(); i++) {
            tempTable += tokens.get(i);
        }
    }

    Vector<string> tabColumns = split(tempTabColumn, ","); // после SELECT
    Vector<string> tables = split(tempTable, ","); // после FROM

    isCorrectTabCol(tabColumns, schema); // проверка, что после SELECT идут существующие таблицы и колонки

    for (int i = 0; i < tables.size(); i++) { // проверка, что после FROM идут существующие таблицы
        string tableName = tables.get(i);
        if (!schema.structure.conatins(tableName)) {
            throw runtime_error("Invalid table " + tableName);
        }
    }

    for (int i = 0; i < tables.size(); i++) { // проверка соответствия таблиц после select и from
        string tempStr = trim(tabColumns.get(i)); // table.column
        Vector<string> tempVec = split(tempStr, ".");
        string tempTableName = trim(tempVec.get(0)); // table из SELECT

        bool isFoundInFrom = false;
        for (int j = 0; j < tables.size(); j++) { // есть ли таблица из select в после from
            if (tempTableName == trim(tables.get(j))) {
                isFoundInFrom = true;
                break;
            }
        }

        if (!isFoundInFrom) {
            throw runtime_error("Table " + tempTableName + " in SELECT does not match any table in FROM");
        }
    }

    Vector<string> conditions;
    string tempConditions = "";
    for (int i = indexWhere + 1; i < tokens.size(); i++) {
        tempConditions += tokens.get(i);
        if (i < tokens.size() - 1) {
            tempConditions += ' ';
        }
    }

    Vector<string> orConditions = split(tempConditions, "OR");
    for (int i = 0; i < orConditions.size(); i++) {
        conditions.pushBack(trim(orConditions.get(i)));
    }

    lockAll(tables, schema.name);
    if (!isWhere) {
        selcet(tables, tabColumns, schema, clientSocket);
    } else {
        selectWhere(tables, tabColumns, conditions, schema, clientSocket);
    }
}

#endif //PR1MAIN_REQUEST_H
