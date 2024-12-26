#include <iostream>
#include "readJson.h"
#include "help.h"
#include "request.h"

void request(string& command, int clientSocket, Schema& schema) {
    Vector<string> tokens = split(command, " ");
    string SQLCommand = tokens.get(0);

    try {
        if (SQLCommand == "INSERT") {
            preInsert(tokens, schema, clientSocket);
        } else if (SQLCommand == "DELETE") {
            preDelete(tokens, schema, clientSocket);
        } else if (SQLCommand == "SELECT") {
            preSelect(tokens, schema, clientSocket);
        } else {
            string errorMessage = "Unknown command\n";
            send(clientSocket, errorMessage.c_str(), errorMessage.size(), 0);
        }
    } catch (exception& e) {
        string errorMessage = string() + e.what() + "\n";
        send(clientSocket, errorMessage.c_str(), errorMessage.size(), 0);
    }
}

void clearUserData(char* userData, int len) {
    for (int i = 0; i < len; i++) {
        userData[i] = 0;
    }
}

void client(int clientSocket, Schema& schema) {
    const int userDataLen = 1024;
    char userData[userDataLen];
    clearUserData(userData, userDataLen);

    while (true) {
        int data = read(clientSocket, userData, userDataLen); // количество байт прочитаных из сокета
        if (data < 0) {
            cerr << "Error while reading query" << endl;
            break;
        } else if (data == 0) {
            cout << "Client disconnected." << endl;
            break;
        }

        string command(userData); // преобразуем данные

        if (command == "EXIT") {
            cout << "Client exit." << endl;
            break;
        }

        request(command, clientSocket, schema);
        clearUserData(userData, userDataLen);
    }

    close(clientSocket);
    cout << "Connection closed." << endl;
}

void startServer(Schema& schema) {
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0); // IPv4, TCP
    if (serverSocket == 0) {
        cerr << "Server creation error" << endl;
        return;
    }

    struct sockaddr_in address = {}; // описание адреса сокета
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY; // сервер принимает все доступные IP
    address.sin_port = htons(7432);

    // привзяка сокета с портом и адресом
    if (bind(serverSocket, (struct sockaddr*)&address, sizeof(address)) < 0) {
        cerr << "Socket binding error" << endl;
        close(serverSocket);
        return;
    }

    // устанавливем сокет в режим ожидания
    if (listen(serverSocket, 10) < 0) {
        cerr << "Listen error" << endl;
        close(serverSocket);
        return;
    }

    while (true) {
        int clientSocket;
        sockaddr_in clientAddress;
        socklen_t clientAddressLen = sizeof(clientAddress);

        clientSocket = accept(serverSocket, (sockaddr*)&clientAddress, &clientAddressLen);
        if (clientSocket < 0) {
            cerr << "Accept client error" << endl;
            continue;
        }

        cout << "Client connected: " << inet_ntoa(clientAddress.sin_addr) << endl; // преобразует в читаемы ip

        thread clientThread(client, clientSocket, std::ref(schema));
        clientThread.detach(); // делает поток независимым от главного
    }

    close(serverSocket);
}

int main() {
    Schema schema = readJson("schema.json");
    createDir(schema);
    startServer(schema);
}
