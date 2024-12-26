#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

using namespace std;

int main() {
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0); // IPv4, TCP
    if (clientSocket < 0) {
        cerr << "Socket creation failed" << endl;
        return 1;
    }

    struct sockaddr_in serverAddress; // описание адреса сервера
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(7432);
    serverAddress.sin_addr.s_addr = inet_addr("127.0.0.1");

    if (connect(clientSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0) {
        cerr << "Connection failed" << endl;
        close(clientSocket);
        return -1;
    }

    cout << "Connected to server!" << endl;

    string message;
    while (true) {
        cout << "> ";
        getline(cin, message);

        if (send(clientSocket, message.c_str(), message.size(), 0) < 0) {
            cerr << "Send failed" << endl;
            break;
        }

        if (message == "EXIT") {
            break;
        }

        string response;
        char userData[1024];
        while (true) {
            int recivedData = recv(clientSocket, userData, sizeof(userData) - 1, 0);
            if (recivedData <= 0) {
                cerr << "Connection closed by server" << endl;
                break;
            }
            userData[recivedData] = '\0';
            response += userData;

            if (response.find('\n') != string::npos) {
                break;
            }
        }

        cout << "Server: " << response << endl;
    }

    close(clientSocket);
    return 0;
}