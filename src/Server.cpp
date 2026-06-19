#include "Server.hpp"
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <thread>
#include <iostream>
#include <sstream>
#include <algorithm>

Server::Server(Engine& engine, int port)
    : engine(engine), port(port) {}

Server::~Server() {
    stop();
}

void Server::start() {
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket creation failed");
        return;
    }

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt SO_REUSEADDR failed");
        close(server_fd);
        server_fd = -1;
        return;
    }

    struct sockaddr_in address;
    std::memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        perror("bind failed");
        close(server_fd);
        server_fd = -1;
        return;
    }

    if (listen(server_fd, 128) < 0) {
        perror("listen failed");
        close(server_fd);
        server_fd = -1;
        return;
    }

    running = true;
    std::cout << "Server listening on port " << port << "..." << std::endl;

    while (running) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int client_sock = accept(server_fd, (struct sockaddr*)&client_addr, &addr_len);
        if (client_sock < 0) {
            if (!running) break;
            perror("accept failed");
            continue;
        }

        if (!running) {
            close(client_sock);
            break;
        }

        {
            std::lock_guard<std::mutex> lock(clients_mutex);
            client_fds.insert(client_sock);
        }

        std::thread(&Server::handle_client, this, client_sock).detach();
    }
}

void Server::stop() {
    if (!running) return;
    running = false;

    // Shutdown the listener socket to unblock accept()
    if (server_fd != -1) {
        shutdown(server_fd, SHUT_RDWR);
        close(server_fd);
        server_fd = -1;
    }

    // Shutdown all client connections to unblock recv()
    {
        std::lock_guard<std::mutex> lock(clients_mutex);
        for (int fd : client_fds) {
            shutdown(fd, SHUT_RDWR);
        }
    }

    // Wait until all client threads exit
    while (active_clients.load() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void Server::handle_client(int client_sock) {
    active_clients++;
    char buffer[1024];
    std::string client_buffer;

    while (running) {
        ssize_t bytes_received = recv(client_sock, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received <= 0) {
            break; // Connection closed or error
        }

        buffer[bytes_received] = '\0';
        client_buffer += buffer;

        // Process all complete lines
        size_t pos;
        while ((pos = client_buffer.find('\n')) != std::string::npos) {
            std::string line = client_buffer.substr(0, pos);
            client_buffer.erase(0, pos + 1);

            std::string response = process_line(line);
            if (!response.empty()) {
                send(client_sock, response.c_str(), response.size(), 0);
            }
        }
    }

    // Clean up
    close(client_sock);
    {
        std::lock_guard<std::mutex> lock(clients_mutex);
        client_fds.erase(client_sock);
    }
    active_clients--;
}

std::string Server::process_line(const std::string& line) {
    std::string clean_line = line;
    if (!clean_line.empty() && clean_line.back() == '\r') {
        clean_line.pop_back();
    }
    if (clean_line.empty()) return "";

    std::istringstream iss(clean_line);
    std::string cmd, key;
    if (!(iss >> cmd)) return "ERR syntax error\n";

    for (char &c : cmd) {
        c = std::toupper(c);
    }

    if (cmd == "SET") {
        if (!(iss >> key)) return "ERR syntax error\n";
        std::string val;
        std::ws(iss);
        std::getline(iss, val);
        try {
            engine.set(key, val);
            return "OK\n";
        } catch (const std::exception& e) {
            return std::string("ERR ") + e.what() + "\n";
        }
    } else if (cmd == "GET") {
        if (!(iss >> key)) return "ERR syntax error\n";
        auto val_opt = engine.get(key);
        if (val_opt.has_value()) {
            return val_opt.value() + "\n";
        } else {
            return "(nil)\n";
        }
    } else if (cmd == "DEL") {
        if (!(iss >> key)) return "ERR syntax error\n";
        try {
            bool erased = engine.remove(key);
            return erased ? "1\n" : "0\n";
        } catch (const std::exception& e) {
            return std::string("ERR ") + e.what() + "\n";
        }
    } else {
        return "ERR unknown command\n";
    }
}
