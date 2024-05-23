#include <iostream>
#include <vector>
#include <utility>
#include "parameters.h" 
#include <cstring> 
#include <unistd.h> 
#include <arpa/inet.h>
#include <sstream>

class Client_message {
public:
    enum Type { META, LOOKUP, INSERT, INVALID, RAW_KEY };

    Type type;
    std::vector<_key_t> keys;
    std::vector<_payload_t> payloads;
    std::vector<std::vector<int>> leaf_paths;
    size_t batch_size;

    static const Client_message InvalidMessage;

    // Client_message(Type t) : type(t) {}
    // Client_message(std::vector<_key_t> keys, size_t batch_size) : type(RAW_KEY), keys(keys), batch_size(batch_size) {}
    // Client_message(const std::vector<std::vector<int>>& paths, std::vector<_key_t> keys, size_t batch_size) : 
    //                 type(LOOKUP), keys(keys), leaf_paths(paths), batch_size(batch_size) {}
    // Client_message(std::vector<_key_t> keys, std::vector<_payload_t> payloads, 
    //                 const std::vector<std::vector<int>>& paths, size_t batch_size) : 
    //                 type(INSERT), keys(keys), payloads(payloads), leaf_paths(paths), batch_size(batch_size) {}
                    
    // ~Client_message() {}

    // Client_message(size_t batch_size) : batch_size(batch_size), keys(batch_size), 
    //                 payloads(batch_size), leaf_paths(batch_size) {}

    Client_message(Type t = META , size_t batch_size = 0) 
        : type(t), batch_size(batch_size), keys(batch_size), payloads(batch_size), leaf_paths(batch_size) {}

    // 简化重复代码，利用委托构造函数
    Client_message(const std::vector<_key_t>& keys, size_t batch_size)
        : Client_message(RAW_KEY, batch_size) {
        this->keys = keys;
    }

    Client_message(const std::vector<std::vector<int>>& paths, const std::vector<_key_t>& keys, size_t batch_size)
        : Client_message(LOOKUP, batch_size) {
        this->keys = keys;
        this->leaf_paths = paths;
    }

    Client_message(const std::vector<_key_t>& keys, const std::vector<_payload_t>& payloads, 
                   const std::vector<std::vector<int>>& paths, size_t batch_size)
        : Client_message(INSERT, batch_size) {
        this->keys = keys;
        this->payloads = payloads;
        this->leaf_paths = paths;
    }

    ~Client_message() {}  // 简洁析构函数，通常无需更改除非有特殊资源管理需要

    // 确保默认拷贝和移动构造函数和赋值操作符不被删除
    Client_message(const Client_message&) = default;
    Client_message(Client_message&&) = default;
    Client_message& operator=(const Client_message&) = default;
    Client_message& operator=(Client_message&&) = default;

    std::string serialize() const {
        
        std::ostringstream content;
        content.write(reinterpret_cast<const char*>(&type), sizeof(Type));
        content.write(reinterpret_cast<const char*>(&batch_size), sizeof(batch_size));

        for (size_t i = 0; i < batch_size; i++) {
            content.write(reinterpret_cast<const char*>(&keys[i]), sizeof(_key_t));
            content.write(reinterpret_cast<const char*>(&payloads[i]), sizeof(_payload_t));

            uint32_t leafPathSize = htonl(leaf_paths[i].size());
            content.write(reinterpret_cast<const char*>(&leafPathSize), sizeof(leafPathSize));
            for (int value : leaf_paths[i]) {
                content.write(reinterpret_cast<const char*>(&value), sizeof(int));
            }
        }

        std::string serializedData = content.str();
        uint32_t totalLength = htonl(serializedData.size() + sizeof(uint32_t));
        std::ostringstream serializedMessage;
        serializedMessage.write(reinterpret_cast<const char*>(&totalLength), sizeof(totalLength));
        serializedMessage << serializedData;

        

        return serializedMessage.str();
    }

    static Client_message deserialize(const std::string& serializedData) {
        std::istringstream ss(serializedData);
        Type t;
        size_t batch_size;

        ss.read(reinterpret_cast<char*>(&t), sizeof(Type));
        ss.read(reinterpret_cast<char*>(&batch_size), sizeof(batch_size));

        std::vector<_key_t> keys(batch_size);
        std::vector<_payload_t> payloads(batch_size);
        std::vector<std::vector<int>> leaf_paths(batch_size);

        for (size_t i = 0; i < batch_size; i++) {
            ss.read(reinterpret_cast<char*>(&keys[i]), sizeof(_key_t));
            ss.read(reinterpret_cast<char*>(&payloads[i]), sizeof(_payload_t));

            uint32_t leafPathSize;
            ss.read(reinterpret_cast<char*>(&leafPathSize), sizeof(leafPathSize));
            leafPathSize = ntohl(leafPathSize);

            leaf_paths[i].resize(leafPathSize);
            for (uint32_t j = 0; j < leafPathSize; j++) {
                ss.read(reinterpret_cast<char*>(&leaf_paths[i][j]), sizeof(int));
            }
        }

        Client_message msg(t, batch_size);
        msg.keys = std::move(keys);
        msg.payloads = std::move(payloads);
        msg.leaf_paths = std::move(leaf_paths);

        return msg;
    }

    // std::string serialize() const {
    //     std::ostringstream content;
    //     content.write(reinterpret_cast<const char*>(&type), sizeof(Type));
    //     content.write(reinterpret_cast<const char*>(&key), sizeof(_key_t));
    //     content.write(reinterpret_cast<const char*>(&payload), sizeof(_payload_t));

    //     uint32_t leafPathSize = htonl(leaf_path.size());
    //     content.write(reinterpret_cast<const char*>(&leafPathSize), sizeof(leafPathSize));
    //     for (int value : leaf_path) {
    //         content.write(reinterpret_cast<const char*>(&value), sizeof(int));
    //     }

    //     std::string serializedData = content.str();
    //     uint32_t totalLength = htonl(serializedData.size() + sizeof(uint32_t)); 

    //     std::ostringstream serializedMessage;
    //     serializedMessage.write(reinterpret_cast<const char*>(&totalLength), sizeof(totalLength));
    //     serializedMessage << serializedData;

    //     return serializedMessage.str();
    // }

    // static Client_message deserialize(const std::string& serializedData) {
    //     std::istringstream ss(serializedData);
    //     Type t;
    //     ss.read(reinterpret_cast<char*>(&t), sizeof(Type));

    //     _key_t key;
    //     ss.read(reinterpret_cast<char*>(&key), sizeof(_key_t));

    //     _payload_t payload;
    //     ss.read(reinterpret_cast<char*>(&payload), sizeof(_payload_t));

    //     uint32_t leafPathSize;
    //     ss.read(reinterpret_cast<char*>(&leafPathSize), sizeof(leafPathSize));
    //     leafPathSize = ntohl(leafPathSize);
    //     std::vector<int> leaf_path(leafPathSize);

    //     if (t == INSERT) {
    //         return Client_message(key, payload, leaf_path);
    //     } else if (t == LOOKUP) {
    //         return Client_message(leaf_path, key);
    //     } else if (t == RAW_KEY) {
    //         return Client_message(key);
    //     } else if (t == META) {
    //         return Client_message(t);
    //     }

    //     return InvalidMessage;
    // }
};

const Client_message Client_message::InvalidMessage(Client_message::INVALID);

void sendClientMessage(int sock, const Client_message& msg) {
    std::string data = msg.serialize();

    if (send(sock, data.c_str(), data.size(), 0) == -1) {
        std::cerr << "Error sending data\n";
        close(sock);
    }
}

Client_message receiveAndDeserialize(int sock) {
    uint32_t dataLength;
    if (recv(sock, &dataLength, sizeof(dataLength), 0) <= 0) {
        std::cerr << "Failed to receive data length or connection closed\n";
        close(sock);
        return Client_message::InvalidMessage;
    }

    dataLength = ntohl(dataLength); 

    dataLength -= sizeof(uint32_t);

    std::vector<char> buffer(dataLength);
    size_t totalReceived = 0;
    while (totalReceived < dataLength) {
        ssize_t bytesRead = recv(sock, buffer.data() + totalReceived, dataLength - totalReceived, 0);
        if (bytesRead <= 0) {
            std::cerr << "Error receiving data or connection closed\n";
            close(sock);
            return Client_message::InvalidMessage;
        }
        totalReceived += bytesRead;
    }

    std::string receivedData(buffer.begin(), buffer.end());
    return Client_message::deserialize(receivedData);
}
