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
    _key_t key;
    _payload_t payload;
    std::vector<int> leaf_path;

    static const Client_message InvalidMessage;

    Client_message(Type t) : type(t) {}
    Client_message(_key_t key) : type(RAW_KEY), key(key) {}
    Client_message(const std::vector<int>& path, _key_t key) : type(LOOKUP), key(key), leaf_path(path) {}
    Client_message(_key_t key, _payload_t payload, const std::vector<int>& path) : type(INSERT), key(key), payload(payload), leaf_path(path){}

    ~Client_message() {}

    std::string serialize() const {
        std::ostringstream content;
        content.write(reinterpret_cast<const char*>(&type), sizeof(Type));
        content.write(reinterpret_cast<const char*>(&key), sizeof(_key_t));
        content.write(reinterpret_cast<const char*>(&payload), sizeof(_payload_t));

        uint32_t leafPathSize = htonl(leaf_path.size());
        content.write(reinterpret_cast<const char*>(&leafPathSize), sizeof(leafPathSize));
        for (int value : leaf_path) {
            content.write(reinterpret_cast<const char*>(&value), sizeof(int));
        }

        // if (type == INSERT) {
        //     content.write(reinterpret_cast<const char*>(&key_value.first), sizeof(_key_t));
        //     content.write(reinterpret_cast<const char*>(&key_value.second), sizeof(_payload_t));
        // }

        // 序列化完成后，将长度作为头部加入
        std::string serializedData = content.str();
        uint32_t totalLength = htonl(serializedData.size() + sizeof(uint32_t)); // 加上长度字段自身的大小
        // std::cout << "send data size: " << serializedData.size() + sizeof(uint32_t) << std::endl;


        std::ostringstream serializedMessage;
        serializedMessage.write(reinterpret_cast<const char*>(&totalLength), sizeof(totalLength));
        serializedMessage << serializedData;

        return serializedMessage.str();
    }

    static Client_message deserialize(const std::string& serializedData) {

        // std::cout << "deserialize data" << std::endl;


        std::istringstream ss(serializedData);
        Type t;
        ss.read(reinterpret_cast<char*>(&t), sizeof(Type));
        // std::cout << "type: " << t << std::endl;

        _key_t key;
        ss.read(reinterpret_cast<char*>(&key), sizeof(_key_t));
        // std::cout << "key: " << key << std::endl;

        _payload_t payload;
        ss.read(reinterpret_cast<char*>(&payload), sizeof(_payload_t));

        uint32_t leafPathSize;
        ss.read(reinterpret_cast<char*>(&leafPathSize), sizeof(leafPathSize));
        leafPathSize = ntohl(leafPathSize);
        std::vector<int> leaf_path(leafPathSize);
        // std::cout << "leafPathsize: " << leafPathSize << std::endl;
        // std::cout << "deserialize leaf path: " << std::endl;
        // for (uint32_t i = 0; i < leafPathSize; ++i) {
            
        //     ss.read(reinterpret_cast<char*>(&leaf_path[i]), sizeof(int));
        //     std::cout << leaf_path[i] << " ";
        // }

        // std::cout << std::endl;

        if (t == INSERT) {
            // _key_t insert_key;
            // _payload_t insert_payload;
            // ss.read(reinterpret_cast<char*>(&insert_key), sizeof(_key_t));
            // ss.read(reinterpret_cast<char*>(&insert_payload), sizeof(_payload_t));
            return Client_message(key, payload, leaf_path);
        } else if (t == LOOKUP) {
            // std::cout << "LOOKUP" << std::endl;
            return Client_message(leaf_path, key);
        } else if (t == RAW_KEY) {
            return Client_message(key);
        } else if (t == META) {
            return Client_message(t);
        }

        return InvalidMessage;
    }
};

const Client_message Client_message::InvalidMessage(Client_message::INVALID);

void sendClientMessage(int sock, const Client_message& msg) {
    std::string data = msg.serialize();

    if (send(sock, data.c_str(), data.size(), 0) == -1) {
        std::cerr << "Error sending data\n";
        close(sock);
    }
    // std::cout << "client message sent" << std::endl;
}

Client_message receiveAndDeserialize(int sock) {
    uint32_t dataLength;
    if (recv(sock, &dataLength, sizeof(dataLength), 0) <= 0) {
        std::cerr << "Failed to receive data length or connection closed\n";
        close(sock);
        return Client_message::InvalidMessage;
    }

    dataLength = ntohl(dataLength); // 转换字节序

    // std::cout << "receive data length: " << dataLength << std::endl;

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
