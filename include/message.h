#include <iostream>
#include <vector>
#include <utility>
#include "parameters.h" // 包含 _key_t 和 _payload_t 类型定义
#include <sys/socket.h>
#include <cstring> // For strlen, memcpy
#include <unistd.h> // For close
#include <arpa/inet.h>

class Client_message {
public:
    enum Type { META, LOOKUP, INSERT };

    Type type;
    union Data {
        std::vector<int> leaf_path;
        std::pair<_key_t, _payload_t> key_value;

        Data() {} // Default constructor
        Data(const Data& other) {} // Copy constructor placeholder
        ~Data() {} // Destructor placeholder
    } data;

public:
    static const Client_message InvalidMessage;

    Client_message() : type(META) { new (&data.leaf_path) std::vector<int>(); }
    Client_message(const std::vector<int>& path) : type(LOOKUP) { new (&data.leaf_path) std::vector<int>(path); }
    Client_message(_key_t key, _payload_t payload) : type(INSERT) { new (&data.key_value) std::pair<_key_t, _payload_t>(key, payload); }

    // Copy constructor
    Client_message(const Client_message& other) : type(other.type) {
        if (type == LOOKUP) {
            new (&data.leaf_path) std::vector<int>(other.data.leaf_path);
        } else if (type == INSERT) {
            new (&data.key_value) std::pair<_key_t, _payload_t>(other.data.key_value);
        }
    }

    // Copy assignment operator
    Client_message& operator=(const Client_message& other) {
        if (this != &other) {
            this->~Client_message(); // Clean up current state
            type = other.type;
            if (type == LOOKUP) {
                new (&data.leaf_path) std::vector<int>(other.data.leaf_path);
            } else if (type == INSERT) {
                new (&data.key_value) std::pair<_key_t, _payload_t>(other.data.key_value);
            }
        }
        return *this;
    }

    ~Client_message() {
        if (type == LOOKUP) {
            data.leaf_path.~vector<int>();
        } else if (type == INSERT) {
            data.key_value.~pair<_key_t, _payload_t>();
        }
    }

    // 序列化方法
    std::string serialize() const {
        std::string serializedContent;
        serializedContent += static_cast<char>(type); // 添加类型信息

        if (type == LOOKUP) {
            int pathSize = data.leaf_path.size();
            serializedContent += std::to_string(pathSize) + ";";
            for (int value : data.leaf_path) {
                serializedContent += std::to_string(value) + ",";
            }
        } else if (type == INSERT) {
            serializedContent += std::to_string(data.key_value.first) + ";" + std::to_string(data.key_value.second);
        }
        // 将数据长度放在最前面
        uint32_t dataLength = htonl(serializedContent.size()); // 确保以网络字节顺序发送长度
        std::string serializedData;
        serializedData.append(reinterpret_cast<char*>(&dataLength), sizeof(dataLength));
        serializedData.append(serializedContent);

        return serializedData;
    }

    // 反序列化方法
    static Client_message deserialize(const std::string& serializedData) {
        if (serializedData.empty()) {
            return InvalidMessage;
        }

        Type t = static_cast<Type>(serializedData[0]);

        if (t == LOOKUP) {
            size_t pos = serializedData.find(';') + 1;
            int pathSize = std::stoi(serializedData.substr(1, pos - 2));
            std::vector<int> path;
            std::string pathData = serializedData.substr(pos);
            size_t start = 0, end;
            while ((end = pathData.find(',', start)) != std::string::npos) {
                path.push_back(std::stoi(pathData.substr(start, end - start)));
                start = end + 1;
            }
            return Client_message(path);
        } else if (t == INSERT) {
            size_t pos = serializedData.find(';') + 1;
            size_t pos2 = serializedData.find(';', pos);
            _key_t key = std::stoll(serializedData.substr(1, pos - 2));
            _payload_t payload = std::stoll(serializedData.substr(pos, pos2 - pos));
            return Client_message(key, payload);
        }

        return Client_message();
    }
};

const Client_message Client_message::InvalidMessage;

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

    dataLength = ntohl(dataLength); // 转换字节序
    if (dataLength == 0) {
        std::cerr << "Data length is zero\n";
        return Client_message::InvalidMessage;
    }

    std::vector<char> buffer(dataLength); // 使用 vector 来存储接收的数据
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
    if (receivedData.size() < sizeof(uint32_t)) {
        std::cerr << "Received data is shorter than expected\n";
        return Client_message::InvalidMessage;
    }

    Client_message receivedMsg = Client_message::deserialize(receivedData.substr(sizeof(uint32_t)));
    return receivedMsg;
}

