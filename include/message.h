#include <iostream>
#include <vector>
#include <utility>
#include "parameters.h" // 包含 _key_t 和 _payload_t 类型定义
#include <sys/socket.h>
#include <cstring> // For strlen, memcpy
#include <unistd.h> // For close
#include <arpa/inet.h>
#include <sstream>
#include <iomanip>


class Client_message {
public:
    enum Type { META, LOOKUP, INSERT, INVALID, RAW_KEY };

    Type type;
    _key_t key;
    union Data {
        std::vector<int> leaf_path;
        std::pair<_key_t, _payload_t> key_value;

        Data() {} // Default constructor
        Data(const Data& other) {} // Copy constructor placeholder
        ~Data() {} // Destructor placeholder
    } data;

public:
    static const Client_message InvalidMessage;

    Client_message(Type t) : type(t) {}
    Client_message(_key_t key) : type(RAW_KEY), key(key) {}
    Client_message(const std::vector<int>& path, _key_t key) : type(LOOKUP), key(key) { new (&data.leaf_path) std::vector<int>(path); }
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


std::string serialize() const {
    std::ostringstream serializedContent;
    serializedContent << static_cast<char>(type); // 添加类型信息

    if (type == LOOKUP) {
        // 以二进制形式序列化 double 类型的 key
        serializedContent.write(reinterpret_cast<const char*>(&key), sizeof(_key_t));
        serializedContent << ";";

        // std::cout << "key ; " << key << std::endl;

        for (int value : data.leaf_path) {
            serializedContent << value << ",";
        }
    } else if (type == INSERT) {
        // 以二进制形式序列化 double 类型的 key_value.first
        serializedContent.write(reinterpret_cast<const char*>(&data.key_value.first), sizeof(_key_t));
        serializedContent << ";";
        // 以二进制形式序列化 double 类型的 key_value.second
        serializedContent.write(reinterpret_cast<const char*>(&data.key_value.second), sizeof(_payload_t));
    }
    else if (type == RAW_KEY) {
        // 以二进制形式序列化 double 类型的 key
        serializedContent.write(reinterpret_cast<const char*>(&key), sizeof(_key_t));
        // serializedContent << ";";

        // std::cout << "key ; " << key << std::endl;

        // for (int value : data.leaf_path) {
        //     serializedContent << value << ",";
        // }
    }

    std::string content = serializedContent.str();
    std::string serializedData;
    uint32_t dataLength = htonl(content.size()); // 确保以网络字节顺序发送长度
    serializedData.append(reinterpret_cast<char*>(&dataLength), sizeof(dataLength));
    serializedData.append(content);

    // std::cout << "serialize: " << serializedData << std::endl;

    return serializedData;

    
}


    // 反序列化方法
    static Client_message deserialize(const std::string& serializedData) {
    if (serializedData.empty()) {
        return InvalidMessage;
    }

    Type t = static_cast<Type>(serializedData[0]);

    if (t == LOOKUP) {
        size_t pos = serializedData.rfind(';') + 1;

        // 使用二进制方式反序列化 double 类型的 key
        _key_t key;
        memcpy(&key, serializedData.data() + 1, sizeof(_key_t));  // 假设 key 紧跟类型信息后存储
        // std::cout << "key: " << key << std::endl;

        // std::cout << "deserialize key: " << key << std::endl;

        std::vector<int> path;
        std::string pathData = serializedData.substr(pos);
        size_t start = 0, end;
        try {
            while ((end = pathData.find(',', start)) != std::string::npos) {
                path.push_back(std::stoi(pathData.substr(start, end - start)));
                start = end + 1;
            }
            if (start < pathData.length()) {
                path.push_back(std::stoi(pathData.substr(start)));
            }
        } catch (const std::exception& e) {
            std::cerr << "Error during deserialization: " << e.what() << std::endl;
            // std::cout << "raw data" << serializedData << std::endl;
            // std::cout << pathData << std::endl;
            return InvalidMessage;
        }
        return Client_message(path, key);
    } else if (t == INSERT) {
        size_t pos = serializedData.find(';') + 1;
        size_t pos2 = serializedData.find(';', pos);

        // 使用二进制方式反序列化 double 类型的 key 和 payload
        _key_t key;
        _payload_t payload;
        memcpy(&key, serializedData.data() + 1, sizeof(_key_t));  // 假设 key 紧跟类型信息后存储
        memcpy(&payload, serializedData.data() + pos + 1, sizeof(_payload_t));  // 假设 payload 紧跟第一个分号后存储

        return Client_message(key, payload);
    }

    else if (t == RAW_KEY) {
        _key_t key;
        memcpy(&key, serializedData.data() + 1, sizeof(_key_t));  
        std::cout << "deserialize key: " << key << std::endl;
        return Client_message(key);
    }

    return Client_message(t);
}
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

    dataLength = ntohl(dataLength); // 转换字节序
    if (dataLength == 0) {
        std::cerr << "Data length is zero\n";
        return Client_message::InvalidMessage;
    }

    // std::cout << "data length: " << dataLength << std::endl;

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
    // if (receivedData.size() < sizeof(uint32_t)) {
    //     std::cerr << "Received data is shorter than expected\n";
    //     return Client_message::InvalidMessage;
    // }

    // Client_message receivedMsg = Client_message::deserialize(receivedData);
    // return receivedMsg;
    return Client_message::deserialize(receivedData);
}

