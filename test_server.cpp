#include <random>
#include <chrono>
#include <iostream>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <unordered_set>
#include <iomanip>
#include <fstream>
#include <cstdlib>

#include "include/plin_index.h"
#include "include/message.h"

using TestIndex = PlinIndex;
// PMAllocator * galc;

void run_search_test(TestIndex& test_index, _key_t* keys, _payload_t* payloads, size_t number){
    std::mt19937 search_gen(123);
    std::uniform_int_distribution<size_t> search_dist(0, number - 1);
    int count = 0 , false_count = 0;
    
    for(size_t i = 0; i < number; i++){
        // size_t target_pos = search_dist(search_gen);

        size_t target_pos = i;
        
        // _key_t target_key = keys[target_pos];
        _key_t target_key = keys[target_pos];
        _payload_t answer;
        
        bool ans = test_index.find(target_key, answer);

        // std::cout << answer << std::endl;
        // std::cout << payloads[target_pos] << std::endl << std::endl;
        
        if (ans == false) 
        {
            std::cout << std::fixed << std::setprecision(6) << target_key << std::endl;
            
            false_count ++ ;
        }
        if(answer != payloads[i]){
            count ++ ;
            
            // std::cout<<"#Number: "<<i<<std::endl;
            // std::cout<<"#Wrong answer: "<<answer<<std::endl;
            // std::cout<<"#Correct answer: "<<payloads[target_pos]<<std::endl;
            // test_index.find(target_key, answer);
            // throw std::logic_error("Answer wrong!");
        }
    }
    std::cout << "wrong count: " << count << std::endl;
    std::cout << "false count: " << false_count << std::endl;
}

void run_upsert_test(TestIndex& test_index, _key_t* keys, _payload_t* payloads, size_t number){
    std::normal_distribution<_key_t> key_dist(0, 1e9);
    std::uniform_int_distribution<_payload_t> payload_dist(0,1e9);
    std::mt19937 key_gen(time(NULL));
    std::mt19937 payload_gen(time(NULL));

    size_t upsert_times = 1e7;
    _key_t* new_keys = new _key_t[upsert_times];
    _payload_t* new_payloads = new _payload_t[upsert_times];
    for(size_t i = 0; i < upsert_times; i++){
        new_keys[i] = key_dist(key_gen);
        new_payloads[i] = payload_dist(payload_gen);
        test_index.upsert(new_keys[i], new_payloads[i]);
        // if (i % 10000 == 0) {
        //     std::cout << i << std::endl;
        // }
        
    }

    // std::cout << "upsert done" << std::endl;

    for(size_t i = 0; i < upsert_times; i++){
        _payload_t answer;
        test_index.find(new_keys[i], answer);
        if(answer != new_payloads[i]){
            std::cout<<"#Number: "<<i<<std::endl;
            std::cout<<"#Key: "<<new_keys[i]<<std::endl;
            std::cout<<"#Wrong answer: "<<answer<<std::endl;
            std::cout<<"#Correct answer: "<<new_payloads[i]<<std::endl;
            // test_index.find(new_keys[i], answer);
            // throw std::logic_error("Answer wrong!");
            count ++ ;
        }
    }

    std::cout << "wrong count: " << count << std::endl;
}

std::vector<int> deserializeVector(const std::vector<char>& buffer) {
    std::vector<int> vec(buffer.size() / sizeof(int));
    std::memcpy(vec.data(), buffer.data(), buffer.size());
    return vec;
}

void sendSerializedData(const std::vector<char>& buffer, int client_socket) {

    size_t totalSent = 0;
    while (totalSent < buffer.size()) {
        ssize_t sent = send(client_socket, buffer.data() + totalSent, buffer.size() - totalSent, 0);
        if (sent == -1) {
            std::cerr << "Failed to send data" << std::endl;
            break;
        }
        totalSent += sent;
    }
}

void handle_connection(int socket) {
    // 这里处理每个客户端的请求
    std::cout << "Connected with client socket " << socket << std::endl;
    // 这里可以读写数据到socket

    // 通信结束后关闭socket
    close(socket);
}

void initialize_data(size_t number, _key_t*& keys, _payload_t*& payloads) {
    std::mt19937 key_gen(456); 
    std::mt19937 payload_gen(456); 
    // std::uniform_real_distribution<double> dist(10.0, 100.0);

    std::normal_distribution<_key_t> key_dist(0, 1e9);
    std::uniform_int_distribution<_payload_t> payload_dist(0,1e9);

    keys = new _key_t[number];
    payloads = new _payload_t[number];

    // _key_t key = 0;
    // for (size_t i = 0; i < number; ++ i) {
    //     key += dist(gen);
    //     keys[i] = key;
    //     payloads[i] = key;
    // }

    std::vector<std::pair<_key_t, _payload_t>> kv_pairs(number);

    // _key_t key = 0;
    for (size_t i = 0; i < number; ++i) {
        kv_pairs[i] = std::make_pair(key_dist(key_gen), payload_dist(payload_gen));
    }
    std::sort(kv_pairs.begin(), kv_pairs.end());
    for (size_t i = 0; i < number; ++i) {
        keys[i] = kv_pairs[i].first;
        payloads[i] = kv_pairs[i].second;
    }
    std::cout << "Data initialized and sorted successfully." << std::endl;
}

void prependSizeToBuffer(std::vector<char>& buffer) {
    uint32_t size = buffer.size();
    std::vector<char> sizeBytes(sizeof(uint32_t));

    memcpy(sizeBytes.data(), &size, sizeof(uint32_t));
    buffer.insert(buffer.begin(), sizeBytes.begin(), sizeBytes.end());
}

int sendPayloads(int sock, const std::vector<_payload_t>& payloads) {
    size_t payloadSize = sizeof(_payload_t); // 获取_payload_t类型的大小
    size_t totalPayloadSize = payloadSize * payloads.size(); // 计算全部有效载荷的总大小
    size_t totalSize = totalPayloadSize + sizeof(uint32_t);

    // 创建一个足够大的缓冲区来存储大小信息和所有有效载荷
    std::vector<char> buffer(totalSize);
    char* bufferPtr = buffer.data();

    // 先将总数据大小放入缓冲区
    uint32_t totalSizeNetwork = htonl(static_cast<uint32_t>(totalPayloadSize));
    std::memcpy(bufferPtr, &totalSizeNetwork, sizeof(totalSizeNetwork));
    bufferPtr += sizeof(totalSizeNetwork);

    // 将所有有效载荷复制到缓冲区中
    for (const auto& payload : payloads) {
        std::memcpy(bufferPtr, &payload, payloadSize);
        bufferPtr += payloadSize;
    }

    // 发送整个缓冲区内容
    ssize_t bytesSent = send(sock, buffer.data(), totalSize, 0);
    if (bytesSent == -1) {
        std::cerr << "Failed to send payloads" << std::endl;
        return -1; // 发送失败
    }

    return 0; // 发送成功
}

void send_insert_Confirmation(int client_socket, size_t batch_size) {
    std::string confirmation = "Confirmation: " + std::to_string(batch_size) + " items inserted successfully.";
    send(client_socket, confirmation.c_str(), confirmation.size(), 0);
}


void handle_client(TestIndex& testIndex, int client_socket, std::vector<char>& buffer) {

    std::cout << "thread: " << client_socket << std::endl;

    while (true) {
        Client_message receivedMsg = receiveAndDeserialize(client_socket);

        // std::cout << "message deserialized" << std::endl;

        if (receivedMsg.type == Client_message::META) {
            std::cout << "META" << std::endl;
            send(client_socket, buffer.data(), buffer.size(), 0);

        }
        else if (receivedMsg.type == Client_message::LOOKUP) {
            // std::cout << "LOOKUP" << std::endl;
            std::vector<_payload_t> payloads;
            // auto findPath_start = std::chrono::high_resolution_clock::now();
            for (size_t i = 0; i < receivedMsg.batch_size; ++i) {
                _payload_t payload;
                // std::cout << "leaf path size: " << receivedMsg.leaf_paths.size() << std::endl;

                // std::cout << "leaf number: " << receivedMsg.leaf_paths[i] << std::endl;
                
                payload = testIndex.find_Payload(receivedMsg.keys[i], receivedMsg.leaf_paths[i]);

                // std::cout << "payload: " << payload << std::endl;
                

                // testIndex.find(receivedMsg.keys[i], payload);
                payloads.push_back(payload);
            }
            // auto findPath_end = std::chrono::high_resolution_clock::now();
            // auto receive_duration = std::chrono::duration_cast<std::chrono::milliseconds>(findPath_end - findPath_start);
            // std::cout << "find payload time: " << receive_duration.count() << std::endl;
            sendPayloads(client_socket, payloads);
        }
        else if (receivedMsg.type == Client_message::RAW_KEY) {
            std::vector<_payload_t> payloads;
            for (size_t i = 0; i < receivedMsg.batch_size; ++i) {
                _payload_t payload;
                testIndex.find(receivedMsg.keys[i], payload);
                payloads.push_back(payload);
            }
            sendPayloads(client_socket, payloads);
        }
        else if (receivedMsg.type == Client_message::INSERT) {
            // std::cout << "INSERT" << std::endl;
            for (size_t i = 0; i < receivedMsg.batch_size; ++i) {
                
                testIndex.upsert_Path(receivedMsg.keys[i], receivedMsg.payloads[i], receivedMsg.leaf_paths[i]);
            }
            // Optionally, confirm back to the client that the insert was successful
            send_insert_Confirmation(client_socket, receivedMsg.batch_size);
        }
        else if (receivedMsg.type == Client_message::RAW_INSERT) {
            // std::cout << "INSERT" << std::endl;
            for (size_t i = 0; i < receivedMsg.batch_size; ++i) {
                testIndex.upsert(receivedMsg.keys[i], receivedMsg.payloads[i]);
            }
            // Optionally, confirm back to the client that the insert was successful
            send_insert_Confirmation(client_socket, receivedMsg.batch_size);
        }
        else if (receivedMsg.type == Client_message::INVALID) {
            break;
        }
        // else if (receivedMsg.type == Client_message::LOOKUP) {
        //     // std::cout << "LOOKUP" << std::endl;
        //     std::vector<int> leaf_path = receivedMsg.leaf_path;
        //     _key_t key = receivedMsg.key;
        //     // std::cout << "key: " << key << std::endl;
        //     _payload_t payload = testIndex.find_Payload(leaf_path, key);

        //     // _payload_t ans;
        //     // bool found = testIndex.find(key, ans);
        //     // std::cout << "found: " << found << std::endl;

            
        //     sendPayload(client_socket, payload);

        // }
        
        // else if (receivedMsg.type == Client_message::RAW_KEY) {
        //     _key_t key = receivedMsg.key;
        //     _payload_t payload;
        //     testIndex.find(key, payload);
        //     sendPayload(client_socket, payload);
        // }
        // else if (receivedMsg.type == Client_message::INSERT) {
        //     std::cout << "INSERT" << std::endl;
        //     _key_t key = receivedMsg.key;
        //     _payload_t payload = receivedMsg.payload;
        //     // std::cout << "true key: " << key << std::endl;
        //     // std::cout << "true payload: " << payload << std::endl;
        //     _payload_t ans;

        //     std::vector<int> leaf_path = receivedMsg.leaf_path;
        //     testIndex.upsert(key, payload);
        //     // testIndex.find(key, ans);
        //     // if (ans != payload)
        //     //     std::cout << "upsert wrong!" << std::endl;
        //     // else 
        //     //     std::cout << "upsert wrong!" << std::endl;

        //     sendPayload(client_socket, payload);
        // }

    }

    // char buf[1024];
    // while (true) {
    //     int recv_len = recv(client_socket, buf, sizeof(buf), 0);
    //     if (recv_len <= 0) break;  // 客户端断开连接

    //     if (std::strcmp(buf, "META") == 0) {
    //         // 发送序列化数据
    //         send(client_socket, buffer.data(), buffer.size(), 0);
    //     } else {
    //         // 处理其他请求
    //         std::cout << "Received other data." << std::endl;
    //     }
    // }
    close(client_socket);
}

void start_server(int port, std::vector<char>& buffer, TestIndex& testIndex) {
    int server_fd, client_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 3) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    std::cout << "Server started on port " << port << std::endl;

    while (true) {
        client_socket = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&addrlen);
        if (client_socket < 0) {
            perror("accept failed");
            continue;
        }

        std::thread(handle_client, std::ref(testIndex), client_socket, std::ref(buffer)).detach();
        
    }
    close(server_fd);
}

void test() {
    
}

int main(int argc, char* argv[]) {

    int port = 8080;
    size_t number = 1e7;

    if (argc == 3) {
        port = std::stoi(argv[1]); 
        number = static_cast<size_t>(std::stod(argv[2]));
    }

    std::cout << "Port: " << port << " number: " << number << std::endl;

    _key_t* keys = nullptr;
    _payload_t* payloads = nullptr;

    initialize_data(number, keys, payloads);
    TestIndex test_index;
    test_index.bulk_load(keys, payloads, number);
    test_index.PrintInfo();

    // run_upsert_test(test_index, keys, payloads, number);

    run_search_test(test_index, keys, payloads, number);

    std::vector<char> buffer;
    test_index.serializePlinIndex(buffer);
    prependSizeToBuffer(buffer);

    start_server(port, buffer, test_index);
    return 0;
}

// void test(uint64_t thread_cnt){
//     size_t number = 1e7;
//     // std::uniform_int_distribution<uint64_t> key_dist(0, 2e8);
//     std::uniform_int_distribution<_payload_t> payload_dist(0, 1e8);

//     // std::mt19937 key_gen(123);   
//     std::mt19937 gen(456); 
//     std::uniform_real_distribution<double> dist(10.0, 100.0);

//     _key_t* keys = new _key_t[number];      // 动态分配数组存储keys
//     _payload_t* payloads = new _payload_t[number]; // 动态分配数组存储payloads

//     _key_t key = 0;
//     _payload_t payload = 0;
//     for (int i = 0; i < number; i ++ )
//     {
//         key += dist(gen);
//         keys[i] = key;
//         payloads[i] = key;
//     }

//     // Sort keys
//     std::sort(keys, keys+number);

//     std::cout << std::fixed << std::setprecision(6) << keys[number - 1] << std::endl;

//     std::cout << "sort successfully" << std::endl;

//     // NVM pool path
    
//     TestIndex test_index;

//     std::cout << "init successfully" << std::endl;

//     test_index.bulk_load(keys, payloads, number);

//     std::cout << "load successfully" << std::endl; 
//     std::vector<char> buffer;
//     test_index.serializePlinIndex(buffer);
//     test_index.PrintInfo();

//     run_search_test(test_index, keys, payloads, number);

//     int server_fd, new_socket;
//     struct sockaddr_in address;
//     int opt = 1;
//     int addrlen = sizeof(address);
//     int port = 8080;

//     // 创建 socket 文件描述符
//     if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
//         perror("socket failed");
//         exit(EXIT_FAILURE);
//     }

//     // 强制附加 socket 到端口 8080
//     if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
//         perror("setsockopt");
//         exit(EXIT_FAILURE);
//     }

//     address.sin_family = AF_INET;
//     address.sin_addr.s_addr = INADDR_ANY;
//     address.sin_port = htons(port);

//     // 绑定 socket 到地址
//     if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
//         perror("bind failed");
//         exit(EXIT_FAILURE);
//     }

//     // 开始监听
//     if (listen(server_fd, 3) < 0) {
//         perror("listen");
//         exit(EXIT_FAILURE);
//     }

//     // 接受客户端连接
//     if ((new_socket = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&addrlen))<0) {
//         perror("accept");
//         exit(EXIT_FAILURE);
//     }

//     std::cout << "Connect success" << std::endl;

//     while (true)
//     {
//         char buf[1024] = "";
//         int recv_len = recv(new_socket, buf, sizeof(buf), 0);

//         if (std::strcmp(buf, "META") == 0) 
//         {
//             sendSerializedData(buffer, new_socket);
//         }
//         else
//         {
//             std::vector<int> leaf_path = deserializeVector(std::vector<char>(buf, buf + recv_len));
            

//         }
//     }

//     close(new_socket);
//     close(server_fd);

// }

// int main() {
//     test(1);
// }
