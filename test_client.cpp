#include <random>
#include <chrono>
#include <iostream>
#include <vector>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <future>
#include <algorithm>
#include <gflags/gflags.h>

#include "include/plin_index.h"
#include "include/message.h"
#include "include/TaskScheduler.h"

using TestIndex = PlinIndex;

DEFINE_int32(port, 8080, "Port number");
DEFINE_double(number, 1e7, "Number of keys");
DEFINE_double(test_size, 1e6, "Test size");
DEFINE_string(index_type, "plin", "Index type (plin, ALEX, BPlus, Btree)");
DEFINE_double(batch_size, 1000, "Batch size");
DEFINE_string(key_distribution, "lognormal", "Key distribution (lognormal, exponential, uniform, normal)");
DEFINE_double(upsert_ratio, 0, "Upsert_ratio");
DEFINE_int32(query_range, 0, "Query range");
DEFINE_int32(num_threads, 4, "Number of threads");

void SetCustomUsageMessage() {
    gflags::SetUsageMessage(
        "Usage: ./your_program [options]\n"
        "Options:\n"
        "  --port=<port>           Port number (default: 8080)\n"
        "  --number=<number>       Number of keys (default: 1e7)\n"
        "  --test_size=<size>      Test size (default: 1e6)\n"
        "  --index_type=<index>    Index type (plin_cache, plin, alex, bplus, btree) (default: plin_cache)\n"
        "  --batch_size=<size>     Batch size (default: 1000)\n"
        "  --key_distribution=<distribution> Key distribution (lognormal, exponential, uniform, normal, zipf) (default: lognormal)\n"
        "  --query_range=<query_range> Range Query Scan length (default 0)"
        "  --num_threads=<threads> Number of threads (default: 4)\n"
        "\n"
    );
}


void initialize_data(size_t number, _key_t*& keys, _payload_t*& payloads, const std::string& distribution) {
    std::mt19937 key_gen(456); 
    std::mt19937 payload_gen(456); 

    std::uniform_int_distribution<_payload_t> payload_dist(0,1e9);

    keys = new _key_t[number];
    payloads = new _payload_t[number];

    std::vector<std::pair<_key_t, _payload_t>> kv_pairs(number);

    if (distribution == "lognormal") {
        std::lognormal_distribution<_key_t> key_dist(0, 10);
        for (size_t i = 0; i < number; ++i) {
        kv_pairs[i] = std::make_pair(key_dist(key_gen), payload_dist(payload_gen));
    }
    } else if (distribution == "exponential") {
        std::exponential_distribution<_key_t> key_dist(1);
        for (size_t i = 0; i < number; ++i) {
        kv_pairs[i] = std::make_pair(key_dist(key_gen), payload_dist(payload_gen));
    }
    } else if (distribution == "uniform") {
        std::uniform_real_distribution<_key_t> key_dist(0, 1e9);
        for (size_t i = 0; i < number; ++i) {
        kv_pairs[i] = std::make_pair(key_dist(key_gen), payload_dist(payload_gen));
    }
    } else if (distribution == "normal") {
        std::normal_distribution<_key_t> key_dist(0, 1e9);
        for (size_t i = 0; i < number; ++i) {
        kv_pairs[i] = std::make_pair(key_dist(key_gen), payload_dist(payload_gen));
    }
    } else {
        throw std::invalid_argument("Unsupported key distribution type");
    }
    
    std::sort(kv_pairs.begin(), kv_pairs.end());

    // std::sort(kv_pairs, kv_pairs + number, [](const std::pair<_key_t, _payload_t>& a, const std::pair<_key_t, _payload_t>& b) {
    //     return a.first < b.first;
    // });

    for (size_t i = 0; i < number; ++i) {
        keys[i] = kv_pairs[i].first;
        payloads[i] = kv_pairs[i].second;
    }
    std::cout << "Data initialized and sorted successfully." << std::endl;
}

int start_client(int port) {
    int sock = 0, valread;
    struct sockaddr_in serv_addr;
    char buffer[1024] = {0};
    std::string ipAddress = "127.0.0.1"; 

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        std::cout << "Socket creation error" << std::endl;
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if(inet_pton(AF_INET, ipAddress.c_str(), &serv_addr.sin_addr)<=0) {
        std::cout << "Invalid address/ Address not supported" << std::endl;
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cout << "Connection Failed" << std::endl;
        return -1;
    }

    return sock;
}

TestIndex GetMeta(int server_sock) {

    Client_message msg(Client_message::META);
    TestIndex testindex;

    sendClientMessage(server_sock, msg);
        
    // 首先接收数据的大小信息
    uint32_t dataSize = 0;
    int bytesRead = recv(server_sock, &dataSize, sizeof(dataSize), 0);
    if (bytesRead != sizeof(dataSize)) {
        std::cerr << "Failed to receive data size" << std::endl;
        close(server_sock);
        return testindex;
    }

    std::vector<char> receivedData(dataSize);

    // 现在接收实际的数据
    char* bufPtr = receivedData.data();
    uint32_t bytesToReceive = dataSize;
    while (bytesToReceive > 0) {
        int bytesReceived = recv(server_sock, bufPtr, bytesToReceive, 0);
        if (bytesReceived <= 0) {
            std::cerr << "Failed to receive data or connection closed" << std::endl;
            break;
        }
        bytesToReceive -= bytesReceived;
        bufPtr += bytesReceived;
    }

    // std::cout << "Received data size: " << receivedData.size() << " bytes" << std::endl;
        
    
    testindex.deserializePlinIndex(receivedData);

    return testindex;
}

int receivePayloads(int sock, std::vector<_payload_t>& payloads, size_t batch_size) {
    uint32_t dataSize;
    ssize_t bytesReceived = recv(sock, &dataSize, sizeof(dataSize), 0);
    if (bytesReceived == -1) {
        std::cerr << "Failed to receive data size" << std::endl;
        return -1;  // 接收失败
    }
    if (bytesReceived != sizeof(dataSize)) {
        std::cerr << "Received incomplete data size" << std::endl;
        return -1;  // 数据大小信息未完全接收
    }

    dataSize = ntohl(dataSize); // 确保数据大小信息符合主机字节序

    std::vector<char> buffer;
    buffer.resize(dataSize); // 调整缓冲区大小以匹配数据大小

    size_t totalReceived = 0; // 已接收数据的总量
    while (totalReceived < dataSize) {
        bytesReceived = recv(sock, buffer.data() + totalReceived, dataSize - totalReceived, 0);
        // std::cout << "bytesReceived " << bytesReceived << std::endl;
        if (bytesReceived == -1) {
            std::cerr << "Failed to receive payload" << std::endl;
            return -1; // 接收失败
        }
        if (bytesReceived == 0) {
            std::cerr << "Connection closed prematurely" << std::endl;
            return -1; // 连接过早关闭
        }
        totalReceived += bytesReceived; // 更新已接收数据总量
    }

    

    const size_t payloadSize = sizeof(_payload_t); // 获取_payload_t类型的大小
    payloads.clear(); // 清空已有内容
    payloads.reserve(batch_size); // 预先分配足够的空间

    // 遍历buffer，每次移动_payload_t大小的步长
    for (size_t i = 0; i < batch_size; ++i) {
        // 计算当前_payload_t数据的起始指针
        const char* payloadPtr = buffer.data() + i * payloadSize;
        // 将指针转换为_payload_t类型，并添加到向量中
        const _payload_t* payloadData = reinterpret_cast<const _payload_t*>(payloadPtr);
        payloads[i] = *payloadData;
    }
    return 0; // 接收成功
}

int receiveResults(int sock, std::vector<std::vector<std::pair<_key_t, _payload_t>>>& results, size_t batch_size) {
    uint32_t dataSize;
    ssize_t bytesReceived = recv(sock, &dataSize, sizeof(dataSize), 0);
    if (bytesReceived == -1) {
        std::cerr << "Failed to receive data size" << std::endl;
        return -1;  // 接收失败
    }
    if (bytesReceived != sizeof(dataSize)) {
        std::cerr << "Received incomplete data size" << std::endl;
        return -1;  // 数据大小信息未完全接收
    }

    dataSize = ntohl(dataSize); // 确保数据大小信息符合主机字节序

    std::vector<char> buffer;
    buffer.resize(dataSize); // 调整缓冲区大小以匹配数据大小

    size_t totalReceived = 0; // 已接收数据的总量
    while (totalReceived < dataSize) {
        bytesReceived = recv(sock, buffer.data() + totalReceived, dataSize - totalReceived, 0);
        if (bytesReceived == -1) {
            std::cerr << "Failed to receive results" << std::endl;
            return -1; // 接收失败
        }
        if (bytesReceived == 0) {
            std::cerr << "Connection closed prematurely" << std::endl;
            return -1; // 连接过早关闭
        }
        totalReceived += bytesReceived; // 更新已接收数据总量
    }

    // 解析接收到的结果
    const char* bufferPtr = buffer.data();
    results.clear(); // 清空已有内容

    while (bufferPtr < buffer.data() + dataSize) {
        uint32_t vecSizeNetwork;
        std::memcpy(&vecSizeNetwork, bufferPtr, sizeof(vecSizeNetwork));
        bufferPtr += sizeof(vecSizeNetwork);
        uint32_t vecSize = ntohl(vecSizeNetwork);

        std::vector<std::pair<_key_t, _payload_t>> vec;
        vec.reserve(vecSize);

        for (uint32_t i = 0; i < vecSize; ++i) {
            _key_t key;
            _payload_t payload;

            std::memcpy(&key, bufferPtr, sizeof(_key_t));
            bufferPtr += sizeof(_key_t);
            std::memcpy(&payload, bufferPtr, sizeof(_payload_t));
            bufferPtr += sizeof(_payload_t);

            vec.emplace_back(key, payload);
        }

        results.push_back(std::move(vec));

        // 如果达到批量大小则返回
        if (results.size() >= batch_size) {
            break;
        }
    }

    return 0; // 接收成功
}


void run_search_test_client(int sock, TestIndex& test_index, _key_t* keys, _payload_t* payloads, 
                            size_t number, size_t test_size, size_t batch_size, bool cache) {
    TaskScheduler scheduler;
    auto start = std::chrono::high_resolution_clock::now();

    // 数据准备线程
    std::thread prepThread([&scheduler, &test_index, keys, payloads, number, test_size, batch_size, cache]() {
        std::mt19937 gen(time(NULL));
        std::uniform_int_distribution<size_t> dist(0, number - 1);

        std::vector<_key_t> batch_keys;
        std::vector<_payload_t> batch_payloads;
        std::vector<int> leaf_paths;
        std::string message;

        for (size_t i = 0; i < test_size; i += batch_size) {
            size_t real_batch_size = std::min(batch_size, test_size - i);
            batch_keys.clear();
            batch_payloads.clear();
            leaf_paths.clear();
            
            if (cache) {
                for (size_t j = 0; j < real_batch_size; ++j) {
                    size_t target_pos = dist(gen);
                    batch_keys.push_back(keys[target_pos]);
                    batch_payloads.push_back(payloads[target_pos]);
                    test_index.find_Path(batch_keys[j], leaf_paths); 
                }
            } else {
                for (size_t j = 0; j < real_batch_size; ++j) {
                    size_t target_pos = dist(gen);
                    batch_keys.push_back(keys[target_pos]);
                    batch_payloads.push_back(payloads[target_pos]);
                }           
            }
            
            if (cache) {
                Client_message msg(leaf_paths, batch_keys, real_batch_size);
                message = msg.serialize();
            } else {
                Client_message msg(batch_keys, real_batch_size);
                message = msg.serialize();
            }
        
            auto data = std::make_tuple(message, real_batch_size, batch_payloads);
            scheduler.addTask(std::move(data));
        }
    });

    // 通信线程
    std::thread commThread([sock, &scheduler, test_size, batch_size]() {
        int false_count = 0;
        // std::chrono::milliseconds receive_duration(0);
        size_t processed = 0;

        while (processed < test_size) {
            auto [message, real_batch_size, batch_payloads] = scheduler.getTask();

            auto findPath_start = std::chrono::high_resolution_clock::now();
            sendClientMessage(sock, message);
            std::vector<_payload_t> recv_payloads(real_batch_size);

            receivePayloads(sock, recv_payloads, real_batch_size);
            // auto findPath_end = std::chrono::high_resolution_clock::now();
            // receive_duration += std::chrono::duration_cast<std::chrono::milliseconds>(findPath_end - findPath_start);
            
            processed += real_batch_size;
        }

        // std::cout << "receive time: " << receive_duration.count() << " milliseconds" << std::endl;
    });

    prepThread.join();
    commThread.join();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Total search time: " << duration.count() << " milliseconds" << std::endl;
}

void run_range_query_test_client(int sock, TestIndex& test_index, _key_t* keys, _payload_t* payloads, 
                            size_t number, size_t test_size, size_t batch_size, size_t query_range, bool cache) {
    TaskScheduler scheduler;
    auto start = std::chrono::high_resolution_clock::now();

    // 数据准备线程
    std::thread prepThread([&scheduler, &test_index, keys, payloads, number, test_size, batch_size, query_range, cache]() {
        std::mt19937 gen(time(NULL));
        std::uniform_int_distribution<size_t> dist(0, number - 1 - query_range);

        std::vector<_key_t> batch_keys_lower;
        std::vector<_key_t> batch_keys_upper;
        std::vector<_payload_t> batch_payloads;
        std::vector<int> leaf_paths;
        std::string message;

        for (size_t i = 0; i < test_size; i += batch_size) {
            size_t real_batch_size = std::min(batch_size, test_size - i);
            batch_keys_lower.clear();
            batch_keys_upper.clear();
            batch_payloads.clear();
            leaf_paths.clear();
            
            if (cache) {
                for (size_t j = 0; j < real_batch_size; ++j) {
                    size_t target_pos = dist(gen);
                    batch_keys_lower.push_back(keys[target_pos]);
                    batch_keys_upper.push_back(keys[target_pos + query_range]);
                    batch_payloads.push_back(payloads[target_pos]);
                    test_index.find_Path(batch_keys_lower[j], leaf_paths); 
                }
            } else {
                for (size_t j = 0; j < real_batch_size; ++j) {
                    size_t target_pos = dist(gen);
                    batch_keys_lower.push_back(keys[target_pos]);
                    batch_keys_upper.push_back(keys[target_pos + query_range]);
                    batch_payloads.push_back(payloads[target_pos]);
                }           
            }
            
            if (cache) {
                Client_message msg(leaf_paths, batch_keys_lower, batch_keys_upper, real_batch_size);
                message = msg.serialize();
            } else {
                Client_message msg(batch_keys_lower, batch_keys_upper, real_batch_size);
                message = msg.serialize();
            }
        
            auto data = std::make_tuple(message, real_batch_size, batch_payloads);
            scheduler.addTask(std::move(data));
        }
    });

    // 通信线程
    std::thread commThread([sock, &scheduler, test_size, batch_size]() {
        int false_count = 0;
        // std::chrono::milliseconds receive_duration(0);
        size_t processed = 0;

        while (processed < test_size) {
            auto [message, real_batch_size, batch_payloads] = scheduler.getTask();

            auto findPath_start = std::chrono::high_resolution_clock::now();
            sendClientMessage(sock, message);
            std::vector<std::vector<std::pair<_key_t, _payload_t>>> results(real_batch_size);

            receiveResults(sock, results, real_batch_size);
            // auto findPath_end = std::chrono::high_resolution_clock::now();
            // receive_duration += std::chrono::duration_cast<std::chrono::milliseconds>(findPath_end - findPath_start);
            
            processed += real_batch_size;
        }

        // std::cout << "receive time: " << receive_duration.count() << " milliseconds" << std::endl;
    });

    prepThread.join();
    commThread.join();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Total range query time: " << duration.count() << " milliseconds" << std::endl;
}

void run_range_query_test_benchmark(int sock, TestIndex& test_index, _key_t* keys, _payload_t* payloads, 
                            size_t number, size_t test_size, size_t batch_size, size_t query_range, Client_message::Type type) {
    TaskScheduler scheduler;
    auto start = std::chrono::high_resolution_clock::now();

    // 数据准备线程
    std::thread prepThread([&scheduler, &test_index, keys, payloads, number, test_size, batch_size, query_range, type]() {
        std::mt19937 gen(time(NULL));
        std::uniform_int_distribution<size_t> dist(0, number - 1 - query_range);

        std::vector<_key_t> batch_keys_lower;
        std::vector<_key_t> batch_keys_upper;
        std::vector<_payload_t> batch_payloads;
        std::vector<int> leaf_paths;
        std::string message;

        for (size_t i = 0; i < test_size; i += batch_size) {
            size_t real_batch_size = std::min(batch_size, test_size - i);
            batch_keys_lower.clear();
            batch_keys_upper.clear();
            batch_payloads.clear();
            leaf_paths.clear();
            
            for (size_t j = 0; j < real_batch_size; ++j) {
                    size_t target_pos = dist(gen);
                    batch_keys_lower.push_back(keys[target_pos]);
                    batch_keys_upper.push_back(keys[target_pos + query_range]);
                    batch_payloads.push_back(payloads[target_pos]);
            }           
            
            Client_message msg(type, batch_keys_lower, batch_keys_upper, real_batch_size);
            message = msg.serialize();
        
            auto data = std::make_tuple(message, real_batch_size, batch_payloads);
            scheduler.addTask(std::move(data));
        }
    });

    // 通信线程
    std::thread commThread([sock, &scheduler, test_size, batch_size]() {
        int false_count = 0;
        // std::chrono::milliseconds receive_duration(0);
        size_t processed = 0;

        while (processed < test_size) {
            auto [message, real_batch_size, batch_payloads] = scheduler.getTask();

            auto findPath_start = std::chrono::high_resolution_clock::now();
            sendClientMessage(sock, message);
            std::vector<std::vector<std::pair<_key_t, _payload_t>>> results(real_batch_size);

            receiveResults(sock, results, real_batch_size);
            // auto findPath_end = std::chrono::high_resolution_clock::now();
            // receive_duration += std::chrono::duration_cast<std::chrono::milliseconds>(findPath_end - findPath_start);
            
            processed += real_batch_size;
        }

        // std::cout << "receive time: " << receive_duration.count() << " milliseconds" << std::endl;
    });

    prepThread.join();
    commThread.join();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Total range query time: " << duration.count() << " milliseconds" << std::endl;
}


int receiveInsertConfirmation(int server_socket) {
    char buffer[1024] = {0}; 
    int bytes_received = recv(server_socket, buffer, sizeof(buffer) - 1, 0);
    if (bytes_received > 0) {
        return 1;
    } else if (bytes_received == 0) {
        std::cout << "Server connection closed." << std::endl;
        return 0;
    } else {
        std::cerr << "Failed to receive confirmation from server." << std::endl;
        return 0;
    }
}            // leaf_path.push_back(i);

void run_upsert_test_client(int server_sock, TestIndex& test_index, _key_t* upsert_keys, size_t upsert_times, size_t batch_size, bool cache, const std::string& distribution) {
    TaskScheduler scheduler;

    auto start = std::chrono::high_resolution_clock::now();

    // std::cout << "start" << std::endl;

    // 数据准备线程
    std::thread prepThread([&scheduler, &test_index, upsert_times, upsert_keys, batch_size, cache, distribution]() {
        std::lognormal_distribution<_key_t> key_dist_log(0, 10);
        std::exponential_distribution<_key_t> key_dist_exp(1);
        std::uniform_real_distribution<_key_t> key_dist_uni(0, 1e9);
        std::normal_distribution<_key_t> key_dist_nor(0, 1e9);

        std::mt19937 gen(time(NULL)); 
        std::uniform_int_distribution<size_t> dist(0, upsert_times - 1);
        std::mt19937 payload_gen(time(NULL));
        std::uniform_int_distribution<_payload_t> payload_dist(0, 1e9);

        std::vector<int> leaf_path;
        std::vector<_key_t> batch_keys;
        std::vector<_payload_t> batch_payloads;

        for (size_t i = 0; i < upsert_times; i += batch_size) {
            leaf_path.clear(); batch_keys.clear(); batch_payloads.clear();
            size_t real_batch_size = std::min(batch_size, upsert_times - i);
            _key_t target_key;
            _payload_t target_payload;
            
            if (cache == true) {
                for (size_t j = 0; j < real_batch_size ; ++j) {
                    if (distribution == "lognormal")
                        target_key = key_dist_log(gen);
                    else if (distribution == "normal")
                        target_key = key_dist_nor(gen);
                    else if (distribution == "exponential")
                        target_key = key_dist_exp(gen);
                    else if (distribution == "uniform")
                        target_key = key_dist_uni(gen);
                    else
                        throw std::invalid_argument("Unsupported key distribution type");
                    // target_key = upsert_keys[dist(gen)];
                    target_payload = payload_dist(payload_gen);

                    batch_keys.push_back(target_key);
                    batch_payloads.push_back(target_payload);
                    test_index.find_Path(target_key, leaf_path);
                }
            }
            else {
                for (size_t j = 0; j < real_batch_size ; ++j) {
                    if (distribution == "lognormal")
                        target_key = key_dist_log(gen);
                    else if (distribution == "normal")
                        target_key = key_dist_nor(gen);
                    else if (distribution == "exponential")
                        target_key = key_dist_exp(gen);
                    else if (distribution == "uniform")
                        target_key = key_dist_uni(gen);
                    else
                        throw std::invalid_argument("Unsupported key distribution type");
                    // target_key = upsert_keys[dist(gen)];
                    target_payload = payload_dist(payload_gen);

                    batch_keys.push_back(target_key);
                    batch_payloads.push_back(target_payload);
                }
            }
            
            // std::cout << "find path done" << std::endl;

            std::string message;

            if (cache == true) {
                Client_message msg(batch_keys, batch_payloads, leaf_path, batch_size);
                message = msg.serialize();
            }
            else {
                Client_message msg(batch_keys, batch_payloads, batch_size);
                message = msg.serialize();
            }
            
            auto data = std::make_tuple(message, batch_size, batch_payloads);
            scheduler.addTask(std::move(data));

            // std::cout << "add task" << std::endl;
        }
    });

    // 通信线程
    std::thread commThread([server_sock, &scheduler, upsert_times]() {
        size_t processed = 0;
        while (processed < upsert_times) {

            // std::cout << "getting task" << std::endl;

            auto [message, real_batch_size, _] = scheduler.getTask(); // 不需要实际负载

            // std::cout << "get task" << std::endl;

            sendClientMessage(server_sock, message);
            
            receiveInsertConfirmation(server_sock);

            processed += real_batch_size;
        }
    });

    prepThread.join();
    commThread.join();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Total upsert time: " << duration.count() << " milliseconds" << std::endl;

    // std::cout << "Justify: " << std::endl;

    // run_search_test_client(server_sock, test_index, new_keys, new_payloads, upsert_times, upsert_times, batch_size, false);
}

void run_upsert_test_benchmark(int server_sock, _key_t* upsert_keys, size_t upsert_times, size_t batch_size, Client_message::Type type, const std::string& distribution) {
    TaskScheduler scheduler;

    auto start = std::chrono::high_resolution_clock::now();

    std::cout << "start" << std::endl;

    std::thread prepThread([&scheduler, upsert_times, upsert_keys, batch_size, type, distribution]() {
        std::lognormal_distribution<_key_t> key_dist_log(0, 10);
        std::exponential_distribution<_key_t> key_dist_exp(1);
        std::uniform_real_distribution<_key_t> key_dist_uni(0, 1e9);
        std::normal_distribution<_key_t> key_dist_nor(0, 1e9);

        std::mt19937 gen(time(NULL)); 
        // std::uniform_int_distribution<size_t> dist(0, upsert_times - 1);
        std::mt19937 payload_gen(time(NULL));
        std::uniform_int_distribution<_payload_t> payload_dist(0, 1e9);

        std::vector<int> leaf_path;
        std::vector<_key_t> batch_keys;
        std::vector<_payload_t> batch_payloads;

        for (size_t i = 0; i < upsert_times; i += batch_size) {
            leaf_path.clear(); batch_keys.clear(); batch_payloads.clear();
            size_t real_batch_size = std::min(batch_size, upsert_times - i);
            _key_t target_key;
            _payload_t target_payload;
            
            for (size_t j = 0; j < real_batch_size ; ++j) {
                if (distribution == "lognormal")
                    target_key = key_dist_log(gen);
                else if (distribution == "normal")
                    target_key = key_dist_nor(gen);
                else if (distribution == "exponential")
                    target_key = key_dist_exp(gen);
                else if (distribution == "uniform")
                    target_key = key_dist_uni(gen);
                else
                    throw std::invalid_argument("Unsupported key distribution type");
                // target_key = upsert_keys[dist(gen)];
                target_payload = payload_dist(payload_gen);

                batch_keys.push_back(target_key);
                batch_payloads.push_back(target_payload);
            }

            std::string message;

            Client_message msg(type, batch_keys, batch_payloads, batch_size);
            message = msg.serialize();

            
            auto data = std::make_tuple(message, batch_size, batch_payloads);
            scheduler.addTask(std::move(data));

        }
    });

    // 通信线程
    std::thread commThread([server_sock, &scheduler, upsert_times]() {
        size_t processed = 0;
        while (processed < upsert_times) {

            auto [message, real_batch_size, _] = scheduler.getTask(); // 不需要实际负载

            sendClientMessage(server_sock, message);
            
            receiveInsertConfirmation(server_sock);

            processed += real_batch_size;
        }
    });

    prepThread.join();
    commThread.join();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Total upsert time : " << duration.count() << " milliseconds" << std::endl;

}

void run_search_test_benchmark(int sock, _key_t* keys, _payload_t* payloads, size_t number, size_t test_size, size_t batch_size, Client_message::Type type) {
    TaskScheduler scheduler;
    
    auto start = std::chrono::high_resolution_clock::now();

    std::thread prepThread([&scheduler, keys, payloads, number, test_size, batch_size, type]() {
        std::mt19937 gen(time(NULL));
        std::uniform_int_distribution<size_t> dist(0, number - 1);

        for (size_t i = 0; i < test_size; i += batch_size) {
            size_t real_batch_size = std::min(batch_size, test_size - i);
            std::vector<_key_t> batch_keys(real_batch_size);
            std::vector<_payload_t> batch_payloads(real_batch_size);

            for (size_t j = 0; j < real_batch_size; ++j) {
                size_t target_pos = dist(gen);
                batch_keys[j] = keys[target_pos];
                batch_payloads[j] = payloads[target_pos];
            }

            Client_message msg(type, batch_keys, real_batch_size);
            std::string message = msg.serialize();

            auto data = std::make_tuple(message, real_batch_size, batch_payloads);
            scheduler.addTask(std::move(data));
        }
    });

    std::thread commThread([sock, &scheduler, test_size, batch_size]() {
        size_t processed = 0;
        // std::chrono::milliseconds receive_duration(0);

        while (processed < test_size) {
            auto [message, real_batch_size, batch_payloads] = scheduler.getTask();

            sendClientMessage(sock, message);

            std::vector<_payload_t> recv_payloads(real_batch_size);
            receivePayloads(sock, recv_payloads, real_batch_size);

            processed += real_batch_size;
        }
    });

    prepThread.join();
    commThread.join();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Total search time: " << duration.count() << " milliseconds" << std::endl;
}

void run_test_client(int thread_id, int port, size_t number, size_t test_size, const std::string& index_type, size_t batch_size, const std::string& key_distribution, int query_range, double upsert_ratio) {
    _key_t* keys = nullptr;
    _payload_t* payloads = nullptr;

    initialize_data(number, keys, payloads, key_distribution);

    int server_sock = start_client(port);
    if (server_sock == -1) {
        std::cerr << "Thread " << thread_id << ": Failed to connect to server" << std::endl;
        return;
    }

    TestIndex testindex;
    if (index_type == "plin_cache") {
        testindex = GetMeta(server_sock);
    }

    // auto start_time = std::chrono::steady_clock::now();
    if (index_type == "plin_cache") {
        std::cout << "Thread " << thread_id << ": run with cache" << std::endl;
        if (query_range != 0 ) {
            std::cout << "Thread " << thread_id << ": run range query with cache" << std::endl;
            run_range_query_test_client(server_sock, testindex, keys, payloads, number, test_size * (1 - upsert_ratio), batch_size, 100, true);
        }
            
        else {
            if (upsert_ratio != 1)
                run_search_test_client(server_sock, testindex, keys, payloads, number, test_size * (1 - upsert_ratio), batch_size, true);
            if (upsert_ratio != 0) {
                run_upsert_test_client(server_sock, testindex, keys, test_size * upsert_ratio, batch_size, true, key_distribution);
            }
        }
        
    } else if (index_type == "plin") {
        std::cout << "Thread " << thread_id << ": run without cache" << std::endl;
        if (query_range != 0 ) {
            std::cout << "Thread " << thread_id << ": run range query without cache" << std::endl;
            run_range_query_test_client(server_sock, testindex, keys, payloads, number, test_size * (1 - upsert_ratio), batch_size, 100, false);
        }
        else {
            if (upsert_ratio != 1)
                run_search_test_client(server_sock, testindex, keys, payloads, number, test_size * (1 - upsert_ratio), batch_size, false);
            if (upsert_ratio != 0) {
                run_upsert_test_client(server_sock, testindex, keys, test_size * upsert_ratio, batch_size, false, key_distribution);
            }
        }
        
    } else if (index_type == "btree") {
        std::cout << "Thread " << thread_id << ": run B Tree" << std::endl;
        if (query_range != 0) {
            std::cout << "Thread " << thread_id << ": run btree range query" << std::endl;
            run_range_query_test_benchmark(server_sock, testindex, keys, payloads, number, test_size * (1 - upsert_ratio), batch_size, 100, Client_message::BTree_RANGE);
        }
        else {
            if (upsert_ratio != 1)
                run_search_test_benchmark(server_sock, keys, payloads, number, test_size * (1 - upsert_ratio), batch_size, Client_message::BTree_lookup);
            if (upsert_ratio != 0) {
                run_upsert_test_benchmark(server_sock, keys, number, test_size * upsert_ratio, Client_message::BTree_upsert, key_distribution);
        }
    }
        
    } else if (index_type == "bplus") {
        std::cout << "Thread " << thread_id << ": run B Plus Tree" << std::endl;
        if (query_range != 0) {
            std::cout << "Thread " << thread_id << ": run btree range query" << std::endl;
            run_range_query_test_benchmark(server_sock, testindex, keys, payloads, number, test_size * (1 - upsert_ratio), batch_size, 100, Client_message::BPtree_RANGE);
        }
        else {
            run_search_test_benchmark(server_sock, keys, payloads, number, test_size * (1 - upsert_ratio), batch_size, Client_message::BPtree_lookup);
            if (upsert_ratio != 0) {
                run_upsert_test_benchmark(server_sock, keys, test_size * upsert_ratio, batch_size, Client_message::BPtree_upsert, key_distribution);
            }
        }
    } else if (index_type == "alex") {
        std::cout << "Thread " << thread_id << ": run ALEX" << std::endl;
        if (query_range != 0) {
            std::cout << "Thread " << thread_id << ": run alex range query" << std::endl;
            run_range_query_test_benchmark(server_sock, testindex, keys, payloads, number, test_size * (1 - upsert_ratio), batch_size, 100, Client_message::ALEX_RANGE);
        }
        else {
            run_search_test_benchmark(server_sock, keys, payloads, number, test_size * (1 - upsert_ratio), batch_size, Client_message::ALEX_lookup);
            if (upsert_ratio != 0) {
                run_upsert_test_benchmark(server_sock, keys, test_size * upsert_ratio, batch_size, Client_message::ALEX_upsert, key_distribution);
            }
        }
    }

    // auto end_time = std::chrono::steady_clock::now();
    // auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    // double throughput = static_cast<double>(test_size) / (total_time / 1000.0); // throughput per second

    // std::cout << "Thread " << thread_id << ": Total runtime: " << total_time << " milliseconds" << std::endl;
    // std::cout << "Thread " << thread_id << ": Throughput: " << throughput << " operations per second" << std::endl;

    close(server_sock);
    delete[] keys;
    delete[] payloads;
}



int main(int argc, char* argv[]) {
    SetCustomUsageMessage();
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    int port = FLAGS_port;
    size_t number = static_cast<size_t>(FLAGS_number);
    size_t test_size = static_cast<size_t>(FLAGS_test_size);
    std::string index_type = FLAGS_index_type;
    size_t batch_size = static_cast<size_t>(FLAGS_batch_size);
    std::string key_distribution = FLAGS_key_distribution;
    double upsert_ratio = FLAGS_upsert_ratio;
    int query_range = FLAGS_query_range;
    int num_threads = FLAGS_num_threads;

    auto start_time = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(run_test_client, i, port, number, test_size, index_type, batch_size, key_distribution, query_range, upsert_ratio);
    }

    
    for (auto& thread : threads) {
        thread.join();
    }
    auto end_time = std::chrono::steady_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    double throughput = static_cast<double>(test_size) * num_threads / (total_time / 1000.0); // throughput per second

    if (query_range != 0)
        throughput *= query_range;

    std::cout << "Total runtime: " << total_time << " milliseconds" << std::endl;
    std::cout << "Total Throughput: " << throughput << " operations per second" << std::endl;

    return 0;
}