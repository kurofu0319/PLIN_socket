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
#include "include/Safe_queue.h"
#include "include/message.h"
#include "include/btree/map.h"
#include "include/alex/alex.h"
#include "include/stx-btree/src/btree_map.h"
#include <gflags/gflags.h>

using BTreeMap = btree::map<_key_t, _payload_t>;
using TestIndex = PlinIndex;
using AlexIndex = alex::Alex<_key_t, _payload_t>;
using BPlustree = stx::btree_map<_key_t, _payload_t>;

DEFINE_int32(port, 8080, "Port number");
DEFINE_double(number, 1e7, "Number of keys");
DEFINE_string(key_distribution, "lognormal", "Key distribution (lognormal, exponential, uniform, normal)");
DEFINE_string(index_type, "plin", "Index type (plin, alex, bplus, btree)");

//alex range query
std::vector<std::pair<_key_t, _payload_t>> range_query_alex(AlexIndex& index, const _key_t& low, const _key_t& high) {
    std::vector<std::pair<_key_t, _payload_t>> results;
    auto it_low = index.lower_bound(low);
    auto it_high = index.upper_bound(high);

    while (it_low != it_high) {
        results.push_back(*it_low);
        ++it_low;
    }

    return results;
}


// BTreeMap range query
std::vector<std::pair<_key_t, _payload_t>> range_query_btree(BTreeMap& btree, const _key_t& low, const _key_t& high) {
    std::vector<std::pair<_key_t, _payload_t>> results;
    auto it_low = btree.lower_bound(low);
    auto it_high = btree.upper_bound(high);

    while (it_low != it_high) {
        results.push_back(*it_low);
        ++it_low;
    }

    return results;
}

// BPlustree range query
std::vector<std::pair<_key_t, _payload_t>> range_query_bplus(BPlustree& bptree, const _key_t& low, const _key_t& high) {
    std::vector<std::pair<_key_t, _payload_t>> results;
    auto it_low = bptree.lower_bound(low);
    auto it_high = bptree.upper_bound(high);

    while (it_low != it_high) {
        results.push_back(*it_low);
        ++it_low;
    }

    return results;
}

void SetCustomUsageMessage() {
    gflags::SetUsageMessage(
        "Usage: ./your_program [options]\n"
        "Options:\n"
        "  --port=<port>           Port number (default: 8080)\n"
        "  --number=<number>       Number of keys (default: 1e7)\n"
        "  --key_distribution=<distribution> Key distribution (lognormal, exponential, uniform, normal, zipf) (default: lognormal)\n"
        "  --index_type=<index> Index type (plin, alex, bplus, btree) (default: plin)"
        "\n"
    );
}

void send_insert_Confirmation(int client_socket, size_t batch_size) {
    std::string confirmation = "Confirmation: " + std::to_string(batch_size) + " items inserted successfully.";
    send(client_socket, confirmation.c_str(), confirmation.size(), 0);
}

void insert_handler(SafeQueue<std::pair<Client_message, int>>& task_queue, BTreeMap& btree, BPlustree& bptree, AlexIndex& alex_index) {
    while (true) {
        std::pair<Client_message, int> task = task_queue.dequeue();
        Client_message message = task.first;
        int client_socket = task.second;
        if (message.type == Client_message::ALEX_upsert) {
            for (size_t i = 0; i < message.batch_size; ++i) {
                alex_index.insert(message.keys[i], message.payloads[i]);
            }
        }
        else if (message.type == Client_message::BTree_upsert) {
            for (size_t i = 0; i < message.batch_size; ++i) 
                btree.insert(std::make_pair(message.keys[i], message.payloads[i]));
        }
        else if (message.type == Client_message::BPtree_upsert) {
            for (size_t i = 0; i < message.batch_size; ++i) 
                bptree.insert(std::make_pair(message.keys[i], message.payloads[i]));
        }
        send_insert_Confirmation(client_socket, message.batch_size);
    }
}


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
            
        }
    }
    std::cout << "wrong count: " << count << std::endl;
    std::cout << "false count: " << false_count << std::endl;
}

// void run_range_query_test(TestIndex& test_index, _key_t* keys, _payload_t* payloads, size_t number){
//     std::mt19937 search_gen(123);
//     std::uniform_int_distribution<size_t> search_dist(0, number - 100);
//     int count = 0 , false_count = 0;
    
//     for(size_t i = 0; i < number; i++){

//         size_t target_pos = search_dist(search_gen);
        
//         _key_t lower_key = keys[target_pos];
//         _key_t upper_key = keys[target_pos + 10];
//         std::vector<std::pair<_key_t, _payload_t>> answers;
        
//         test_index.range_query(lower_key, upper_key, answers);

//     }
//     // std::cout << "wrong count: " << count << std::endl;
//     std::cout << "range query test done" << std::endl;
// }

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

void initialize_btree(BTreeMap& btree, _key_t* keys, _payload_t* payloads, size_t number) {
    for (size_t i = 0; i < number; ++i) {
        btree.insert(std::make_pair(keys[i], payloads[i]));
    }
}

void initialize_bptree(BPlustree& bptree, _key_t* keys, _payload_t* payloads, size_t number) {
    for (size_t i = 0; i < number; ++i) {
        bptree.insert(std::make_pair(keys[i], payloads[i]));
    }
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

void initialize_data(size_t number, _key_t*& keys, _payload_t*& payloads, std::pair<_key_t, _payload_t> *kv_pairs, const std::string& distribution) {
    std::mt19937 key_gen(456); 
    std::mt19937 payload_gen(456); 

    std::uniform_int_distribution<_payload_t> payload_dist(0,1e9);

    keys = new _key_t[number];
    payloads = new _payload_t[number];

    // std::vector<std::pair<_key_t, _payload_t>> kv_pairs(number);

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
    
    // std::sort(kv_pairs.begin(), kv_pairs.end());

    std::sort(kv_pairs, kv_pairs + number, [](const std::pair<_key_t, _payload_t>& a, const std::pair<_key_t, _payload_t>& b) {
        return a.first < b.first;
    });

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

int sendResults(int sock, const std::vector<std::vector<std::pair<_key_t, _payload_t>>>& results) {
    // 计算所有数据的总大小
    size_t keyPayloadSize = sizeof(_key_t) + sizeof(_payload_t);
    size_t totalPayloadSize = 0;

    for (const auto& vec : results) {
        totalPayloadSize += sizeof(uint32_t); // 存储每个子vector的大小
        totalPayloadSize += vec.size() * keyPayloadSize; // 存储每个子vector的内容
    }

    size_t totalSize = totalPayloadSize + sizeof(uint32_t); // 总大小包括results的大小

    // 创建一个足够大的缓冲区来存储大小信息和所有有效载荷
    std::vector<char> buffer(totalSize);
    char* bufferPtr = buffer.data();

    // 先将总数据大小放入缓冲区
    uint32_t totalSizeNetwork = htonl(static_cast<uint32_t>(totalPayloadSize));
    std::memcpy(bufferPtr, &totalSizeNetwork, sizeof(totalSizeNetwork));
    bufferPtr += sizeof(totalSizeNetwork);

    // 将所有有效载荷复制到缓冲区中
    for (const auto& vec : results) {
        uint32_t vecSizeNetwork = htonl(static_cast<uint32_t>(vec.size()));
        std::memcpy(bufferPtr, &vecSizeNetwork, sizeof(vecSizeNetwork));
        bufferPtr += sizeof(vecSizeNetwork);

        for (const auto& pair : vec) {
            std::memcpy(bufferPtr, &pair.first, sizeof(_key_t));
            bufferPtr += sizeof(_key_t);
            std::memcpy(bufferPtr, &pair.second, sizeof(_payload_t));
            bufferPtr += sizeof(_payload_t);
        }
    }

    // 发送整个缓冲区内容
    ssize_t bytesSent = send(sock, buffer.data(), totalSize, 0);
    if (bytesSent == -1) {
        std::cerr << "Failed to send results" << std::endl;
        return -1; // 发送失败
    }

    return 0; // 发送成功
}




void handle_client(TestIndex& testIndex, int client_socket, std::vector<char>& buffer, BTreeMap &btree, BPlustree &bptree, AlexIndex &alex_index, SafeQueue<std::pair<Client_message, int>>& task_queue) {

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
                
                payload = testIndex.find_Payload(receivedMsg.keys[i], receivedMsg.leaf_paths[i]);

                payloads.push_back(payload);
            }

            sendPayloads(client_socket, payloads);
        }
        else if (receivedMsg.type == Client_message::RANGE) {
            
            std::vector<std::vector<std::pair<_key_t, _payload_t>>> results;

            for (size_t i = 0; i < receivedMsg.batch_size; ++i) {
                std::vector<std::pair<_key_t, _payload_t>> answers;
                
                testIndex.range_query_Path(receivedMsg.keys[i], receivedMsg.upper_keys[i], answers, receivedMsg.leaf_paths[i]);
                results.push_back(answers);
            }
            
            sendResults(client_socket, results);
        }
        else if (receivedMsg.type == Client_message::RAW_RANGE) {
            
            std::vector<std::vector<std::pair<_key_t, _payload_t>>> results;

            for (size_t i = 0; i < receivedMsg.batch_size; ++i) {
                std::vector<std::pair<_key_t, _payload_t>> answers;
                
                testIndex.range_query(receivedMsg.keys[i], receivedMsg.upper_keys[i], answers);
                results.push_back(answers);
            }
            
            sendResults(client_socket, results);
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
                if (receivedMsg.leaf_paths[i] == -1) {
                    // std::cout << "normal upsert" << std::endl;
                    testIndex.upsert(receivedMsg.keys[i], receivedMsg.payloads[i]);
                }
                    
                else
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
        else if (receivedMsg.type == Client_message::BTree_lookup) {
            std::vector<_payload_t> payloads;
            for (size_t i = 0; i < receivedMsg.batch_size; ++i) {
                _payload_t payload;
                auto it = btree.find(receivedMsg.keys[i]);
                if (it != btree.end()) {
                    payloads.push_back(it->second);
                }
            }
            sendPayloads(client_socket, payloads);
        }
        else if (receivedMsg.type == Client_message::BTree_upsert) {
            task_queue.enqueue(std::make_pair(receivedMsg, client_socket));
            // for (size_t i = 0; i < receivedMsg.batch_size; ++i) {
            //     btree.insert(std::make_pair(receivedMsg.keys[i], receivedMsg.payloads[i]));
            // }
            // send_insert_Confirmation(client_socket, receivedMsg.batch_size);
        }
        else if (receivedMsg.type == Client_message::BTree_RANGE) {
            
            std::vector<std::vector<std::pair<_key_t, _payload_t>>> results;

            for (size_t i = 0; i < receivedMsg.batch_size; ++i) {
                std::vector<std::pair<_key_t, _payload_t>> answers;
                
                answers = range_query_btree(btree, receivedMsg.keys[i], receivedMsg.upper_keys[i]);
                results.push_back(answers);
            }
            
            sendResults(client_socket, results);
        }
        else if (receivedMsg.type == Client_message::BPtree_lookup) {
            std::vector<_payload_t> payloads;
            for (size_t i = 0; i < receivedMsg.batch_size; ++i) {
                _payload_t payload;
                auto it = bptree.find(receivedMsg.keys[i]);
                if (it != bptree.end()) {
                    payloads.push_back(it->second);
                }
            }
            sendPayloads(client_socket, payloads);
        }
        else if (receivedMsg.type == Client_message::BPtree_upsert) {
             task_queue.enqueue(std::make_pair(receivedMsg, client_socket));
            // for (size_t i = 0; i < receivedMsg.batch_size; ++i) {
            //     bptree.insert(receivedMsg.keys[i], receivedMsg.payloads[i]);
            // }
            // send_insert_Confirmation(client_socket, receivedMsg.batch_size);
        }
        else if (receivedMsg.type == Client_message::BPtree_RANGE) {
            
            std::vector<std::vector<std::pair<_key_t, _payload_t>>> results;

            for (size_t i = 0; i < receivedMsg.batch_size; ++i) {
                std::vector<std::pair<_key_t, _payload_t>> answers;
                
                answers = range_query_bplus(bptree, receivedMsg.keys[i], receivedMsg.upper_keys[i]);
                results.push_back(answers);
            }
            
            sendResults(client_socket, results);
        }
        else if (receivedMsg.type == Client_message::ALEX_lookup) {
            std::vector<_payload_t> payloads;
            for (size_t i = 0; i < receivedMsg.batch_size; ++i) {
                _payload_t* payload = alex_index.get_payload(receivedMsg.keys[i]);
                payloads.push_back(*payload);

            }
            sendPayloads(client_socket, payloads);
        }
        else if (receivedMsg.type == Client_message::ALEX_upsert) {
             task_queue.enqueue(std::make_pair(receivedMsg, client_socket));
            // for (size_t i = 0; i < receivedMsg.batch_size; ++i) {
            //     alex_index.insert(receivedMsg.keys[i], receivedMsg.payloads[i]);
            // }
            // send_insert_Confirmation(client_socket, receivedMsg.batch_size);
        }
        else if (receivedMsg.type == Client_message::ALEX_RANGE) {
            
            std::vector<std::vector<std::pair<_key_t, _payload_t>>> results;

            for (size_t i = 0; i < receivedMsg.batch_size; ++i) {
                std::vector<std::pair<_key_t, _payload_t>> answers;
                
                answers = range_query_alex(alex_index, receivedMsg.keys[i], receivedMsg.upper_keys[i]);
                results.push_back(answers);
            }
            
            sendResults(client_socket, results);
        }
        else if (receivedMsg.type == Client_message::INVALID) {
            break;
        }
    }

    close(client_socket);
}

void start_server(int port, std::vector<char>& buffer, TestIndex& testIndex, BTreeMap &btree, BPlustree &bptree, AlexIndex &alex_index, SafeQueue<std::pair<Client_message, int>> &task_queue) {
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

        

        std::thread(handle_client, std::ref(testIndex), client_socket, std::ref(buffer), std::ref(btree), std::ref(bptree), 
                                                                                                    std::ref(alex_index), std::ref(task_queue)).detach();
        
    }
    close(server_fd);
}


int main(int argc, char* argv[]) {

    SetCustomUsageMessage();
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    
    int port = FLAGS_port;
    size_t number = static_cast<size_t>(FLAGS_number);
    std::string key_distribution = FLAGS_key_distribution;
    std::string index_type = FLAGS_index_type;

    if (argc == 3) {
        port = std::stoi(argv[1]); 
        number = static_cast<size_t>(std::stod(argv[2]));
    }

    std::cout << "Port: " << port << " number: " << number << std::endl;

    _key_t* keys = nullptr;
    _payload_t* payloads = nullptr;

    auto values = new std::pair<_key_t, _payload_t>[number];

    initialize_data(number, keys, payloads, values, key_distribution);

    auto start_time = std::chrono::steady_clock::now();

    TestIndex test_index;
    BTreeMap btree;
    BPlustree bptree;
    AlexIndex alex_index;

    if (index_type == "plin") {
        test_index.bulk_load(keys, payloads, number);
    }

    else if (index_type == "alex") {
        alex_index.bulk_load(values, number);
    }
    else if (index_type == "bplus")
        initialize_bptree(bptree, keys, payloads, number);
    else if (index_type == "btree")
        initialize_btree(btree, keys, payloads, number);
    else
        throw std::invalid_argument("Unsupported index type");

        // auto start_time = std::chrono::steady_clock::now();
    
        // test_index.bulk_load(keys, payloads, number);

        // auto end_time = std::chrono::steady_clock::now();
        // auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

        // std::cout << "Plin bulk load time: " << total_time << " milliseconds" << std::endl;

        // start_time = std::chrono::steady_clock::now();

        // alex_index.bulk_load(values, number);

        // end_time = std::chrono::steady_clock::now();
        // total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

        // std::cout << "Alex bulk load time: " << total_time << " milliseconds" << std::endl;

        // start_time = std::chrono::steady_clock::now();

        // initialize_bptree(bptree, keys, payloads, number);

        // end_time = std::chrono::steady_clock::now();
        // total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

        // std::cout << "btree bulk load time: " << total_time << " milliseconds" << std::endl;

        // start_time = std::chrono::steady_clock::now();

        // initialize_btree(btree, keys, payloads, number);

        // end_time = std::chrono::steady_clock::now();
        // total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

        // std::cout << "bplus tree bulk load time: " << total_time << " milliseconds" << std::endl;

    auto end_time = std::chrono::steady_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    std::cout << "Bulk load time: " << total_time << " milliseconds" << std::endl;
  
    std::cout << "init sucessfully" << std::endl;

    std::vector<char> buffer;
    test_index.serializePlinIndex(buffer);
    prependSizeToBuffer(buffer);

    SafeQueue<std::pair<Client_message, int>> task_queue;
    std::thread insert_thread(insert_handler, std::ref(task_queue), std::ref(btree), std::ref(bptree), std::ref(alex_index));


    start_server(port, buffer, test_index, btree, bptree, alex_index, task_queue);

    insert_thread.join();
    return 0;
}

