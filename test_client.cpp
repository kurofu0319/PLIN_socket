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


#include "include/plin_index.h"
#include "include/message.h"
#include "include/TaskScheduler.h"

using TestIndex = PlinIndex;

void initialize_data(size_t number, _key_t*& keys, _payload_t*& payloads) {
    std::mt19937 key_gen(456); 
    std::mt19937 payload_gen(456); 
    // std::uniform_real_distribution<double> dist(10.0, 100.0);

    std::normal_distribution<_key_t> key_dist(0, 1e9);
    std::uniform_int_distribution<_payload_t> payload_dist(0,1e9);

    keys = new _key_t[number];
    payloads = new _payload_t[number];

    for (size_t i = 0; i < number; ++i) {
        keys[i] = key_dist(key_gen);
        payloads[i] = payload_dist(payload_gen);
    }
    std::cout << "prepared" << std::endl;
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

    std::cout << "Data size: " << dataSize << std::endl;

    std::vector<char> receivedData(dataSize);

    // 现在接收实际的数据
    char* bufPtr = receivedData.data();
    uint32_t bytesToReceive = dataSize;
    while (bytesToReceive > 0) {
        int bytesReceived = recv(server_sock, bufPtr, bytesToReceive, 0);
        if (bytesReceived <= 0) {
            // 接收失败或连接关闭
            std::cerr << "Failed to receive data or connection closed" << std::endl;
            break;
        }
        bytesToReceive -= bytesReceived;
        bufPtr += bytesReceived;
    }

    std::cout << "Received data size: " << receivedData.size() << " bytes" << std::endl;
        
    
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

void lookup_Preparation(TaskScheduler& scheduler, PlinIndex& test_index, _key_t* keys, _payload_t* payloads, 
                           size_t number, size_t test_size, size_t batch_size, bool cache) {
    std::mt19937 gen(123);
    std::uniform_int_distribution<size_t> dist(0, number - 1);
    uint32_t level = test_index.get_level();

    // std::cout << "level: " << level << std::endl;
    // std::cout << "test_size: " << test_size << std::endl;
    // std::cout << "batch_size: " << batch_size << std::endl;

    std::vector<_key_t> batch_keys;
    std::vector<_payload_t> batch_payloads;
    std::vector<int> leaf_paths;
    std::string message;

    for (size_t i = 0; i < test_size; i += batch_size) {
        // std::cout << "batch_size: " << i << std::endl;
        size_t real_batch_size = std::min(batch_size, test_size - i);
        batch_keys.clear();
        batch_payloads.clear();
        

        
        if (cache == true) {
            leaf_paths.clear();
            for (size_t j = 0; j < real_batch_size; ++j) {
                size_t target_pos = dist(gen);
                batch_keys.push_back(keys[target_pos]);
                batch_payloads.push_back(payloads[target_pos]);
                test_index.find_Path(batch_keys[j], leaf_paths); 
            }
        }
        else {
            for (size_t j = 0; j < real_batch_size; ++j) {
                size_t target_pos = dist(gen);
                batch_keys.push_back(keys[target_pos]);
                batch_payloads.push_back(payloads[target_pos]);
            }

            // std::cout << "generate kv" << std::endl;
            
        }
        
        
        

        if (cache == true) {
            Client_message msg(leaf_paths, batch_keys, real_batch_size, level);
            message = msg.serialize();
        }
            
        else {
            Client_message msg(batch_keys, real_batch_size);
            message = msg.serialize();
            // std::cout << "serialized" << std::endl;
        }
    
        auto data = std::make_tuple(message, real_batch_size, batch_payloads);

        // std::cout << "adding Task: " << i << std::endl;
        
        scheduler.addTask(std::move(data));

        // std::cout << "add Task: " << i << std::endl;
        
    }
}

void lookup_sendMessage(int sock, TaskScheduler& scheduler, size_t test_size, size_t batch_size) {
    int false_count = 0;
    
    std::chrono::milliseconds receive_duration(0);

    size_t processed = 0;
    while (processed < test_size) {
        auto [message, real_batch_size, batch_payloads] = scheduler.getTask();
        // std::cout << "get Task" << std::endl;

        auto findPath_start = std::chrono::high_resolution_clock::now();
        sendClientMessage(sock, message);
        std::vector<_payload_t> recv_payloads(real_batch_size);
        

        receivePayloads(sock, recv_payloads, real_batch_size);
        auto findPath_end = std::chrono::high_resolution_clock::now();
        receive_duration += std::chrono::duration_cast<std::chrono::milliseconds>(findPath_end - findPath_start);
        
        

        for (size_t j = 0; j < real_batch_size; ++j) {
            if (recv_payloads[j] != batch_payloads[j]) {
                std::cout << "Wrong payload!" << std::endl;
                false_count++;
            }
        }
        processed += real_batch_size;
    }

    std::cout << "receive time: " << receive_duration.count() << " milliseconds" << std::endl;
}

void run_search_test_client(int sock, TestIndex& test_index, _key_t* keys, _payload_t* payloads, 
                            size_t number, size_t test_size, size_t batch_size, bool cache) {

    TaskScheduler scheduler;
    auto start = std::chrono::high_resolution_clock::now();

    std::thread prepThread(lookup_Preparation, std::ref(scheduler), std::ref(test_index), keys, payloads, 
                            number, test_size, batch_size, cache);
    std::thread commThread(lookup_sendMessage, sock, std::ref(scheduler), test_size, batch_size);

    prepThread.join();
    commThread.join();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Total duration time: " << duration.count() << " milliseconds" << std::endl;
}

void run_search_test_client_nocache(int sock, TestIndex& test_index, _key_t* keys, _payload_t* payloads, 
                                    size_t number, size_t test_size, size_t batch_size){
        std::mt19937 search_gen(123);
    std::uniform_int_distribution<size_t> search_dist(0, number - 1);
    int count = 0 , false_count = 0;
    std::chrono::milliseconds no_findPath_duration(0);

    auto start = std::chrono::high_resolution_clock::now();
    
    for(size_t i = 0; i < test_size; i += batch_size){ 

        size_t percentage = test_size / 10;
        if (i % percentage == 0) 
            std::cout << i / percentage * 10 << " percentage finished." << std::endl;

        size_t real_batch_size = std::min(batch_size, test_size - i);
        std::vector<_key_t> batch_keys(real_batch_size);
        std::vector<_payload_t> batch_payloads(real_batch_size);
        // std::vector<std::vector<int>> leaf_paths(real_batch_size);

        

        for (int j = 0; j < real_batch_size; ++ j) {
            size_t target_pos = search_dist(search_gen);
            batch_keys[j] = keys[target_pos];
            batch_payloads[j] = payloads[target_pos];         
        }

        Client_message msg(batch_keys, real_batch_size);
        auto findPath_start = std::chrono::high_resolution_clock::now();
        sendClientMessage(sock, msg);
        
        

        std::vector<_payload_t> recv_payloads(real_batch_size);
        
        
        receivePayloads(sock, recv_payloads, real_batch_size);
        auto findPath_end = std::chrono::high_resolution_clock::now();
        no_findPath_duration += std::chrono::duration_cast<std::chrono::milliseconds>(findPath_end - findPath_start);

        // std::cout << "receivePayloads " << std::endl;  
        
        for (int i = 0; i < batch_size; ++ i) {
            if (recv_payloads[i] != batch_payloads[i]) {
                std::cout << "Wrong payload!" << std::endl;
                std::cout << "true answer: " << batch_payloads[i] << " false: " << recv_payloads[i] << std::endl;
                false_count ++ ;
            }
                
        }
    }
    std::cout << "false count: " << false_count << std::endl;

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Elapsed time: " << duration.count() << " milliseconds" << std::endl;
    std::cout << "No findPath receive time: " << no_findPath_duration.count() << " milliseconds" << std::endl;



    // std::mt19937 search_gen(123);
    // std::uniform_int_distribution<size_t> search_dist(0, number - 1);
    // int count = 0 , false_count = 0;
    // // std::vector<int> leaf_path;

    // auto start = std::chrono::high_resolution_clock::now();
    
    // for(size_t i = 0; i < test_size; i++){

    //     if (i % 10000 == 0) { // 每10000次迭代输出一次进度
            
    //         auto end = std::chrono::high_resolution_clock::now();
    //         auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    //         std::cout << "Duration time: " << duration.count() << " milliseconds" << std::endl;
    //     }

    //     size_t target_pos = search_dist(search_gen);
    //     _key_t target_key = keys[target_pos];
    //     _payload_t answer;

    //     // leaf_path.clear();

    //     // test_index.find_Path(target_key, leaf_path);

    //     // answer = GetPayload(sock, leaf_path, target_key);

    //     answer = GetPayload_rawkey(sock, target_key);

    //     if (answer != payloads[target_pos]) {
    //         // std::cout << "False!" << std::endl;
    //         // std::cout << "true answer: " << payloads[target_pos] << " false: " << answer << std::endl;
    //         false_count ++ ;
    //     }
    // }
    // std::cout << "false count: " << false_count << std::endl;

    // auto end = std::chrono::high_resolution_clock::now();
    // auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // std::cout << "Elapsed time: " << duration.count() << " milliseconds" << std::endl;
}

// void run_upsert_test_client(int server_sock, TestIndex& test_index, _key_t* &new_keys, _payload_t* &new_payloads, size_t number, size_t upsert_times) {
//     std::normal_distribution<_key_t> key_dist(0, 1e9);
//     std::uniform_int_distribution<_payload_t> payload_dist(0,1e9);
//     std::mt19937 key_gen(456);
//     std::mt19937 payload_gen(456);

//     // size_t upsert_times = 1e7;
//     new_keys = new _key_t[upsert_times];
//     new_payloads = new _payload_t[upsert_times];
//     std::vector<int> leaf_path;

//     auto start = std::chrono::high_resolution_clock::now();

//     for(size_t i = 0; i < upsert_times; i++){

//         if (i % 10000 == 0) { // 每10000次迭代输出一次进度
            
//             auto end = std::chrono::high_resolution_clock::now();
//             auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

//             std::cout << "Duration time: " << duration.count() << " milliseconds" << std::endl;
//         }

//         new_keys[i] = key_dist(key_gen);
//         new_payloads[i] = payload_dist(payload_gen);

        

//         leaf_path.clear();

//         test_index.find_Path(new_keys[i], leaf_path);

        

//         Client_message msg(new_keys[i], new_payloads[i], leaf_path);
//         // std::cout << "send key: " << new_keys[i] << std::endl;
//         // std::cout << "send payload: " << new_payloads[i] << std::endl;

//         // std::cout << "sendClientMessage" << std::endl;

//         sendClientMessage(server_sock, msg);
        
//         _payload_t ans;
//         receivePayload(server_sock, ans);

//     }

//     auto end = std::chrono::high_resolution_clock::now();
//     auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

//     std::cout << "Elapsed time: " << duration.count() << " milliseconds" << std::endl;

//     std::cout << "Justify upsert: " << std::endl;

//     run_search_test_client_nocache(server_sock, test_index, new_keys, new_payloads, upsert_times, upsert_times);

//     // std::mt19937 search_gen(123);
//     // std::uniform_int_distribution<size_t> search_dist(0, number - 1);
//     // int count = 0 , false_count = 0;
//     // // std::vector<int> leaf_path;

//     // start = std::chrono::high_resolution_clock::now();
    
//     // for(size_t i = 0; i < upsert_times; i++){

//     //     if (i % 10000 == 0) { // 每10000次迭代输出一次进度
            
//     //         auto end = std::chrono::high_resolution_clock::now();
//     //         auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

//     //         std::cout << "Duration time: " << duration.count() << " milliseconds" << std::endl;
//     //     }

//     //     size_t target_pos = search_dist(search_gen);
//     //     _key_t target_key = new_keys[target_pos];
//     //     _payload_t answer;

//     //     // leaf_path.clear();

//     //     // test_index.find_Path(target_key, leaf_path);

//     //     // answer = GetPayload(sock, leaf_path, target_key);

//     //     answer = GetPayload_rawkey(server_sock, target_key);

//     //     if (answer != new_payloads[target_pos]) {
//     //         // std::cout << "False!" << std::endl;
//     //         // std::cout << "true answer: " << payloads[target_pos] << " false: " << answer << std::endl;
//     //         false_count ++ ;
//     //     }
//     // }
//     // std::cout << "false count: " << false_count << std::endl;

//     // end = std::chrono::high_resolution_clock::now();
//     // duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

//     // std::cout << "Elapsed time: " << duration.count() << " milliseconds" << std::endl;
        
//         // testIndex.upsert(new_keys[i], new_payloads[i]);


    

//     // for(size_t i = 0; i < upsert_times; i++){
//     //     _payload_t answer;
//     //     testIndex.find(new_keys[i], answer);
//     //     if(answer != new_payloads[i]){
//     //         // std::cout<<"#Number: "<<i<<std::endl;
//     //         // std::cout<<"#Key: "<<new_keys[i]<<std::endl;
//     //         // std::cout<<"#Wrong answer: "<<answer<<std::endl;
//     //         // std::cout<<"#Correct answer: "<<new_payloads[i]<<std::endl;
//     //         // test_index.find(new_keys[i], answer);
//     //         // throw std::logic_error("Answer wrong!");
//     //         count ++ ;
//     //     }
//     // }
// }


int main(int argc, char* argv[]) {

    int port = 8080;
    size_t number = 1e7;
    size_t test_size = 1e6;
    int mode = 0;   // run_search_test
    size_t batch_size = 1000;

    if (argc > 1) {
        port = std::stoi(argv[1]);
    }
    if (argc > 2) {
        number = static_cast<size_t>(std::stod(argv[2]));
    }
    if (argc > 3) {
        test_size = static_cast<size_t>(std::stod(argv[3]));
    }
    if (argc > 4) {
        mode = std::stoi(argv[4]);
    }
    if (argc > 5) {
        batch_size = static_cast<size_t>(std::stod(argv[5]));
    }

    std::cout << "Port: " << port << " number: " << number << std::endl;

    // std::mt19937 gen(456); 
    // std::uniform_real_distribution<double> dist(10.0, 100.0);

    _key_t* keys = nullptr;      // 动态分配数组存储keys
    _payload_t* payloads = nullptr;

    // _key_t key = 0;
    // _payload_t payload = 0;
    // for (int i = 0; i < number; i ++ )
    // {
    //     key += dist(gen);
    //     keys[i] = key;
    //     payloads[i] = key;
    // }

    initialize_data(number, keys, payloads);

    int server_sock = start_client(port);

    // get PLIN cache
    TestIndex testindex = GetMeta(server_sock);
    
    testindex.PrintInfo();

    // while(true) {}

    if (mode == 0) {
        std::cout << "run with cache" << std::endl;
        run_search_test_client(server_sock, testindex, keys, payloads, number, test_size, batch_size, true);
    }
    else if (mode == 1) {
        std::cout << "run without cache" << std::endl;
        run_search_test_client(server_sock, testindex, keys, payloads, number, test_size, batch_size, false);
        // run_search_test_client_nocache(server_sock, testindex, keys, payloads, number, test_size, batch_size);
    }
    // else if (mode == 3) {
    //     std::cout << "run upsert with cache" << std::endl;
    //     _key_t* new_keys = nullptr;
    //     _payload_t* new_payloads = nullptr;
    //     run_upsert_test_client(server_sock, testindex, new_keys, new_payloads, number, test_size);
        
    // }

    close(server_sock);
}
