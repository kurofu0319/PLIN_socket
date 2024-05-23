#include <random>
#include <chrono>
#include <iostream>
#include <vector>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>


#include "include/plin_index.h"
#include "include/message.h"

using TestIndex = PlinIndex;
// PMAllocator * galc;

void run_search_test(TestIndex& test_index, _key_t* keys, _payload_t* payloads, size_t number){
    std::mt19937 search_gen(123);
    std::uniform_int_distribution<size_t> search_dist(0, number - 1);
    int count = 0 , false_count = 0;
    
    for(size_t i = 0; i < 1e6; i++){
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
            std::cout << target_key << std::endl;
            
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
    std::normal_distribution<_key_t> key_dist(0, 1e10);
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

    for(size_t i = 0; i < upsert_times; i++){
        _payload_t answer;
        test_index.find(new_keys[i], answer);
        if(answer != new_payloads[i]){
            // std::cout<<"#Number: "<<i<<std::endl;
            // std::cout<<"#Key: "<<new_keys[i]<<std::endl;
            // std::cout<<"#Wrong answer: "<<answer<<std::endl;
            // std::cout<<"#Correct answer: "<<new_payloads[i]<<std::endl;
            // test_index.find(new_keys[i], answer);
            // throw std::logic_error("Answer wrong!");
            count ++ ;
        }
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

    // std::cout << "wrong count: " << count << std::endl;
    // std::cout << "false count: " << false_count << std::endl;

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

    // std::vector<std::pair<_key_t, _payload_t>> kv_pairs(number);

    // _key_t key = 0;
    // for (size_t i = 0; i < number; ++i) {
    //     kv_pairs[i] = std::make_pair(key_dist(gen), payload_dist(gen));
    // }
    // std::sort(kv_pairs.begin(), kv_pairs.end());
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
    std::string ipAddress = "127.0.0.1"; // 服务器的 IP 地址

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        std::cout << "Socket creation error" << std::endl;
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    // 将地址从文本转换为二进制形式
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


// int receivePayload(int sock, _payload_t& payload) {
//     char buffer[sizeof(_payload_t)]; // 创建一个足够存放_payload_t类型的缓冲区
//     ssize_t bytesReceived = recv(sock, buffer, sizeof(_payload_t), 0);
    
//     if (bytesReceived == -1) {
//         std::cerr << "Failed to receive payload" << std::endl;
//         return -1; // 接收失败
//     }
//     if (bytesReceived != sizeof(_payload_t)) {
//         std::cerr << "Received incomplete data" << std::endl;
//         return -1; // 数据未完全接收
//     }

//     payload = *reinterpret_cast<_payload_t*>(buffer); // 将接收到的字节流转换回_payload_t类型
//     return 0; // 接收成功
// }

// _payload_t GetPayloads(int server_sock, std::vector<int> leaf_path, _key_t key) {

//     _payload_t ans;

//     Client_message msg(leaf_path, key);

//     // std::cout << "get payload leaf_path: " << std::endl;
//     // for (int path : msg.leaf_path) {
//     //     std::cout << path ;
//     // }
//     // std::cout << std::endl;

//     sendClientMessage(server_sock, msg);

//     receivePayload(server_sock, ans);

//     return ans;
// }

void run_search_test_client(int sock, TestIndex& test_index, _key_t* keys, _payload_t* payloads, 
                            size_t number, size_t test_size, size_t batch_size){
    std::mt19937 search_gen(123);
    std::uniform_int_distribution<size_t> search_dist(0, number - 1);
    int count = 0 , false_count = 0;

    auto start = std::chrono::high_resolution_clock::now();
    
    for(size_t i = 0; i < test_size; i += batch_size){ 

        size_t percentage = test_size / 10;
        if (i % percentage == 0) 
            std::cout << i / percentage * 10 << " percentage finished." << std::endl;

        size_t real_batch_size = std::min(batch_size, test_size - i);
        std::vector<_key_t> batch_keys(real_batch_size);
        std::vector<_payload_t> batch_payloads(real_batch_size);
        std::vector<std::vector<int>> leaf_paths(real_batch_size);

        for (int j = 0; j < real_batch_size; ++ j) {
            size_t target_pos = search_dist(search_gen);
            batch_keys[j] = keys[target_pos];
            batch_payloads[j] = payloads[target_pos];

            test_index.find_Path(keys[target_pos], leaf_paths[j]);          
        }

        Client_message msg(leaf_paths, batch_keys, real_batch_size);
        sendClientMessage(sock, msg);

        std::vector<_payload_t> recv_payloads(real_batch_size);
        receivePayloads(sock, recv_payloads, real_batch_size);

        // std::cout << "receivePayloads " << std::endl;  
        
        for (int i = 0; i < batch_size; ++ i) {
            if (recv_payloads[i] != batch_payloads[i]) {
                std::cout << "Wrong payload!" << std::endl;
                std::cout << "true answer: " << batch_payloads[i] << " false: " << recv_payloads[i] << std::endl;
                false_count ++ ;
            }
                
        }

        // auto end = std::chrono::high_resolution_clock::now();
        // auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        // std::cout << "Duration time: " << duration.count() << " milliseconds" << std::endl;
    }
    std::cout << "false count: " << false_count << std::endl;

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Elapsed time: " << duration.count() << " milliseconds" << std::endl;
}

// _payload_t GetPayload_rawkey(int server_sock, _key_t key) {
//     Client_message msg(key);
//     _payload_t ans;

//     sendClientMessage(server_sock, msg);

//     receivePayload(server_sock, ans);

//     return ans;

// }

void run_search_test_client_nocache(int sock, TestIndex& test_index, _key_t* keys, _payload_t* payloads, 
                                    size_t number, size_t test_size, size_t batch_size){
        std::mt19937 search_gen(123);
    std::uniform_int_distribution<size_t> search_dist(0, number - 1);
    int count = 0 , false_count = 0;

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

            // test_index.find_Path(keys[target_pos], leaf_paths[j]);          
        }

        Client_message msg(batch_keys, real_batch_size);
        sendClientMessage(sock, msg);

        std::vector<_payload_t> recv_payloads(real_batch_size);
        receivePayloads(sock, recv_payloads, real_batch_size);

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
        run_search_test_client(server_sock, testindex, keys, payloads, number, test_size, batch_size);
    }
    else if (mode == 1) {
        std::cout << "run without cache" << std::endl;
        run_search_test_client_nocache(server_sock, testindex, keys, payloads, number, test_size, batch_size);
    }
    // else if (mode == 3) {
    //     std::cout << "run upsert with cache" << std::endl;
    //     _key_t* new_keys = nullptr;
    //     _payload_t* new_payloads = nullptr;
    //     run_upsert_test_client(server_sock, testindex, new_keys, new_payloads, number, test_size);
        
    // }

    close(server_sock);
}
