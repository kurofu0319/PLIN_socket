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



void run_search_test_client(int sock, TestIndex& test_index, _key_t* keys, _payload_t* payloads, size_t number){
    std::mt19937 search_gen(123);
    std::uniform_int_distribution<size_t> search_dist(0, number - 1);
    int count = 0 , false_count = 0;
    std::vector<int> leaf_path;
    
    for(size_t i = 0; i < 1e6; i++){
        // size_t target_pos = search_dist(search_gen);

        // size_t target_pos = i;
        
        // _key_t target_key = keys[target_pos];
        _key_t target_key = keys[search_dist(search_gen)];
        _payload_t answer;

        leaf_path.clear();

        test_index.find_Path(target_key, leaf_path);

        std::vector<char> buffer;
        size_t bufferSize = leaf_path.size() * sizeof(int);
        buffer.resize(bufferSize);
        std::memcpy(buffer.data(), leaf_path.data(), bufferSize);
        sendSerializedData(buffer, sock);


        // for (const auto& num : leaf_path) 
        //     std::cout << num << " ";
        // std::cout << std::endl << std::endl;


        // for (int num : leaf_path) {
        //     std::cout << num << " ";
        // }
        // std::cout << std::endl;

        // std::string message = std::to_string(target_key);

        // if (send(sock, message.c_str(), message.length(), 0) == -1) {
        //     std::cerr << "Error sending data\n";
        //     close(sock);
        //     return ;
        // }
        
        // bool ans = test_index.find(target_key, answer);

        // std::cout << answer << std::endl;
        // std::cout << payloads[target_pos] << std::endl << std::endl;
        
        // if (ans == false) 
        // {
        //     std::cout << target_key << std::endl;
            
        //     false_count ++ ;
        // }
        // if(answer != payloads[i]){
        //     count ++ ;
            
            // std::cout<<"#Number: "<<i<<std::endl;
            // std::cout<<"#Wrong answer: "<<answer<<std::endl;
            // std::cout<<"#Correct answer: "<<payloads[target_pos]<<std::endl;
            // test_index.find(target_key, answer);
            // throw std::logic_error("Answer wrong!");
        }
    }
    // std::cout << "wrong count: " << count << std::endl;
    // std::cout << "false count: " << false_count << std::endl;

void initialize_data(size_t number, _key_t*& keys, _payload_t*& payloads) {
    std::mt19937 gen(456); 
    std::uniform_real_distribution<double> dist(10.0, 100.0);

    keys = new _key_t[number];
    payloads = new _payload_t[number];

    _key_t key = 0;
    for (int i = 0; i < number; i++) {
        key += dist(gen);
        keys[i] = key;
        payloads[i] = key;
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

    Client_message msg;
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




int main() 
{

    size_t number = 1e7;
    // std::mt19937 gen(456); 
    // std::uniform_real_distribution<double> dist(10.0, 100.0);

    _key_t* keys = new _key_t[number];      // 动态分配数组存储keys
    _payload_t* payloads = new _payload_t[number];

    // _key_t key = 0;
    // _payload_t payload = 0;
    // for (int i = 0; i < number; i ++ )
    // {
    //     key += dist(gen);
    //     keys[i] = key;
    //     payloads[i] = key;
    // }

    initialize_data(number, keys, payloads);

    int server_sock = start_client(8080);

    // get PLIN cache

    // META

    TestIndex testindex = GetMeta(server_sock);
    



    testindex.PrintInfo();

    while(true) {}

    // run_search_test_client(sock, testindex, keys, payloads, number);

    close(server_sock);
}
