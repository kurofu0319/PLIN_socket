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

#include "include/plin_index.h"

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

void test(uint64_t thread_cnt){

    // std::ifstream file("books_200M_uint32", std::ios::binary | std::ios::in);
    // if (!file.is_open()) {
    //     std::cerr << "Failed to open file!" << std::endl;
    //     return ;
    // }


    size_t number = 1e7;
    // std::uniform_int_distribution<uint64_t> key_dist(0, 2e8);
    std::uniform_int_distribution<_payload_t> payload_dist(0, 1e8);

    // std::mt19937 key_gen(123);   
    std::mt19937 gen(456); 
    std::uniform_real_distribution<double> dist(10.0, 100.0);

    _key_t* keys = new _key_t[number];      // 动态分配数组存储keys
    _payload_t* payloads = new _payload_t[number]; // 动态分配数组存储payloads

    // std::unordered_set<_key_t> unique_keys; // 用于检测重复keys
    // unique_keys.reserve(number);            // 预留空间以提高效率

    // size_t index = 0;
    // while (index < number) {
    //     uint64_t key_int = key_dist(key_gen);
    //     _key_t key_candidate = static_cast<_key_t>(key_int);
    //     // 尝试将生成的key加入set，如果是新元素，则加入数组
    //     if (unique_keys.insert(key_candidate).second) {
    //         keys[index] = key_candidate; // 存储key
    //         payloads[index] = payload_dist(payload_gen); // 生成并存储payload
    //         index++; // 只有在成功添加新key时才递增索引
    //     }
    // }
    // std::unordered_set<_key_t> unique_keys;
    // unique_keys.reserve(number);

    // size_t lines_read = 0;
    // uint32_t key;
    // while (lines_read < number && file.read(reinterpret_cast<char*>(&key), sizeof(uint32_t))) {
    //     if (unique_keys.insert(key).second) {
    //         keys[lines_read] = key;
    //         payloads[lines_read] = key;
    //         lines_read++;
    //     }
    // }
    // std::cout << lines_read << std::endl;

    _key_t key = 0;
    _payload_t payload = 0;
    for (int i = 0; i < number; i ++ )
    {
        key += dist(gen);
        keys[i] = key;
        payloads[i] = key;
    }

    // Sort keys
    std::sort(keys, keys+number);

    std::cout << std::fixed << std::setprecision(6) << keys[number - 1] << std::endl;

    std::cout << "sort successfully" << std::endl;

    // NVM pool path
    
    TestIndex test_index;

        
    

    std::cout << "init successfully" << std::endl;

    test_index.bulk_load(keys, payloads, number);

    std::cout << "load successfully" << std::endl; 
    std::vector<char> buffer;
    test_index.serializePlinIndex(buffer);
    test_index.PrintInfo();

    run_search_test(test_index, keys, payloads, number);

    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    int port = 8080;

    // 创建 socket 文件描述符
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // 强制附加 socket 到端口 8080
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    // 绑定 socket 到地址
    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // 开始监听
    if (listen(server_fd, 3) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    // 接受客户端连接
    if ((new_socket = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&addrlen))<0) {
        perror("accept");
        exit(EXIT_FAILURE);
    }

    std::cout << "Connect success" << std::endl;

    while (true)
    {
        char buf[1024] = "";
        int recv_len = recv(new_socket, buf, sizeof(buf), 0);

        if (std::strcmp(buf, "META") == 0) 
        {
            sendSerializedData(buffer, new_socket);
        }
        else
        {
            std::vector<int> leaf_path = deserializeVector(std::vector<char>(buf, buf + recv_len));
            

        }
    }

    

    

    close(new_socket);
    close(server_fd);

    // std::vector<char> buffer;

    // test_index.serializePlinIndex(buffer);

    // test_index.PrintInfo();

    // TestIndex copy_index;

    // copy_index.deserializePlinIndex(buffer);

    // copy_index.PrintInfo();

    // std::cout << buffer.size() << std::endl;

    // std::thread *search_test[thread_cnt];
    // for (size_t i = 0; i < thread_cnt; i++){
    //     search_test[i] = new std::thread(run_search_test, std::ref(test_index), keys, payloads, number);
    // }

    // run_search_test(test_index, keys, payloads, number);

    // std::cout << "search_test successfully" << std::endl;

    // for (size_t i = 0; i < thread_cnt; i++){
    //     search_test[i]->join();
    //     std::cout << "delete successfully" << std::endl;
    //     delete search_test[i];
    // }

    // std::cout << "delete successfully" << std::endl;

    // std::thread *upsert_test[thread_cnt];
    // for (size_t i = 0; i < thread_cnt; i++){
    //     upsert_test[i] = new std::thread(run_upsert_test, std::ref(test_index), keys, payloads,number);
        
    // }

    // std::cout << "upsert_test successfully" << std::endl;

    

    // for (size_t i = 0; i < thread_cnt; i++){
    //     upsert_test[i]->join();
    //     delete upsert_test[i];
    // }
    // std::cout << "wrong count: " << count << std::endl;

    // std::cout << "delete successfully" << std::endl;
}

int main() {
    test(1);
}
