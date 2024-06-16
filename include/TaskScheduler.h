#include <iostream>
#include <vector>
#include <random>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include "plin_index.h"

class TaskScheduler {
public:
    std::mutex mu;
    std::condition_variable cv;
    bool ready = false;
    std::queue<std::tuple<std::string, size_t, std::vector<_payload_t>>> tasks;

    void addTask(std::tuple<std::string, size_t, std::vector<_payload_t>>&& data) {
        std::lock_guard<std::mutex> slock(mu);
        tasks.push(std::move(data));
        ready = true;
        cv.notify_all();
    }

    std::tuple<std::string, size_t,std::vector<_payload_t>> getTask() {
        std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock, [this]{ return ready; });
        auto task = tasks.front();
        tasks.pop();
        ready = !tasks.empty();
        return task;
    }
};



